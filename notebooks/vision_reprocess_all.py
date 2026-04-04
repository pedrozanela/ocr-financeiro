# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin Vision OCR — Reprocessar Todos (Paralelo)
# MAGIC
# MAGIC Dispara `vision_extraction` em paralelo para todos os PDFs do volume
# MAGIC (ou filtrados por `pdf_filter`). Usa `dbutils.notebook.run()` por PDF,
# MAGIC com até `max_workers` notebooks simultâneos.
# MAGIC
# MAGIC Salva direto nas tabelas oficiais `resultados` + `resultados_final`.

# COMMAND ----------

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------
# MAGIC %md ## 1. Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",      "")
dbutils.widgets.text("schema",       "ocr_financeiro")
dbutils.widgets.text("volume_path",  "")
dbutils.widgets.text("pdf_filter",   "")        # vazio = todos; ex: "MGLU3" filtra por nome
dbutils.widgets.text("max_workers",  "3")       # notebooks paralelos (cuidado com rate limits do Vision)
dbutils.widgets.text("reprocess_existing", "false")  # "true" = reprocessa mesmo os já extraídos
dbutils.widgets.text("extractor_endpoint", "extrator-financeiro")
dbutils.widgets.text("secret_scope",       "ocr-financeiro")
dbutils.widgets.text("secret_key",         "pat-servico")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH         = dbutils.widgets.get("volume_path") or f"/Volumes/{_cat}/{_sch}/documentos_pdf"
PDF_FILTER          = dbutils.widgets.get("pdf_filter").strip()
MAX_WORKERS         = int(dbutils.widgets.get("max_workers"))
REPROCESS_EXISTING  = dbutils.widgets.get("reprocess_existing").lower() == "true"
EXTRACTOR_ENDPOINT  = dbutils.widgets.get("extractor_endpoint")
SECRET_SCOPE        = dbutils.widgets.get("secret_scope")
SECRET_KEY          = dbutils.widgets.get("secret_key")

RESULTS_TABLE = f"{_cat}.{_sch}.resultados"
VISION_NOTEBOOK = "/Users/pedro.zanela@databricks.com/techfin/notebooks/vision_extraction"

print(f"Volume          : {VOLUME_PATH}")
print(f"Filtro          : {PDF_FILTER or '(todos)'}")
print(f"Workers         : {MAX_WORKERS}")
print(f"Reprocess exist : {REPROCESS_EXISTING}")

# COMMAND ----------
# MAGIC %md ## 2. Identificar PDFs a processar

# COMMAND ----------

all_pdfs = sorted([f.name for f in dbutils.fs.ls(VOLUME_PATH) if f.name.lower().endswith(".pdf")])

if PDF_FILTER:
    all_pdfs = [p for p in all_pdfs if PDF_FILTER.lower() in p.lower()]

if not REPROCESS_EXISTING:
    processed = {row["document_name"] for row in
                 spark.sql(f"SELECT DISTINCT document_name FROM {RESULTS_TABLE}").collect()}
    to_process = [p for p in all_pdfs if p not in processed]
else:
    to_process = all_pdfs

print(f"PDFs no volume  : {len(all_pdfs)}")
print(f"A processar     : {len(to_process)}")
for name in to_process:
    print(f"  • {name}")

if not to_process:
    print("Nenhum PDF a processar. Encerrando.")
    dbutils.notebook.exit("no_pdfs")

# COMMAND ----------
# MAGIC %md ## 3. Processar em paralelo

# COMMAND ----------

print_lock = threading.Lock()


def run_vision_notebook(pdf_name: str) -> dict:
    t0   = time.time()
    stat = {"pdf": pdf_name, "status": "error", "elapsed_s": 0.0, "error_msg": ""}
    try:
        result = dbutils.notebook.run(
            VISION_NOTEBOOK,
            timeout_seconds=900,
            arguments={
                "catalog":            _cat,
                "schema":             _sch,
                "volume_path":        VOLUME_PATH,
                "pdf_name":           pdf_name,
                "extractor_endpoint": EXTRACTOR_ENDPOINT,
                "secret_scope":       SECRET_SCOPE,
                "secret_key":         SECRET_KEY,
            },
        )
        stat["status"]  = "ok" if result == "ok" else "error"
        stat["result"]  = result
        if stat["status"] != "ok":
            stat["error_msg"] = result
    except Exception as e:
        stat["error_msg"] = str(e)[:300]

    stat["elapsed_s"] = round(time.time() - t0, 1)
    with print_lock:
        icon = "✓" if stat["status"] == "ok" else "✗"
        print(f"  {icon} [{pdf_name}] {stat['elapsed_s']}s  {stat.get('error_msg','')[:60]}")
    return stat


total_start = time.time()
stats       = []

print(f"\nProcessando {len(to_process)} PDF(s) com {MAX_WORKERS} workers paralelos...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {pool.submit(run_vision_notebook, name): name for name in to_process}
    for future in as_completed(futures):
        stats.append(future.result())

total_elapsed = time.time() - total_start

# COMMAND ----------
# MAGIC %md ## 4. Relatório

# COMMAND ----------

ok     = [s for s in stats if s["status"] == "ok"]
errors = [s for s in stats if s["status"] != "ok"]

print("=" * 65)
print(f"  Tempo total : {int(total_elapsed//60)}m {int(total_elapsed%60)}s")
print(f"  Sucesso     : {len(ok)} / {len(to_process)}")
print(f"  Erros       : {len(errors)}")
print("=" * 65)
for s in sorted(stats, key=lambda x: x["pdf"]):
    icon = "✓" if s["status"] == "ok" else "✗"
    print(f"  {icon} {s['pdf'][:50]:<50} {s['elapsed_s']:>6.1f}s")
if errors:
    print()
    for s in errors:
        print(f"  ✗ {s['pdf']}: {s['error_msg']}")

if errors and len(errors) > len(to_process) // 2:
    raise Exception(f"Mais da metade falhou ({len(errors)}/{len(to_process)})")
