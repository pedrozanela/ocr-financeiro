# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Batch Job Diário
# MAGIC Detecta PDFs novos no volume UC e extrai dados financeiros via endpoint `techfin-ocr-v4`.

# COMMAND ----------

# MAGIC %pip install pypdf -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import io
import json
import os
import time
import requests
from pypdf import PdfReader

VOLUME_PATH     = "/Volumes/pedro_zanela/ia/dados/techfin/ocr"
RESULTS_TABLE   = "pedro_zanela.ia.new_ocr_techfin_results"
OCR_ENDPOINT    = "techfin-ocr-v4"
DATABRICKS_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
ENDPOINT_URL    = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

TOKEN   = dbutils.secrets.get("pedro-zanela-scope", "techfin-ocr-pat")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------
# MAGIC %md ## 1. Identificar PDFs novos

# COMMAND ----------

volume_pdfs = {f for f in os.listdir(VOLUME_PATH) if f.lower().endswith(".pdf")}
processed   = {row["document_name"] for row in spark.sql(f"SELECT document_name FROM {RESULTS_TABLE}").collect()}
new_pdfs    = volume_pdfs - processed

print(f"Volume: {len(volume_pdfs)} PDFs | Já processados: {len(processed)} | Novos: {len(new_pdfs)}")

if not new_pdfs:
    print("Nenhum PDF novo encontrado. Encerrando.")
    dbutils.notebook.exit("no_new_pdfs")

# Warm-up: acorda o endpoint do scale-to-zero antes de processar
print("Aquecendo endpoint (scale-to-zero)...")
for attempt in range(10):
    try:
        r = requests.post(ENDPOINT_URL, headers=HEADERS,
                          json={"dataframe_records": [{"text": "teste"}]}, timeout=60)
        if r.status_code in (200, 422, 400):
            print(f"  Endpoint pronto (status {r.status_code})")
            break
        elif r.status_code == 504:
            print(f"  Ainda inicializando... tentativa {attempt+1}/10")
            time.sleep(30)
        else:
            print(f"  Status inesperado: {r.status_code} — continuando mesmo assim")
            break
    except requests.exceptions.Timeout:
        print(f"  Timeout no warm-up — tentativa {attempt+1}/10")
        time.sleep(30)

# COMMAND ----------
# MAGIC %md ## 2. Processar PDFs novos

# COMMAND ----------

def extract_text(filepath: str) -> str:
    reader = PdfReader(filepath)
    pages = [p.extract_text() for p in reader.pages if p.extract_text()]
    return "\n\n".join(pages)

def call_endpoint(text: str, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT_URL, headers=HEADERS,
                                 json={"dataframe_records": [{"text": text}]},
                                 timeout=300)
            if resp.status_code == 504:
                wait = 60 * (attempt + 1)
                print(f"    504 Gateway Timeout — aguardando {wait}s antes de retry {attempt+1}/{max_retries}")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            r = resp.json().get("predictions", resp.json())
            # predictions is a list of one result (one per input row)
            # each result is itself a list of dicts (tipo_entidade × periodo)
            if isinstance(r, list) and len(r) == 1:
                r = r[0]
            if isinstance(r, str):  r = json.loads(r)
            return r
        except requests.exceptions.Timeout:
            wait = 60 * (attempt + 1)
            print(f"    Timeout local — aguardando {wait}s antes de retry {attempt+1}/{max_retries}")
            time.sleep(wait)
    raise Exception(f"Endpoint falhou após {max_retries} tentativas")

def get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict): return None
        d = d.get(k)
    return d

def save_result(pdf_name: str, results):
    """Salva array de resultados — múltiplas linhas por PDF (tipo_entidade × periodo)."""
    if isinstance(results, dict):
        results = [results]

    def esc(v): return str(v or "").replace("'", "''")
    doc = pdf_name.replace("'", "''")

    for result in results:
        ej   = json.dumps(result, ensure_ascii=False).replace("'", "''")
        rs   = esc(get_nested(result, "razao_social"))
        cnpj = esc(get_nested(result, "cnpj"))
        per  = esc(get_nested(result, "identificacao.periodo"))
        te   = esc(get_nested(result, "tipo_entidade"))
        at   = esc(get_nested(result, "ativo_total"))
        ll   = esc(get_nested(result, "dre.lucro_liquido"))

        spark.sql(f"""
            MERGE INTO {RESULTS_TABLE} t
            USING (SELECT '{doc}' AS document_name,
                          '{te}'  AS tipo_entidade,
                          '{per}' AS periodo) s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json = '{ej}',
                razao_social   = '{rs}',
                cnpj           = '{cnpj}',
                ativo_total    = TRY_CAST('{at}' AS DOUBLE),
                lucro_liquido  = TRY_CAST('{ll}' AS DOUBLE)
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json,
                 razao_social, cnpj, ativo_total, lucro_liquido)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}',
                 '{rs}', '{cnpj}',
                 TRY_CAST('{at}' AS DOUBLE), TRY_CAST('{ll}' AS DOUBLE))
        """)

# COMMAND ----------

errors    = []
successes = []

for pdf_name in sorted(new_pdfs):
    pdf_path = f"{VOLUME_PATH}/{pdf_name}"
    print(f"\n→ {pdf_name}")
    try:
        text = extract_text(pdf_path)
        if not text.strip():
            print("  ⚠ Sem texto extraível (PDF escaneado sem OCR)")
            errors.append((pdf_name, "no_text"))
            continue

        result = call_endpoint(text)

        # result is now a list of dicts (one per tipo_entidade × periodo)
        if isinstance(result, dict):
            result = [result]
        for r in result:
            if isinstance(r, dict) and r.get("error") == "parse_failed":
                raise ValueError(f"Modelo retornou erro de parse: {r.get('raw', '')[:200]}")

        save_result(pdf_name, result)
        successes.append(pdf_name)
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo")) for r in result]
        print(f"  ✓ Salvo | {len(result)} registro(s): {combos}")

    except Exception as e:
        errors.append((pdf_name, str(e)))
        print(f"  ✗ Erro: {e}")

# COMMAND ----------
# MAGIC %md ## 3. Resumo

# COMMAND ----------

print(f"\n{'='*50}")
print(f"Processados com sucesso : {len(successes)}")
print(f"Erros                   : {len(errors)}")
if errors:
    print("\nErros por arquivo:")
    for name, err in errors:
        print(f"  - {name}: {err}")

# Se houver erros críticos (não apenas PDFs sem texto), falhar o job
hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    raise Exception(f"{len(hard_errors)} PDF(s) falharam no processamento: {[n for n, _ in hard_errors]}")
