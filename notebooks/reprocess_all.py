# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Reprocessar TUDO
# MAGIC Reprocessa todos os PDFs do volume com o modelo atual, sobrescrevendo resultados anteriores.
# MAGIC Processa em paralelo (4 workers) e sincroniza `resultados_final` ao final.
# MAGIC
# MAGIC **Custos estimados:**
# MAGIC - `ai_parse_document`: ~$0.0150/página (Serverless SQL)
# MAGIC - `claude-sonnet-4-6` via Foundation Model API: $3.00/1M tokens entrada, $15.00/1M tokens saída

# COMMAND ----------

import json
import time
import requests
from pyspark.sql.functions import expr, concat_ws

# Configuração via widgets (compatível com Serverless)
dbutils.widgets.text("catalog",      "")
dbutils.widgets.text("schema",       "ocr_financeiro")
dbutils.widgets.text("volume_path",  "")
dbutils.widgets.text("endpoint",     "extrator-financeiro")
_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH     = dbutils.widgets.get("volume_path") or f"/Volumes/{_cat}/{_sch}/documentos_pdf"
RESULTS_TABLE   = f"{_cat}.{_sch}.resultados"
FINAL_TABLE     = f"{_cat}.{_sch}.resultados_final"
SOURCE_TABLE    = f"{_cat}.{_sch}.documentos"
OCR_ENDPOINT    = dbutils.widgets.get("endpoint")

DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if not DATABRICKS_HOST.startswith("http"):
    DATABRICKS_HOST = f"https://{DATABRICKS_HOST}"
ENDPOINT_URL = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

PRICE_INPUT_PER_TOKEN  = 3.00 / 1_000_000
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000
PRICE_AI_PARSE_PER_PAGE = 0.015

TOKEN   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

print(f"Catalog  : {_cat}.{_sch}")
print(f"Volume   : {VOLUME_PATH}")
print(f"Endpoint : {OCR_ENDPOINT}")

# COMMAND ----------
# MAGIC %md ## 1. Listar todos os PDFs no volume

# COMMAND ----------

all_files = dbutils.fs.ls(VOLUME_PATH)
all_pdfs  = sorted([f.name for f in all_files if f.name.lower().endswith(".pdf")])

print(f"Total de PDFs no volume: {len(all_pdfs)}")
for name in all_pdfs:
    print(f"  • {name}")

# COMMAND ----------
# MAGIC %md ## 2. Funções

# COMMAND ----------

def extract_text_ai_parse(pdf_name: str) -> tuple[str, int]:
    """Extrai texto + nº de páginas via ai_parse_document."""
    volume_path = f"{VOLUME_PATH}/{pdf_name}"
    df = (
        spark.read.format("binaryFile").load(volume_path)
        .withColumn("parsed", expr("ai_parse_document(content)"))
        .withColumn("num_pages", expr("size(try_cast(parsed:document:pages AS ARRAY<VARIANT>))"))
        .withColumn("text", concat_ws("\n\n", expr("""
            transform(
                try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                element -> try_cast(element:content AS STRING)
            )
        """)))
        .select("text", "num_pages")
    )
    rows = df.collect()
    if not rows:
        return "", 0
    return rows[0]["text"] or "", int(rows[0]["num_pages"] or 0)


def call_endpoint(text: str, max_retries: int = 3) -> tuple[list, int, int]:
    """Chama endpoint OCR. Retorna (resultados, input_tokens, output_tokens)."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT_URL, headers=HEADERS,
                                 json={"dataframe_records": [{"text": text}]},
                                 timeout=600)
            if resp.status_code in (429, 503, 504):
                wait = 60 * (attempt + 1)
                print(f"    HTTP {resp.status_code} — retry {attempt+1}/{max_retries} em {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            body          = resp.json()
            metadata      = body.get("metadata", {})
            input_tokens  = int(metadata.get("input_tokens",  0)) or len(text) // 4
            output_tokens = int(metadata.get("output_tokens", 0)) or 2000
            r = body.get("predictions", body)
            if isinstance(r, list) and len(r) == 1:
                r = r[0]
            if isinstance(r, str):
                r = json.loads(r)
            if isinstance(r, dict):
                r = [r]
            return r, input_tokens, output_tokens
        except requests.exceptions.Timeout:
            wait = 60 * (attempt + 1)
            print(f"    Timeout — retry {attempt+1}/{max_retries} em {wait}s")
            time.sleep(wait)
    raise Exception(f"Endpoint falhou após {max_retries} tentativas")


def get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def save_result(pdf_name: str, results: list):
    """Salva resultados em `resultados` via MERGE com todas as colunas."""
    if isinstance(results, dict):
        results = [results]

    def esc(v): return str(v or "").replace("'", "''")
    doc = pdf_name.replace("'", "''")

    for result in results:
        result     = dict(result)
        assessment = result.pop("_assessment", [])
        usage      = result.pop("_usage", {})

        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        uj   = json.dumps(usage,      ensure_ascii=False).replace("'", "''")
        ej   = json.dumps(result,     ensure_ascii=False).replace("'", "''")
        rs   = esc(get_nested(result, "razao_social"))
        cnpj = esc(get_nested(result, "cnpj"))
        per  = esc(get_nested(result, "identificacao.periodo"))
        te   = esc(get_nested(result, "tipo_entidade"))
        td   = esc(get_nested(result, "identificacao.tipo_demonstrativo"))
        moe  = esc(get_nested(result, "identificacao.moeda"))
        escv = esc(get_nested(result, "identificacao.escala_valores"))

        spark.sql(f"""
            MERGE INTO {RESULTS_TABLE} t
            USING (SELECT '{doc}' AS document_name,
                          '{te}'  AS tipo_entidade,
                          '{per}' AS periodo) s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json     = '{ej}',
                assessment_json    = '{aj}',
                token_usage_json   = '{uj}',
                razao_social       = '{rs}',
                cnpj               = '{cnpj}',
                tipo_demonstrativo = '{td}',
                moeda              = '{moe}',
                escala_valores     = '{escv}',
                processado_em      = CURRENT_TIMESTAMP(),
                modelo_versao      = '{OCR_ENDPOINT}'
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                 token_usage_json, razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores,
                 processado_em, modelo_versao)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}', '{aj}',
                 '{uj}', '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{OCR_ENDPOINT}')
        """)

# COMMAND ----------
# MAGIC %md ## 3. Reprocessar todos os PDFs (paralelo)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

MAX_WORKERS = 4
print_lock  = threading.Lock()

def process_one(pdf_name: str) -> dict:
    """Processa um PDF completo. Thread-safe."""
    doc_start = time.time()
    doc_stat  = {
        "pdf": pdf_name, "status": "error",
        "time_parse_s": 0.0, "time_llm_s": 0.0,
        "pages": 0, "input_tokens": 0, "output_tokens": 0,
        "cost_parse_usd": 0.0, "cost_llm_usd": 0.0, "cost_total_usd": 0.0,
        "records": 0, "error_msg": "",
    }

    try:
        # ai_parse_document
        t0 = time.time()
        text, num_pages = extract_text_ai_parse(pdf_name)
        doc_stat["time_parse_s"]   = round(time.time() - t0, 1)
        doc_stat["pages"]          = num_pages
        doc_stat["cost_parse_usd"] = round(num_pages * PRICE_AI_PARSE_PER_PAGE, 4)
        with print_lock:
            print(f"  [{pdf_name}] ai_parse: {num_pages} pág em {doc_stat['time_parse_s']}s (${doc_stat['cost_parse_usd']:.4f})")

        if not text or not text.strip():
            doc_stat["error_msg"] = "no_text"
            with print_lock:
                print(f"  [{pdf_name}] ⚠ Sem texto extraível")
            return doc_stat

        # Salvar texto na tabela documentos
        text_esc = text.replace("'", "''")
        doc_esc  = pdf_name.replace("'", "''")
        spark.sql(f"""
            MERGE INTO {SOURCE_TABLE} t
            USING (SELECT '{doc_esc}' AS document_name) s ON t.document_name = s.document_name
            WHEN MATCHED THEN UPDATE SET document_text = '{text_esc}'
            WHEN NOT MATCHED THEN INSERT (document_name, document_text) VALUES ('{doc_esc}', '{text_esc}')
        """)

        # Endpoint OCR
        t0 = time.time()
        results, input_tokens, output_tokens = call_endpoint(text)
        doc_stat["time_llm_s"]    = round(time.time() - t0, 1)
        doc_stat["input_tokens"]  = input_tokens
        doc_stat["output_tokens"] = output_tokens
        cost_llm = (input_tokens * PRICE_INPUT_PER_TOKEN) + (output_tokens * PRICE_OUTPUT_PER_TOKEN)
        doc_stat["cost_llm_usd"]  = round(cost_llm, 4)

        valid   = [r for r in results if not (isinstance(r, dict) and r.get("error"))]
        errored = [r for r in results if isinstance(r, dict) and r.get("error")]
        if errored and not valid:
            raise ValueError(f"parse_failed: {errored[0].get('raw','')[:200]}")
        if errored:
            with print_lock:
                print(f"  [{pdf_name}] ⚠ {len(errored)} registro(s) com parse truncado descartados")

        save_result(pdf_name, valid)
        doc_stat["records"] = len(valid)
        doc_stat["status"]  = "ok"
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo")) for r in valid]
        with print_lock:
            icon = "✓" if not errored else "⚠"
            print(f"  {icon} [{pdf_name}] {len(valid)} registro(s) {combos} em {doc_stat['time_llm_s']}s (${doc_stat['cost_llm_usd']:.4f})")

    except Exception as e:
        doc_stat["error_msg"] = str(e)
        with print_lock:
            print(f"  ✗ [{pdf_name}] {e}")

    doc_stat["time_total_s"]   = round(time.time() - doc_start, 1)
    doc_stat["cost_total_usd"] = round(doc_stat["cost_parse_usd"] + doc_stat["cost_llm_usd"], 4)
    return doc_stat


total_start = time.time()
stats       = []
errors      = []
successes   = []

print(f"Reprocessando {len(all_pdfs)} PDF(s) com {MAX_WORKERS} workers paralelos...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {pool.submit(process_one, name): name for name in all_pdfs}
    for future in as_completed(futures):
        doc_stat = future.result()
        stats.append(doc_stat)
        if doc_stat["status"] == "ok":
            successes.append(doc_stat["pdf"])
        if doc_stat["error_msg"]:
            errors.append((doc_stat["pdf"], doc_stat["error_msg"]))

total_elapsed = time.time() - total_start

# COMMAND ----------
# MAGIC %md ## 4. Sincronizar resultados_final

# COMMAND ----------

if successes:
    in_clause = ", ".join(f"'{n.replace(chr(39), chr(39)+chr(39))}'" for n in successes)
    spark.sql(f"""
        MERGE INTO {FINAL_TABLE} AS t
        USING (
            SELECT document_name, tipo_entidade, periodo, extracted_json,
                   razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores
            FROM {RESULTS_TABLE}
            WHERE document_name IN ({in_clause})
        ) AS s
        ON  t.document_name = s.document_name
        AND COALESCE(t.tipo_entidade, '') = COALESCE(s.tipo_entidade, '')
        AND COALESCE(t.periodo, '')       = COALESCE(s.periodo, '')
        WHEN MATCHED AND COALESCE(t.atualizado_por, 'model') = 'model' THEN UPDATE SET
            extracted_json     = s.extracted_json,
            razao_social       = s.razao_social,
            cnpj               = s.cnpj,
            tipo_demonstrativo = s.tipo_demonstrativo,
            moeda              = s.moeda,
            escala_valores     = s.escala_valores,
            atualizado_em      = CURRENT_TIMESTAMP(),
            atualizado_por     = 'model'
        WHEN NOT MATCHED THEN INSERT
            (document_name, tipo_entidade, periodo, extracted_json,
             razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores,
             atualizado_em, atualizado_por)
        VALUES
            (s.document_name, s.tipo_entidade, s.periodo, s.extracted_json,
             s.razao_social, s.cnpj, s.tipo_demonstrativo, s.moeda, s.escala_valores,
             CURRENT_TIMESTAMP(), 'model')
    """)
    print(f"✓ resultados_final sincronizado para {len(successes)} documento(s)")

# COMMAND ----------
# MAGIC %md ## 5. Relatório de Custos e Tempo

# COMMAND ----------

total_pages         = sum(s["pages"]           for s in stats)
total_input_tokens  = sum(s["input_tokens"]    for s in stats)
total_output_tokens = sum(s["output_tokens"]   for s in stats)
total_cost_parse    = sum(s["cost_parse_usd"]  for s in stats)
total_cost_llm      = sum(s["cost_llm_usd"]    for s in stats)
total_cost          = total_cost_parse + total_cost_llm

print("=" * 72)
print(f"  RELATÓRIO FINAL — TechFin OCR Reprocessamento Total")
print("=" * 72)
print(f"  Tempo total        : {int(total_elapsed//60)}m {int(total_elapsed%60)}s")
print(f"  PDFs processados   : {len(successes)} / {len(all_pdfs)}")
print(f"  Erros              : {len(errors)}")
print()
print(f"  ── ai_parse_document ────────────────────────────────────────")
print(f"  Páginas processadas: {total_pages}")
print(f"  Custo estimado     : ${total_cost_parse:.4f}  (@$0.015/página)")
print()
print(f"  ── Foundation Model API (claude-sonnet-4-6) ─────────────────")
print(f"  Tokens de entrada  : {total_input_tokens:,}  (${total_input_tokens * PRICE_INPUT_PER_TOKEN:.4f})")
print(f"  Tokens de saída    : {total_output_tokens:,}  (${total_output_tokens * PRICE_OUTPUT_PER_TOKEN:.4f})")
print(f"  Custo estimado LLM : ${total_cost_llm:.4f}")
print()
print(f"  ── TOTAL ────────────────────────────────────────────────────")
print(f"  Custo total        : ${total_cost:.4f}")
print("=" * 72)
print()
print(f"  {'PDF':<45} {'Págs':>5} {'Tempo':>7} {'Custo':>8}  Status")
print(f"  {'-'*45} {'-'*5} {'-'*7} {'-'*8}  ------")
for s in sorted(stats, key=lambda x: x["pdf"]):
    status_str = "✓" if s["status"] == "ok" else f"✗ {s['error_msg'][:20]}"
    print(f"  {s['pdf'][:44]:<45} {s['pages']:>5} {s['time_total_s']:>6.1f}s ${s['cost_total_usd']:>7.4f}  {status_str}")

if errors:
    print()
    for name, err in errors:
        print(f"  ✗ {name}: {err}")

hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    raise Exception(f"{len(hard_errors)} PDF(s) falharam: {[n for n, _ in hard_errors]}")
