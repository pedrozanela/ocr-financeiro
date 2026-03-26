# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Retry PDFs com Falha
# MAGIC Reprocessa apenas os PDFs que falharam no último reprocess_all.

# COMMAND ----------

import json
import time
import requests
from pyspark.sql.functions import expr, concat_ws

VOLUME_PATH     = "/Volumes/pedro_zanela/ia/dados/techfin/ocr"
RESULTS_TABLE   = "pedro_zanela.ia.new_ocr_techfin_results"
OCR_ENDPOINT    = "techfin-ocr-v4"
DATABRICKS_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
ENDPOINT_URL    = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

PRICE_INPUT_PER_TOKEN  = 3.00 / 1_000_000
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000
PRICE_AI_PARSE_PER_PAGE = 0.015

TOKEN   = dbutils.secrets.get("pedro-zanela-scope", "techfin-ocr-pat")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# PDFs que falharam no último run
FAILED_PDFS = [
    "CM COMERCIO E SERVICOS LTDA BP 24.pdf",
]

print(f"PDFs para retry: {len(FAILED_PDFS)}")
for p in FAILED_PDFS:
    print(f"  • {p}")

# COMMAND ----------

def extract_text_ai_parse(pdf_name: str) -> tuple[str, int]:
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


def call_endpoint(text: str, max_retries: int = 5) -> tuple[dict, int, int]:
    """Mais retries e waits maiores para PDFs grandes."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT_URL, headers=HEADERS,
                                 json={"dataframe_records": [{"text": text}]},
                                 timeout=480)  # 8min timeout
            if resp.status_code == 504:
                wait = 90 * (attempt + 1)
                print(f"    504 — aguardando {wait}s antes de retry {attempt+1}/{max_retries}")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            body = resp.json()
            metadata      = body.get("metadata", {})
            input_tokens  = int(metadata.get("input_tokens",  0))
            output_tokens = int(metadata.get("output_tokens", 0))
            if input_tokens == 0:
                input_tokens = len(text) // 4
            if output_tokens == 0:
                output_tokens = 2000
            r = body.get("predictions", body)
            if isinstance(r, list) and len(r) == 1:
                r = r[0]
            if isinstance(r, str):
                r = json.loads(r)
            return r, input_tokens, output_tokens
        except requests.exceptions.Timeout:
            wait = 90 * (attempt + 1)
            print(f"    Timeout — aguardando {wait}s antes de retry {attempt+1}/{max_retries}")
            time.sleep(wait)
    raise Exception(f"Endpoint falhou após {max_retries} tentativas")


def get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def save_result(pdf_name: str, results):
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
                extracted_json = '{ej}',
                razao_social   = '{rs}',
                cnpj           = '{cnpj}',
                tipo_demonstrativo = '{td}',
                moeda              = '{moe}',
                escala_valores     = '{escv}' 
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json,
                 razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}',
                 '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}')
        """)

# COMMAND ----------
# MAGIC %md ## Reprocessar

# COMMAND ----------

total_start = time.time()
stats = []
errors = []
successes = []

for pdf_name in FAILED_PDFS:
    doc_start = time.time()
    print(f"\n→ {pdf_name}")
    doc_stat = {
        "pdf": pdf_name, "status": "error",
        "time_parse_s": 0.0, "time_llm_s": 0.0, "time_total_s": 0.0,
        "pages": 0, "input_tokens": 0, "output_tokens": 0,
        "cost_parse_usd": 0.0, "cost_llm_usd": 0.0, "cost_total_usd": 0.0,
        "records": 0, "error_msg": "",
    }
    try:
        t0 = time.time()
        text, num_pages = extract_text_ai_parse(pdf_name)
        doc_stat["time_parse_s"]   = round(time.time() - t0, 2)
        doc_stat["pages"]          = num_pages
        doc_stat["cost_parse_usd"] = round(num_pages * PRICE_AI_PARSE_PER_PAGE, 4)
        print(f"  ai_parse: {num_pages} pág em {doc_stat['time_parse_s']}s  (${doc_stat['cost_parse_usd']:.4f})")

        if not text or not text.strip():
            print("  ⚠ Sem texto extraível")
            doc_stat["error_msg"] = "no_text"
            errors.append((pdf_name, "no_text"))
            stats.append(doc_stat)
            continue

        t1 = time.time()
        result, input_tokens, output_tokens = call_endpoint(text)
        doc_stat["time_llm_s"]    = round(time.time() - t1, 2)
        doc_stat["input_tokens"]  = input_tokens
        doc_stat["output_tokens"] = output_tokens
        cost_llm = (input_tokens * PRICE_INPUT_PER_TOKEN) + (output_tokens * PRICE_OUTPUT_PER_TOKEN)
        doc_stat["cost_llm_usd"]  = round(cost_llm, 4)
        print(f"  LLM: {input_tokens:,} in + {output_tokens:,} out tokens em {doc_stat['time_llm_s']}s  (${doc_stat['cost_llm_usd']:.4f})")

        if isinstance(result, dict):
            result = [result]
        for r in result:
            if isinstance(r, dict) and r.get("error") == "parse_failed":
                raise ValueError(f"Modelo retornou erro de parse: {r.get('raw', '')[:200]}")

        save_result(pdf_name, result)
        doc_stat["records"] = len(result)
        doc_stat["status"]  = "ok"
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo")) for r in result]
        print(f"  ✓ Salvo | {len(result)} registro(s): {combos}")
        successes.append(pdf_name)

    except Exception as e:
        doc_stat["error_msg"] = str(e)
        errors.append((pdf_name, str(e)))
        print(f"  ✗ Erro: {e}")

    doc_stat["time_total_s"]   = round(time.time() - doc_start, 2)
    doc_stat["cost_total_usd"] = round(doc_stat["cost_parse_usd"] + doc_stat["cost_llm_usd"], 4)
    stats.append(doc_stat)

total_elapsed = time.time() - total_start

# COMMAND ----------
# MAGIC %md ## Relatório

# COMMAND ----------

total_cost_parse = sum(s["cost_parse_usd"] for s in stats)
total_cost_llm   = sum(s["cost_llm_usd"]   for s in stats)
total_cost       = total_cost_parse + total_cost_llm

print("=" * 72)
print(f"  RETRY — TechFin OCR")
print("=" * 72)
print(f"  Tempo total       : {int(total_elapsed // 60)}m {int(total_elapsed % 60)}s")
print(f"  Sucesso           : {len(successes)} / {len(FAILED_PDFS)}")
print(f"  Erros             : {len(errors)}")
print(f"  Custo ai_parse    : ${total_cost_parse:.4f}")
print(f"  Custo LLM         : ${total_cost_llm:.4f}")
print(f"  Custo total       : ${total_cost:.4f}")
print("=" * 72)
print()
print(f"  {'PDF':<50} {'Pág':>4} {'Tempo':>7} {'Custo':>8}  Status")
print(f"  {'-'*50} {'-'*4} {'-'*7} {'-'*8}  ------")
for s in stats:
    status_str = "✓" if s["status"] == "ok" else f"✗ {s['error_msg'][:25]}"
    print(f"  {s['pdf'][:50]:<50} {s['pages']:>4} {s['time_total_s']:>6.1f}s ${s['cost_total_usd']:>7.4f}  {status_str}")

if errors:
    print()
    for name, err in errors:
        print(f"  ✗ {name}: {err}")

hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    raise Exception(f"{len(hard_errors)} PDF(s) ainda falharam: {[n for n, _ in hard_errors]}")
