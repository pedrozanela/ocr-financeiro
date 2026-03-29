# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Rodar modelo estruturado a partir de pedro_zanela.ia.new_ocr_techfin
# MAGIC Usa o texto já extraído na tabela (sem ai_parse_document) e chama o endpoint OCR.
# MAGIC Salva resultados em `pedro_zanela.ia.new_ocr_techfin_results`.

# COMMAND ----------

import json
import time
import requests

# Configuração via widgets (compatível com Serverless)
dbutils.widgets.text("catalog", "pedro_zanela")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("endpoint", "extrator-financeiro")
dbutils.widgets.text("secret_scope", "ocr-financeiro")
dbutils.widgets.text("secret_key", "pat-servico")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")
SOURCE_TABLE    = f"{_cat}.{_sch}.documentos"
RESULTS_TABLE   = f"{_cat}.{_sch}.resultados"
OCR_ENDPOINT    = dbutils.widgets.get("endpoint")
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
if not DATABRICKS_HOST.startswith("http"):
    DATABRICKS_HOST = f"https://{DATABRICKS_HOST}"
SECRET_SCOPE    = dbutils.widgets.get("secret_scope")
SECRET_KEY      = dbutils.widgets.get("secret_key")
ENDPOINT_URL    = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

PRICE_INPUT_PER_TOKEN  = 3.00 / 1_000_000
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000

TOKEN   = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------
# MAGIC %md ## 1. Carregar documentos da tabela

# COMMAND ----------

rows = spark.sql(f"SELECT document_name, document_text FROM {SOURCE_TABLE} ORDER BY document_name").collect()
print(f"Total de documentos: {len(rows)}")
for r in rows:
    print(f"  • {r['document_name']}  ({len(r['document_text'] or '')} chars)")

# COMMAND ----------
# MAGIC %md ## 2. Funções

# COMMAND ----------

RETRYABLE_CODES = {400, 429, 503, 504, 555}

def call_endpoint(text: str, max_retries: int = 5) -> tuple[dict, int, int]:
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT_URL, headers=HEADERS,
                                 json={"dataframe_records": [{"text": text}]},
                                 timeout=600)
            if resp.status_code in RETRYABLE_CODES:
                body = ""
                try: body = resp.text[:200]
                except: pass
                wait = 30 * (attempt + 1)
                with print_lock:
                    print(f"    HTTP {resp.status_code} — retry {attempt+1}/{max_retries} em {wait}s ({body})")
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


def save_result(pdf_name: str, results):
    if isinstance(results, dict):
        results = [results]
    def esc(v): return str(v or "").replace("'", "''")
    doc = pdf_name.replace("'", "''")
    for result in results:
        result = dict(result)
        if result.get("error"):
            print(f"  ⚠ Skipping parse_failed result for {pdf_name}")
            continue
        assessment = result.pop("_assessment", [])
        usage     = result.pop("_usage", {})
        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        uj   = json.dumps(usage, ensure_ascii=False).replace("'", "''")
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
                extracted_json     = '{ej}',
                assessment_json    = '{aj}',
                token_usage_json   = '{uj}',
                razao_social       = '{rs}',
                cnpj               = '{cnpj}',
                tipo_demonstrativo = '{td}',
                moeda              = '{moe}',
                escala_valores     = '{escv}',
                processado_em      = CURRENT_TIMESTAMP(),
                modelo_versao      = '{ENDPOINT_NAME}'
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                 token_usage_json, razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores,
                 processado_em, modelo_versao)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}', '{aj}',
                 '{uj}', '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{ENDPOINT_NAME}')
        """)

# COMMAND ----------
# MAGIC %md ## 3. Processar todos os documentos (paralelo, até 5 simultâneos)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

MAX_WORKERS = 2
print_lock = threading.Lock()

def process_one(pdf_name: str, text: str) -> dict:
    """Processa um documento. Thread-safe."""
    doc_stat = {
        "pdf": pdf_name, "status": "error",
        "time_llm_s": 0.0, "time_total_s": 0.0,
        "input_tokens": 0, "output_tokens": 0,
        "cost_llm_usd": 0.0, "records": 0, "error_msg": "",
    }
    doc_start = time.time()

    try:
        if not text.strip():
            doc_stat["error_msg"] = "no_text"
            with print_lock:
                print(f"  ⚠ {pdf_name}: texto vazio")
            return doc_stat

        t0 = time.time()
        result, input_tokens, output_tokens = call_endpoint(text)
        doc_stat["time_llm_s"]    = round(time.time() - t0, 2)
        doc_stat["input_tokens"]  = input_tokens
        doc_stat["output_tokens"] = output_tokens
        cost_llm = (input_tokens * PRICE_INPUT_PER_TOKEN) + (output_tokens * PRICE_OUTPUT_PER_TOKEN)
        doc_stat["cost_llm_usd"]  = round(cost_llm, 4)

        if isinstance(result, dict):
            result = [result]
        valid   = [r for r in result if not (isinstance(r, dict) and r.get("error"))]
        errored = [r for r in result if isinstance(r, dict) and r.get("error")]
        if errored and not valid:
            err_info = errored[0]
            raise ValueError(
                f"parse_failed (finish={err_info.get('finish_reason','?')}, "
                f"tokens={err_info.get('completion_tokens',0)}): "
                f"{err_info.get('raw', '')[:300]}"
            )
        if errored and valid:
            with print_lock:
                print(f"  ⚠ {pdf_name}: {len(errored)} registro(s) com parse truncado descartados, {len(valid)} válidos recuperados")
        result = valid

        save_result(pdf_name, result)
        doc_stat["records"] = len(result)
        doc_stat["status"]  = "ok" if not errored else "partial"
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo")) for r in result]
        with print_lock:
            icon = "✓" if not errored else "⚠"
            print(f"  {icon} {pdf_name}: {len(result)} registro(s) {combos} em {doc_stat['time_llm_s']}s (${doc_stat['cost_llm_usd']:.4f})")

    except Exception as e:
        doc_stat["error_msg"] = str(e)
        with print_lock:
            print(f"  ✗ {pdf_name}: {e}")

    doc_stat["time_total_s"] = round(time.time() - doc_start, 2)
    return doc_stat

# Execução paralela
total_start = time.time()
stats   = []
errors  = []
successes = []

print(f"Processando {len(rows)} documentos com {MAX_WORKERS} workers paralelos...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {
        pool.submit(process_one, row["document_name"], row["document_text"] or ""): row["document_name"]
        for row in rows
    }
    for future in as_completed(futures):
        pdf_name = futures[future]
        doc_stat = future.result()
        stats.append(doc_stat)
        if doc_stat["status"] in ("ok", "partial"):
            successes.append(pdf_name)
        if doc_stat["error_msg"]:
            errors.append((pdf_name, doc_stat["error_msg"]))

total_elapsed = time.time() - total_start

# COMMAND ----------
# MAGIC %md ## 4. Relatório

# COMMAND ----------

total_input  = sum(s["input_tokens"]  for s in stats)
total_output = sum(s["output_tokens"] for s in stats)
total_cost   = sum(s["cost_llm_usd"]  for s in stats)

print("=" * 72)
print(f"  RELATÓRIO — LLM a partir de {SOURCE_TABLE}")
print("=" * 72)
print(f"  Tempo total       : {int(total_elapsed // 60)}m {int(total_elapsed % 60)}s")
print(f"  Sucesso           : {len(successes)} / {len(rows)}")
print(f"  Erros             : {len(errors)}")
print(f"  Tokens entrada    : {total_input:,}  (${total_input * PRICE_INPUT_PER_TOKEN:.4f})")
print(f"  Tokens saída      : {total_output:,}  (${total_output * PRICE_OUTPUT_PER_TOKEN:.4f})")
print(f"  Custo total LLM   : ${total_cost:.4f}")
print("=" * 72)
print()
print(f"  {'PDF':<50} {'Tempo':>7} {'Custo':>8}  Status")
print(f"  {'-'*50} {'-'*7} {'-'*8}  ------")
for s in stats:
    status_str = "✓" if s["status"] == "ok" else "⚠ parcial" if s["status"] == "partial" else f"✗ {s['error_msg'][:25]}"
    print(f"  {s['pdf'][:50]:<50} {s['time_total_s']:>6.1f}s ${s['cost_llm_usd']:>7.4f}  {status_str}")

if errors:
    print()
    for name, err in errors:
        print(f"  ✗ {name}: {err}")

hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    print(f"\n⚠ {len(hard_errors)} documento(s) falharam (os demais foram salvos):")
    for n, e in hard_errors:
        print(f"  ✗ {n}: {e[:120]}")
    if len(hard_errors) > len(rows) // 2:
        raise Exception(f"Mais da metade falhou ({len(hard_errors)}/{len(rows)}): {[n for n, _ in hard_errors]}")
