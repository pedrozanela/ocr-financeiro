# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Reprocessar TUDO
# MAGIC Reprocessa todos os PDFs do volume com o modelo atual, sobrescrevendo resultados anteriores.
# MAGIC Rastreia tempo de execução e custo estimado de cada componente.
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
dbutils.widgets.text("catalog",       "cedip_fevm_aws_classic_stable_catalog")
dbutils.widgets.text("schema",        "ocr_financeiro")
dbutils.widgets.text("volume_path",   "/Volumes/cedip_fevm_aws_classic_stable_catalog/ocr_financeiro/documentos_pdf")
dbutils.widgets.text("endpoint",      "extrator-financeiro")
dbutils.widgets.text("secret_scope",  "ocr-financeiro")
dbutils.widgets.text("secret_key",    "pat-servico")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH     = dbutils.widgets.get("volume_path")
RESULTS_TABLE   = f"{_cat}.{_sch}.resultados"
OCR_ENDPOINT    = dbutils.widgets.get("endpoint")
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
if not DATABRICKS_HOST.startswith("http"):
    DATABRICKS_HOST = f"https://{DATABRICKS_HOST}"
ENDPOINT_URL    = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"
SECRET_SCOPE    = dbutils.widgets.get("secret_scope")
SECRET_KEY      = dbutils.widgets.get("secret_key")

# Preços claude-sonnet-4-6 (Foundation Model API, por token)
PRICE_INPUT_PER_TOKEN  = 3.00 / 1_000_000   # $3.00 por 1M tokens entrada
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000  # $15.00 por 1M tokens saída

# ai_parse_document: ~$0.015 por página (Serverless SQL DBU)
PRICE_AI_PARSE_PER_PAGE = 0.015

TOKEN   = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------
# MAGIC %md ## 1. Listar todos os PDFs no volume

# COMMAND ----------

all_files = dbutils.fs.ls(VOLUME_PATH)
all_pdfs  = sorted([f.name for f in all_files if f.name.lower().endswith(".pdf")])

print(f"Total de PDFs no volume: {len(all_pdfs)}")
for name in all_pdfs:
    print(f"  • {name}")

# COMMAND ----------
# MAGIC %md ## 2. Funções de extração e chamada

# COMMAND ----------

def extract_text_ai_parse(pdf_name: str) -> tuple[str, int]:
    """Extrai texto + conta páginas usando ai_parse_document.
    Retorna (texto, num_paginas).
    """
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
    text      = rows[0]["text"] or ""
    num_pages = int(rows[0]["num_pages"] or 0)
    return text, num_pages


def call_endpoint(text: str, max_retries: int = 3) -> tuple[dict, int, int]:
    """Chama o endpoint OCR.
    Retorna (resultado, input_tokens, output_tokens).
    Tokens são estimados a partir do comprimento do texto se não disponíveis na resposta.
    """
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
            body = resp.json()

            # Extrair tokens do metadata do endpoint (se disponível)
            metadata     = body.get("metadata", {})
            input_tokens  = int(metadata.get("input_tokens",  0))
            output_tokens = int(metadata.get("output_tokens", 0))

            # Fallback: estimar tokens por comprimento (aproximação: ~4 chars/token)
            if input_tokens == 0:
                input_tokens = len(text) // 4
            if output_tokens == 0:
                output_tokens = 2000  # estimativa conservadora para JSON de saída

            r = body.get("predictions", body)
            if isinstance(r, list) and len(r) == 1:
                r = r[0]
            if isinstance(r, str):
                r = json.loads(r)
            return r, input_tokens, output_tokens

        except requests.exceptions.Timeout:
            wait = 60 * (attempt + 1)
            print(f"    Timeout local — aguardando {wait}s antes de retry {attempt+1}/{max_retries}")
            time.sleep(wait)
    raise Exception(f"Endpoint falhou após {max_retries} tentativas")


def get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def save_result(pdf_name: str, results):
    """Salva array de resultados — múltiplas linhas por PDF (tipo_entidade × periodo).
    Usa MERGE para sobrescrever resultados anteriores.
    """
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
# MAGIC %md ## 3. Reprocessar todos os PDFs

# COMMAND ----------

# Métricas globais
total_start = time.time()

stats = []  # list of dicts per PDF

errors    = []
successes = []

for pdf_name in all_pdfs:
    doc_start = time.time()
    print(f"\n→ {pdf_name}")

    doc_stat = {
        "pdf":            pdf_name,
        "status":         "error",
        "time_parse_s":   0.0,
        "time_llm_s":     0.0,
        "time_save_s":    0.0,
        "time_total_s":   0.0,
        "pages":          0,
        "input_tokens":   0,
        "output_tokens":  0,
        "cost_parse_usd": 0.0,
        "cost_llm_usd":   0.0,
        "cost_total_usd": 0.0,
        "records":        0,
        "error_msg":      "",
    }

    try:
        # --- ai_parse_document ---
        t0 = time.time()
        text, num_pages = extract_text_ai_parse(pdf_name)
        doc_stat["time_parse_s"] = round(time.time() - t0, 2)
        doc_stat["pages"]        = num_pages
        doc_stat["cost_parse_usd"] = round(num_pages * PRICE_AI_PARSE_PER_PAGE, 4)
        print(f"  ai_parse: {num_pages} página(s) em {doc_stat['time_parse_s']}s  (${doc_stat['cost_parse_usd']:.4f})")

        if not text or not text.strip():
            print("  ⚠ Sem texto extraível")
            doc_stat["error_msg"] = "no_text"
            errors.append((pdf_name, "no_text"))
            stats.append(doc_stat)
            continue

        # --- Endpoint LLM ---
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

        # --- Salvar resultado ---
        t2 = time.time()
        save_result(pdf_name, result)
        doc_stat["time_save_s"] = round(time.time() - t2, 2)
        doc_stat["records"]     = len(result)
        doc_stat["status"]      = "ok"

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
# MAGIC %md ## 4. Relatório de Custos e Tempo

# COMMAND ----------

import math

# Agregados
total_pages        = sum(s["pages"]          for s in stats)
total_input_tokens = sum(s["input_tokens"]   for s in stats)
total_output_tokens= sum(s["output_tokens"]  for s in stats)
total_cost_parse   = sum(s["cost_parse_usd"] for s in stats)
total_cost_llm     = sum(s["cost_llm_usd"]   for s in stats)
total_cost         = total_cost_parse + total_cost_llm

print("=" * 72)
print(f"  RELATÓRIO FINAL — TechFin OCR Reprocessamento Total")
print("=" * 72)
print(f"  Tempo total de execução : {int(total_elapsed // 60)}m {int(total_elapsed % 60)}s  ({total_elapsed:.1f}s)")
print(f"  PDFs processados        : {len(successes)} / {len(all_pdfs)}")
print(f"  Erros                   : {len(errors)}")
print()
print("  ── ai_parse_document ──────────────────────────────────────────")
print(f"  Páginas processadas     : {total_pages}")
print(f"  Custo estimado          : ${total_cost_parse:.4f}  (@$0.015/página)")
print()
print("  ── Foundation Model API (claude-sonnet-4-6) ───────────────────")
print(f"  Tokens de entrada       : {total_input_tokens:,}  (${total_input_tokens * PRICE_INPUT_PER_TOKEN:.4f})")
print(f"  Tokens de saída         : {total_output_tokens:,}  (${total_output_tokens * PRICE_OUTPUT_PER_TOKEN:.4f})")
print(f"  Custo estimado LLM      : ${total_cost_llm:.4f}")
print()
print("  ── TOTAL ───────────────────────────────────────────────────────")
print(f"  Custo total estimado    : ${total_cost:.4f}")
print("=" * 72)

print()
print("  ── Por documento ───────────────────────────────────────────────")
print(f"  {'PDF':<45} {'Páginas':>7} {'Tempo':>7} {'Custo':>8}  Status")
print(f"  {'-'*45} {'-'*7} {'-'*7} {'-'*8}  ------")
for s in stats:
    name_short = s["pdf"][:44]
    status_str = "✓" if s["status"] == "ok" else f"✗ {s['error_msg'][:20]}"
    print(f"  {name_short:<45} {s['pages']:>7} {s['time_total_s']:>6.1f}s ${s['cost_total_usd']:>7.4f}  {status_str}")

if errors:
    print()
    print("  ── Erros ───────────────────────────────────────────────────────")
    for name, err in errors:
        print(f"  ✗ {name}: {err}")

hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    raise Exception(f"{len(hard_errors)} PDF(s) falharam no processamento: {[n for n, _ in hard_errors]}")
