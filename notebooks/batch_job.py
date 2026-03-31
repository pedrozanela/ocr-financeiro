# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Batch Job Diário
# MAGIC Detecta PDFs novos no volume UC e extrai dados financeiros via endpoint `techfin-ocr-v4`.
# MAGIC Usa `ai_parse_document` para extração de texto (suporta PDFs digitais e escaneados).

# COMMAND ----------

import json
import time
import requests
from pyspark.sql.functions import expr, concat_ws

# Configuração via widgets (compatível com DABs e Serverless)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("volume_path", "")
dbutils.widgets.text("endpoint", "extrator-financeiro")
dbutils.widgets.text("secret_scope", "ocr-financeiro")
dbutils.widgets.text("secret_key", "pat-servico")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")
VOLUME_PATH     = dbutils.widgets.get("volume_path")
RESULTS_TABLE   = f"{_cat}.{_sch}.resultados"
SOURCE_TABLE    = f"{_cat}.{_sch}.documentos"
OCR_ENDPOINT    = dbutils.widgets.get("endpoint")
DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if not DATABRICKS_HOST.startswith("http"):
    DATABRICKS_HOST = f"https://{DATABRICKS_HOST}"
ENDPOINT_URL    = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

TOKEN   = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key"))
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# COMMAND ----------
# MAGIC %md ## 1. Identificar PDFs novos

# COMMAND ----------

volume_pdfs = {f for f in dbutils.fs.ls(VOLUME_PATH) if f.name.lower().endswith(".pdf")}
volume_pdfs = {f.name for f in volume_pdfs}
processed   = {row["document_name"] for row in spark.sql(f"SELECT document_name FROM {RESULTS_TABLE}").collect()}
new_pdfs    = volume_pdfs - processed

print(f"Volume: {len(volume_pdfs)} PDFs | Já processados: {len(processed)} | Novos: {len(new_pdfs)}")

if not new_pdfs:
    print("Nenhum PDF novo encontrado. Encerrando.")
    dbutils.notebook.exit("no_new_pdfs")

# COMMAND ----------
# MAGIC %md ## 2. Processar PDFs novos

# COMMAND ----------

def extract_text_ai_parse(pdf_name: str) -> str:
    """Extrai texto do PDF usando ai_parse_document (suporta PDFs escaneados)."""
    volume_path = f"{VOLUME_PATH}/{pdf_name}"
    df = (
        spark.read.format("binaryFile").load(volume_path)
        .withColumn("parsed", expr("ai_parse_document(content)"))
        .withColumn("text", concat_ws("\n\n", expr("""
            transform(
                try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                element -> try_cast(element:content AS STRING)
            )
        """)))
        .select("text")
    )
    rows = df.collect()
    if not rows:
        return ""
    return rows[0]["text"] or ""


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

errors    = []
successes = []

for pdf_name in sorted(new_pdfs):
    print(f"\n→ {pdf_name}")
    try:
        text = extract_text_ai_parse(pdf_name)
        if not text or not text.strip():
            print("  ⚠ Sem texto extraível")
            errors.append((pdf_name, "no_text"))
            continue

        result = call_endpoint(text)

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

hard_errors = [(n, e) for n, e in errors if e != "no_text"]
if hard_errors:
    raise Exception(f"{len(hard_errors)} PDF(s) falharam no processamento: {[n for n, _ in hard_errors]}")
