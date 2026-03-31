# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Batch via endpoint extrator-financeiro
# MAGIC
# MAGIC Processa documentos chamando o serving endpoint `extrator-financeiro` em paralelo
# MAGIC via pandas UDF — sem ThreadPoolExecutor manual.
# MAGIC
# MAGIC **Vantagens sobre `run_llm_from_table`:**
# MAGIC - Usa o modelo MLflow registrado completo (v8+): Judge, recovery, few-shot
# MAGIC - Paralelismo nativo do Spark (uma partição por doc, sem ThreadPoolExecutor)
# MAGIC - Qualquer upgrade do modelo (v9, v10…) é refletido automaticamente

# COMMAND ----------

import json
import time

from pyspark.sql.functions import col, pandas_udf
import pandas as pd

# --- Configuração via widgets ---
dbutils.widgets.text("catalog",  "pedro_zanela")
dbutils.widgets.text("schema",   "ocr_financeiro")
dbutils.widgets.text("endpoint", "extrator-financeiro")
dbutils.widgets.dropdown("filter", "all", ["all", "new", "failed"])

CATALOG  = dbutils.widgets.get("catalog")
SCHEMA   = dbutils.widgets.get("schema")
ENDPOINT = dbutils.widgets.get("endpoint")
FILTER   = dbutils.widgets.get("filter")

SOURCE_TABLE  = f"{CATALOG}.{SCHEMA}.documentos"
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.resultados"
MODELO_VERSAO = f"batch/{ENDPOINT}"

_TOKEN = dbutils.secrets.get("ocr-financeiro", "pat-servico")
_HOST  = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
if not _HOST.startswith("http"):
    _HOST = f"https://{_HOST}"
_URL = f"{_HOST.rstrip('/')}/serving-endpoints/{ENDPOINT}/invocations"

print(f"Source   : {SOURCE_TABLE}")
print(f"Results  : {RESULTS_TABLE}")
print(f"Endpoint : {_URL}")
print(f"Filter   : {FILTER}")

# COMMAND ----------
# MAGIC %md ## 1. Selecionar documentos a processar

# COMMAND ----------

if FILTER == "new":
    docs_df = spark.sql(f"""
        SELECT d.document_name, d.document_text
        FROM {SOURCE_TABLE} d
        LEFT JOIN {RESULTS_TABLE} r ON d.document_name = r.document_name
        WHERE r.document_name IS NULL
          AND d.document_text IS NOT NULL
          AND length(trim(d.document_text)) > 0
    """)
elif FILTER == "failed":
    docs_df = spark.sql(f"""
        SELECT d.document_name, d.document_text
        FROM {SOURCE_TABLE} d
        LEFT JOIN (
            SELECT document_name
            FROM {RESULTS_TABLE}
            WHERE extracted_json NOT LIKE '%"error"%'
            GROUP BY document_name
        ) ok ON d.document_name = ok.document_name
        WHERE ok.document_name IS NULL
          AND d.document_text IS NOT NULL
          AND length(trim(d.document_text)) > 0
    """)
else:  # all
    docs_df = spark.sql(f"""
        SELECT document_name, document_text
        FROM {SOURCE_TABLE}
        WHERE document_text IS NOT NULL
          AND length(trim(document_text)) > 0
    """)

doc_count = docs_df.count()
print(f"Documentos selecionados ({FILTER}): {doc_count}")

if doc_count == 0:
    print("Nenhum documento para processar. Encerrando.")
    dbutils.notebook.exit("no_documents")

# COMMAND ----------
# MAGIC %md ## 2. Chamar endpoint via pandas UDF (paralelo por Spark)

# COMMAND ----------

# MAX_PARTITIONS controla o grau de paralelismo.
# Valor baixo evita sobrecarregar o endpoint (429/504).
MAX_PARTITIONS = 4
n_partitions = min(doc_count, MAX_PARTITIONS)
print(f"Partições : {n_partitions}")


@pandas_udf("string")
def _call_endpoint(texts: pd.Series) -> pd.Series:
    """Chama extrator-financeiro para cada documento. Roda em paralelo nos workers."""
    import json
    import time as _time
    import requests as _req

    _headers = {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}
    out = []

    for text in texts:
        last_err = None
        for attempt in range(5):
            try:
                resp = _req.post(
                    _URL,
                    headers=_headers,
                    json={"dataframe_records": [{"text": text}]},
                    timeout=600,
                )
                if resp.status_code in (429, 503, 504, 555):
                    wait = 60 * (attempt + 1)
                    last_err = Exception(f"HTTP {resp.status_code} — aguardando {wait}s")
                    _time.sleep(wait)
                    continue
                resp.raise_for_status()

                body = resp.json()
                r = body.get("predictions", body)
                # predictions é [[rec1, rec2, ...]] — uma lista por input
                if isinstance(r, list) and r and isinstance(r[0], list):
                    r = r[0]
                if isinstance(r, str):
                    r = json.loads(r)

                out.append(json.dumps(r, ensure_ascii=False))
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < 4:
                    _time.sleep(30 * (attempt + 1))

        if last_err is not None:
            out.append(json.dumps([{"error": str(last_err)}], ensure_ascii=False))

    return pd.Series(out)


batch_df = (
    docs_df.repartition(n_partitions)
    .withColumn("raw_response", _call_endpoint(col("document_text")))
    .select("document_name", "raw_response")
)

t0 = time.time()
results = batch_df.collect()
elapsed = time.time() - t0

print(f"✓ {len(results)} chamadas em {int(elapsed // 60)}m {int(elapsed % 60)}s")

# COMMAND ----------
# MAGIC %md ## 3. Parse e persistência

# COMMAND ----------

def _get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _save_records(pdf_name: str, records: list) -> int:
    """MERGE INTO resultados. Retorna nº de registros salvos."""
    saved = 0

    def esc(v):
        return str(v or "").replace("'", "''")

    doc = pdf_name.replace("'", "''")
    mv  = MODELO_VERSAO.replace("'", "''")

    for r in records:
        if isinstance(r, dict) and r.get("error"):
            continue
        r = dict(r)
        assessment = r.pop("_assessment", [])
        usage      = r.pop("_usage", {})

        ej   = json.dumps(r,          ensure_ascii=False).replace("'", "''")
        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        uj   = json.dumps(usage,      ensure_ascii=False).replace("'", "''")
        rs   = esc(_get_nested(r, "razao_social"))
        cnpj = esc(_get_nested(r, "cnpj"))
        per  = esc(_get_nested(r, "identificacao.periodo"))
        te   = esc(_get_nested(r, "tipo_entidade"))
        td   = esc(_get_nested(r, "identificacao.tipo_demonstrativo"))
        moe  = esc(_get_nested(r, "identificacao.moeda"))
        escv = esc(_get_nested(r, "identificacao.escala_valores"))

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
                modelo_versao      = '{mv}'
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json,
                 assessment_json, token_usage_json, razao_social, cnpj,
                 tipo_demonstrativo, moeda, escala_valores,
                 processado_em, modelo_versao)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}',
                 '{aj}', '{uj}', '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{mv}')
        """)
        saved += 1

    return saved


successes, errors = [], []

for row in results:
    pdf_name = row["document_name"]
    raw      = row["raw_response"] or ""

    if not raw:
        errors.append((pdf_name, "resposta vazia"))
        print(f"  ✗ {pdf_name}: resposta vazia")
        continue

    try:
        records = json.loads(raw)
        if isinstance(records, dict):
            records = [records]

        valid = [r for r in records if not (isinstance(r, dict) and r.get("error"))]
        if not valid:
            err_msg = records[0].get("error", "sem registros válidos") if records else "lista vazia"
            raise ValueError(err_msg)

        saved  = _save_records(pdf_name, valid)
        combos = [(_get_nested(r, "tipo_entidade"), _get_nested(r, "identificacao.periodo")) for r in valid]
        print(f"  ✓ {pdf_name}: {saved} registro(s) {combos}")
        successes.append(pdf_name)

    except Exception as e:
        errors.append((pdf_name, str(e)))
        print(f"  ✗ {pdf_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 4. Relatório

# COMMAND ----------

print("=" * 65)
print(f"  RELATÓRIO — Batch {ENDPOINT}")
print("=" * 65)
print(f"  Documentos        : {doc_count}")
print(f"  Sucesso           : {len(successes)}")
print(f"  Erros             : {len(errors)}")
print(f"  Tempo total       : {int(elapsed // 60)}m {int(elapsed % 60)}s")
if doc_count > 0:
    print(f"  Média por doc     : {elapsed / doc_count:.1f}s")
print("=" * 65)

if errors:
    print("\nErros:")
    for name, err in errors:
        print(f"  ✗ {name}: {err[:120]}")

hard_errors = [(n, e) for n, e in errors if "vazia" not in e and "no_text" not in e]

summary = {
    "success":     len(successes),
    "errors":      len(errors),
    "hard_errors": len(hard_errors),
    "elapsed_s":   round(elapsed, 1),
    "first_errors": [(n, e[:300]) for n, e in hard_errors[:5]],
}
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False))
