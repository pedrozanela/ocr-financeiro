# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Batch via ai_query
# MAGIC
# MAGIC Processa documentos usando `ai_query` direto no Foundation Model API — sem passar pelo
# MAGIC serving endpoint `extrator-financeiro`.
# MAGIC
# MAGIC **Vantagens sobre `run_llm_from_table`:**
# MAGIC - Sem timeout 504/555 (Spark gerencia internamente)
# MAGIC - Paralelismo nativo do Spark (sem ThreadPoolExecutor manual)
# MAGIC - Mais barato: sem custo de endpoint ocioso
# MAGIC - Endpoint pode usar `scale_to_zero=true` para requests individuais da UI

# COMMAND ----------

import json
import sys
import time

from pyspark.sql.functions import col, concat, expr, lit

# --- Configuração via widgets ---
dbutils.widgets.text("catalog", "pedro_zanela")
dbutils.widgets.text("schema",  "ocr_financeiro")
dbutils.widgets.text("model",   "databricks-claude-sonnet-4-6")
dbutils.widgets.dropdown("filter", "all", ["all", "new", "failed"])

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
MODEL   = dbutils.widgets.get("model")
FILTER  = dbutils.widgets.get("filter")

SOURCE_TABLE  = f"{CATALOG}.{SCHEMA}.documentos"
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.resultados"
MODELO_VERSAO = f"batch-ai_query/{MODEL}"

PRICE_INPUT_PER_TOKEN  = 3.00  / 1_000_000  # Sonnet 4.6
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000

print(f"Source  : {SOURCE_TABLE}")
print(f"Results : {RESULTS_TABLE}")
print(f"Model   : {MODEL}")
print(f"Filter  : {FILTER}")

# COMMAND ----------
# MAGIC %md ## 1. Carregar artifacts e montar prompts

# COMMAND ----------

WORKSPACE_BASE = "/Workspace/Repos/pedro.zanela@databricks.com/ocr-financeiro"
sys.path.insert(0, WORKSPACE_BASE)

from model.agent import (
    build_depara_section,
    build_regras_section,
    INSTRUCTIONS,
    SYSTEM_PROMPT,
)

with open(f"{WORKSPACE_BASE}/model/output_schema.json") as f:
    output_schema = json.load(f)

with open(f"{WORKSPACE_BASE}/model/depara.json") as f:
    depara_data = json.load(f)

with open(f"{WORKSPACE_BASE}/model/regras_classificacao.json") as f:
    regras_data = json.load(f)

with open(f"{WORKSPACE_BASE}/model/few_shot_examples.json") as f:
    fewshot_data = json.load(f)


def _build_fewshot_section(examples: list) -> str:
    if not examples:
        return ""
    lines = [
        "\n## EXEMPLOS DE CORREÇÕES ANTERIORES",
        "",
        "Os exemplos abaixo representam erros recorrentes encontrados em extrações anteriores.",
        "Use-os como referência para evitar repetir os mesmos erros.",
        "",
    ]
    for i, ex in enumerate(examples, 1):
        freq = ex.get("frequencia", 1)
        lines.append(f"### {i}. `{ex['campo']}` ({freq}x corrigido)")
        if ex.get("fonte_doc"):
            lines.append(f"- Texto no documento: \"{ex['fonte_doc']}\"")
        lines.append(f"- Extração errada: {ex['valor_errado']}")
        lines.append(f"- Valor correto: {ex['valor_correto']}")
        lines.append(f"- Motivo: {ex['explicacao']}")
        lines.append("")
    return "\n".join(lines)


SYSTEM_PROMPT_FULL = SYSTEM_PROMPT.format(
    depara=build_depara_section(depara_data),
    regras=build_regras_section(regras_data),
    instructions=INSTRUCTIONS,
    fewshot=_build_fewshot_section(fewshot_data),
)

schema_str = json.dumps(output_schema, ensure_ascii=False, indent=2)
USER_PROMPT_PREFIX = (
    "Extraia as informações financeiras do seguinte documento e retorne um JSON "
    f"seguindo exatamente este schema:\n\n{schema_str}\n\n"
    "DOCUMENTO:\n"
)

print(f"System prompt : {len(SYSTEM_PROMPT_FULL):,} chars")
print(f"User prefix   : {len(USER_PROMPT_PREFIX):,} chars")
print(f"Few-shot      : {len(fewshot_data)} exemplos")

# COMMAND ----------
# MAGIC %md ## 2. Selecionar documentos a processar

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
# MAGIC %md ## 3. Executar ai_query em batch (paralelo nativo do Spark)

# COMMAND ----------

# system_prompt e user_prefix são adicionados como colunas lit() —
# Spark gerencia o escape automaticamente, sem risco de injeção SQL.
batch_df = (
    docs_df
    .withColumn("_sys",  lit(SYSTEM_PROMPT_FULL))
    .withColumn("_user", concat(lit(USER_PROMPT_PREFIX), col("document_text")))
    .withColumn(
        "raw_response",
        expr(f"""
            ai_query(
              '{MODEL}',
              named_struct(
                'messages', array(
                  named_struct('role', 'system', 'content', _sys),
                  named_struct('role', 'user',   'content', _user)
                ),
                'max_tokens',  64000,
                'temperature', cast(0 as double)
              ),
              'STRING',
              false
            )
        """),
    )
    .select("document_name", "raw_response")
)

t0 = time.time()
results = batch_df.collect()  # dispara ai_query em paralelo por Spark
elapsed = time.time() - t0

print(f"✓ {len(results)} chamadas em {int(elapsed // 60)}m {int(elapsed % 60)}s")

# COMMAND ----------
# MAGIC %md ## 4. Parse e persistência

# COMMAND ----------

def _clean_raw(raw: str) -> str:
    """Remove delimitadores markdown se o modelo os incluiu."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.split("```")[0].strip()
    elif not (raw.startswith("[") or raw.startswith("{")):
        for start in ["[{", "[ {", "[\n{", "[\r\n{", "[  {"]:
            idx = raw.find(start)
            if idx >= 0:
                return raw[idx:]
        idx = raw.find("[")
        if idx >= 0:
            raw = raw[idx:]
    return raw


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
        r.pop("_assessment", None)
        r.pop("_usage", None)

        ej   = json.dumps(r, ensure_ascii=False).replace("'", "''")
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
                assessment_json    = '[]',
                token_usage_json   = '{{}}',
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
                 '[]', '{{}}', '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{mv}')
        """)
        saved += 1

    return saved


successes, errors = [], []

for row in results:
    pdf_name = row["document_name"]
    raw      = row["raw_response"] or ""

    if not raw:
        errors.append((pdf_name, "ai_query retornou NULL (failOnError=false)"))
        print(f"  ✗ {pdf_name}: ai_query NULL")
        continue

    try:
        parsed = json.loads(_clean_raw(raw))
        if isinstance(parsed, dict):
            parsed = [parsed]

        valid = [r for r in parsed if not (isinstance(r, dict) and r.get("error"))]
        if not valid:
            raise ValueError(f"Sem registros válidos: {raw[:200]}")

        saved  = _save_records(pdf_name, valid)
        combos = [(_get_nested(r, "tipo_entidade"), _get_nested(r, "identificacao.periodo")) for r in valid]
        print(f"  ✓ {pdf_name}: {saved} registro(s) {combos}")
        successes.append(pdf_name)

    except Exception as e:
        errors.append((pdf_name, str(e)))
        print(f"  ✗ {pdf_name}: {e}")

# COMMAND ----------
# MAGIC %md ## 5. Relatório

# COMMAND ----------

print("=" * 65)
print(f"  RELATÓRIO — Batch ai_query ({MODEL})")
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

hard_errors = [(n, e) for n, e in errors if "NULL" not in e and "no_text" not in e]
if hard_errors and len(hard_errors) > doc_count // 2:
    raise Exception(
        f"Mais da metade falhou ({len(hard_errors)}/{doc_count}): "
        f"{[n for n, _ in hard_errors]}"
    )
