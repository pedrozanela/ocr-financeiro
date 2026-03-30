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

with open(f"{WORKSPACE_BASE}/model/output_schema.json") as f:
    output_schema = json.load(f)

with open(f"{WORKSPACE_BASE}/model/depara.json") as f:
    depara_data = json.load(f)

with open(f"{WORKSPACE_BASE}/model/regras_classificacao.json") as f:
    regras_data = json.load(f)

with open(f"{WORKSPACE_BASE}/model/few_shot_examples.json") as f:
    fewshot_data = json.load(f)

# ── helpers inline (agent.py importa mlflow, incompatível com Serverless) ──

_SECTION_LABELS = {
    "ativo_circulante":       "Ativo Circulante",
    "ativo_nao_circulante":   "Ativo Não Circulante",
    "ativo_permanente":       "Ativo Permanente",
    "passivo_circulante":     "Passivo Circulante",
    "passivo_nao_circulante": "Passivo Não Circulante",
    "patrimonio_liquido":     "Patrimônio Líquido",
    "dre":                    "DRE — Demonstração do Resultado",
}


def build_depara_section(depara: dict) -> str:
    sections: dict[str, list] = {}
    for path, entry in depara.items():
        top = path.split(".")[0]
        sections.setdefault(top, []).append((path, entry["conceito"], entry["aliases"]))
    lines = [
        "## DICIONÁRIO DE CONTAS (DE-PARA)", "",
        "Se o nome de uma linha do documento corresponder (exato ou similar) a um dos aliases abaixo,",
        "mapeie para o campo indicado. Quando houver ambiguidade, use o contexto da seção do balanço.", "",
    ]
    for section_key, field_entries in sections.items():
        label = _SECTION_LABELS.get(section_key, section_key)
        lines.append(f"### {label}")
        lines.append("")
        for path, conceito, aliases in field_entries:
            aliases_str = ", ".join(aliases) if isinstance(aliases, list) else aliases
            lines.append(f"**{path}** — {conceito}")
            lines.append(f"→ {aliases_str}")
            lines.append("")
    return "\n".join(lines)


def build_regras_section(regras: list) -> str:
    if not regras:
        return ""
    lines = [
        "## REGRAS DE CLASSIFICAÇÃO CONTÁBIL", "",
        "As regras abaixo são OBRIGATÓRIAS e têm prioridade sobre qualquer interpretação individual.", "",
    ]
    for r in regras:
        lines.append(f"### {r['id']}. {r['titulo']}")
        lines.append(r["regra"])
        lines.append("")
    return "\n".join(lines)


INSTRUCTIONS = (
    "* O documento pode conter MÚLTIPLAS colunas de dados: diferentes tipos de entidade "
    "(Consolidado, Controladora/Individual) e/ou diferentes períodos (datas de referência). "
    "Você DEVE extrair TODAS as combinações presentes, gerando um elemento no array para cada "
    "combinação única de (tipo_entidade, periodo). Exemplos comuns: "
    "[Consolidado 2024-12-31, Controladora 2024-12-31], "
    "[Consolidado 2024-12-31, Consolidado 2023-12-31], "
    "[Consolidado 2024-12-31, Controladora 2024-12-31, Consolidado 2023-12-31, Controladora 2023-12-31].\n"
    "* Para cada elemento, preencha `tipo_entidade` com CONSOLIDADO, CONTROLADORA ou INDIVIDUAL, "
    "conforme o cabeçalho da coluna correspondente no documento.\n"
    "* Substitua qualquer valor null, vazio ou não informado por zero.\n"
    "* Formate todos os números para exibir exatamente 2 casas decimais, usando ponto como separador, "
    "mesmo que o valor seja inteiro ou zero (ex: 834988.00, 0.00, 15.50).\n"
    "* Preencha o objeto `fontes` no JSON de saída: para cada campo extraído, indique qual texto exato do PDF "
    "originou o valor. Use o caminho do campo como chave (ex: 'ativo_circulante.impostos_a_recuperar') "
    "e como valor descreva brevemente: o(s) nome(s) da(s) linha(s) do documento, os valores individuais "
    "e a operação realizada (ex: soma, leitura direta). "
    "Exemplo: 'Impostos a recuperar (2.411) + IRPJ e CSLL a compensar (4.596) = 7.007 (escala: milhares)'. "
    "Se o valor foi lido diretamente de uma única linha, indique apenas o nome da linha e o valor. "
    "Inclua fontes apenas para campos com valor diferente de zero.\n\n"
)

SYSTEM_PROMPT_TEMPLATE = """\
Você é um especialista em análise de demonstrações financeiras brasileiras.
Sua tarefa é extrair informações estruturadas de documentos financeiros (Balanço Patrimonial e DRE).

{depara}

{regras}

## INSTRUÇÕES DE EXTRAÇÃO

{instructions}
{fewshot}
Retorne SOMENTE um JSON array válido seguindo exatamente o schema fornecido. Sem texto adicional.\
"""


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


SYSTEM_PROMPT_FULL = SYSTEM_PROMPT_TEMPLATE.format(
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
# MAGIC %md ## 3. Chamar FM API via pandas UDF (system + user roles, paralelo por Spark)
# MAGIC
# MAGIC `ai_query` neste workspace só aceita `StringType` — não suporta messages array.
# MAGIC Usamos um pandas UDF que chama o endpoint de chat completions diretamente.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# Auth: secrets (único método confiável em Serverless)
_TOKEN = dbutils.secrets.get("ocr-financeiro", "pat-servico")
_HOST  = spark.conf.get("spark.databricks.workspaceUrl", "e2-demo-field-eng.cloud.databricks.com")
if not _HOST.startswith("http"):
    _HOST = f"https://{_HOST}"
_URL   = f"{_HOST.rstrip('/')}/serving-endpoints/{MODEL}/invocations"

# Captura prompts via closure (picklados com o UDF, sem broadcast)
_SYS    = SYSTEM_PROMPT_FULL
_PREFIX = USER_PROMPT_PREFIX

print(f"Endpoint : {_URL}")
print(f"Token    : {_TOKEN[:8]}...")


@pandas_udf("string")
def _call_llm(texts: pd.Series) -> pd.Series:
    """Chama FM API para cada documento. Executa em paralelo nos workers Spark."""
    import json
    import time as _time
    import requests as _req

    _headers = {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}
    out = []

    for text in texts:
        payload = {
            "messages": [
                {"role": "system", "content": _SYS},
                {"role": "user",   "content": _PREFIX + text},
            ],
            "max_tokens": 64000,
            "temperature": 0,
        }
        last_err = None
        for attempt in range(5):
            try:
                resp = _req.post(_URL, headers=_headers, json=payload, timeout=600)
                if resp.status_code in (429, 503, 504):
                    wait = 60 * (attempt + 1)
                    _time.sleep(wait)
                    last_err = Exception(f"{resp.status_code} {resp.text[:100]}")
                    continue
                resp.raise_for_status()
                # /invocations pode retornar chat format (choices) ou predictions
                body = resp.json()
                if "choices" in body:
                    content = body["choices"][0]["message"]["content"]
                elif "predictions" in body:
                    p = body["predictions"]
                    content = p if isinstance(p, str) else (p[0] if isinstance(p, list) else str(p))
                else:
                    content = str(body)
                out.append(content)
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < 4:
                    _time.sleep(30 * (attempt + 1))

        if last_err is not None:
            out.append(json.dumps({"error": str(last_err)}))

    return pd.Series(out)


# Reparticiona para paralelismo real.
# MAX_PARTITIONS baixo evita rate limiting (429) no FM API.
MAX_PARTITIONS = 4
n_partitions = min(doc_count, MAX_PARTITIONS)
print(f"Partições : {n_partitions}")

batch_df = (
    docs_df.repartition(n_partitions)
    .withColumn("raw_response", _call_llm(col("document_text")))
    .select("document_name", "raw_response")
)

t0 = time.time()
results = batch_df.collect()
elapsed = time.time() - t0

print(f"✓ {len(results)} chamadas em {int(elapsed // 60)}m {int(elapsed % 60)}s")

# COMMAND ----------
# MAGIC %md ## 4. Parse e persistência

# COMMAND ----------

def _clean_raw(raw: str) -> str:
    """Remove delimitadores markdown e normaliza para array JSON."""
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


def _parse_json_robust(raw: str):
    """Parse JSON; se 'Extra data', tenta envolver múltiplos objetos em array."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        if "Extra data" not in str(e):
            raise
        # Múltiplos objetos JSON concatenados sem array — envolver em []
        import re
        objs = re.findall(r'\{(?:[^{}]|\{[^{}]*\})*\}', raw, re.DOTALL)
        if objs:
            candidate = "[" + ",".join(objs) + "]"
            return json.loads(candidate)
        raise


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
    raw_val  = row["raw_response"]

    raw = str(raw_val) if raw_val is not None else ""

    if not raw:
        errors.append((pdf_name, "resposta vazia do FM API"))
        print(f"  ✗ {pdf_name}: resposta vazia")
        continue

    try:
        parsed = _parse_json_robust(_clean_raw(raw))
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

hard_errors = [(n, e) for n, e in errors if "vazia" not in e and "no_text" not in e]

# Expõe detalhes de erro via notebook.exit (visível no API get-output)
summary = {
    "success": len(successes),
    "errors":  len(errors),
    "hard_errors": len(hard_errors),
    "elapsed_s": round(elapsed, 1),
    "first_errors": [(n, e[:300]) for n, e in hard_errors[:5]],
}

if hard_errors and len(hard_errors) > doc_count // 2:
    dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False))
else:
    dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False))
