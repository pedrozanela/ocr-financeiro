# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR v4 — Avaliação e Melhoria Contínua
# MAGIC
# MAGIC Fluxo:
# MAGIC 1. **Extrair** documentos → captura traces automáticos
# MAGIC 2. **Revisar** → registrar correções na tabela `new_ocr_techfin_corrections`
# MAGIC 3. **Avaliar** → mlflow.genai.evaluate() compara output vs. correção
# MAGIC 4. **Melhorar** → iteração no system prompt guiada pelos erros encontrados

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.1.0" openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import mlflow
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# COMMAND ----------

# ── Configuração ───────────────────────────────────────────────────────────────
WORKSPACE_URL  = "https://e2-demo-field-eng.cloud.databricks.com"
ENDPOINT_NAME  = "techfin-ocr-v4"
INPUT_TABLE    = "pedro_zanela.ia.new_ocr_techfin"
RESULTS_TABLE  = "pedro_zanela.ia.new_ocr_techfin_results"
CORRECTIONS_TABLE = "pedro_zanela.ia.new_ocr_techfin_corrections"
EXPERIMENT_PATH = "/Users/pedro.zanela@databricks.com/techfin-ocr-v4-eval"

TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_PATH)

# Ativa tracing automático nas chamadas OpenAI/HTTP feitas pelo endpoint
mlflow.openai.autolog()

# COMMAND ----------

# MAGIC %md ## Parte 1 — Extrair com tracing
# MAGIC
# MAGIC Chama o endpoint com `@mlflow.trace` para que cada extração gere um trace
# MAGIC no MLflow Experiment. Você poderá inspecionar cada chamada na UI.

# COMMAND ----------

@mlflow.trace(name="techfin_extract")
def extract_document(document_name: str, document_text: str) -> dict:
    """Chama techfin-ocr-v4 e retorna o JSON extraído. Gera trace automático."""
    resp = requests.post(
        f"{WORKSPACE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        json={"dataframe_records": [{"text": document_text}]},
        timeout=180,
    )
    resp.raise_for_status()
    body = resp.json()
    result = body.get("predictions", body)
    if isinstance(result, list):
        result = result[0]
    if isinstance(result, str):
        result = json.loads(result)

    # Adiciona metadados ao trace para facilitar busca
    mlflow.set_tag("document_name", document_name)
    return result


# Executar extração com tracing para todos os documentos
rows = spark.table(INPUT_TABLE).collect()
results = []

with mlflow.start_run(run_name="batch_extraction"):
    for i, row in enumerate(rows):
        print(f"[{i+1}/{len(rows)}] {row['document_name']}")
        try:
            extracted = extract_document(row["document_name"], row["document_text"])
            results.append({
                "document_name": row["document_name"],
                "extracted_json": json.dumps(extracted, ensure_ascii=False),
                "status": "ok",
            })
        except Exception as e:
            results.append({"document_name": row["document_name"], "extracted_json": "{}", "status": f"error: {e}"})

print(f"\n✓ {sum(1 for r in results if r['status'] == 'ok')}/{len(results)} documentos processados")

# COMMAND ----------

# MAGIC %md ## Parte 2 — Registrar correções
# MAGIC
# MAGIC Quando o modelo erra, você preenche a tabela de correções com o valor correto.
# MAGIC Ela serve como **ground truth** para a avaliação.
# MAGIC
# MAGIC Estrutura: document_name | campo | valor_extraido | valor_correto | comentario

# COMMAND ----------

# Criar tabela de correções se não existir
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CORRECTIONS_TABLE} (
    document_name STRING,
    campo         STRING COMMENT 'Ex: ativo_total, dre.lucro_liquido, razao_social',
    valor_extraido STRING,
    valor_correto  STRING,
    comentario    STRING,
    criado_em     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Correções manuais das extrações do TechFin OCR v4'
""")
print(f"Tabela {CORRECTIONS_TABLE} pronta.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como registrar uma correção
# MAGIC
# MAGIC Execute o cell abaixo sempre que encontrar um erro.
# MAGIC Exemplos de campos: `razao_social`, `ativo_total`, `dre.lucro_liquido`, `identificacao.periodo`

# COMMAND ----------

# ── EDITE AQUI para registrar uma correção ─────────────────────────────────────
nova_correcao = {
    "document_name": "AD FOODS INDUSTRIA BP 2023.pdf",  # nome do arquivo
    "campo":          "ativo_total",                      # campo errado
    "valor_extraido": "34400976.09",                      # o que o modelo disse
    "valor_correto":  "34400976.09",                      # o valor certo
    "comentario":     "Exemplo — altere conforme necessário",
}

spark.createDataFrame([nova_correcao]).write.mode("append").saveAsTable(CORRECTIONS_TABLE)
display(spark.table(CORRECTIONS_TABLE))

# COMMAND ----------

# MAGIC %md ## Parte 3 — Avaliação com MLflow
# MAGIC
# MAGIC Compara a extração atual com as correções registradas.
# MAGIC Gera métricas de acurácia por campo e por documento.

# COMMAND ----------

from mlflow.genai.scorers import scorer
from mlflow.entities import Feedback

@scorer
def numeric_accuracy(outputs, expectations):
    """Verifica se valores numéricos estão dentro de 1% do valor correto."""
    if not expectations:
        return Feedback(name="numeric_accuracy", value=None, rationale="Sem correção registrada")

    campo = expectations.get("campo", "")
    valor_correto_str = expectations.get("valor_correto", "")

    # Navegar no JSON extraído usando o caminho do campo (ex: "dre.lucro_liquido")
    parts = campo.split(".")
    val = outputs if isinstance(outputs, dict) else {}
    for p in parts:
        val = val.get(p) if isinstance(val, dict) else None
        if val is None:
            break

    try:
        extraido = float(str(val).replace(",", "."))
        correto  = float(str(valor_correto_str).replace(",", "."))
        if correto == 0:
            ok = extraido == 0
        else:
            ok = abs(extraido - correto) / abs(correto) <= 0.01  # tolerância 1%
        return Feedback(
            name="numeric_accuracy",
            value=ok,
            rationale=f"Extraído: {extraido} | Correto: {correto} | Delta: {abs(extraido - correto):.2f}"
        )
    except (TypeError, ValueError):
        # Campo texto — comparação exata
        ok = str(val).strip().lower() == str(valor_correto_str).strip().lower()
        return Feedback(name="text_match", value=ok, rationale=f"Extraído: '{val}' | Correto: '{valor_correto_str}'")


@scorer
def json_structure_valid(outputs):
    """Verifica se o output tem os campos obrigatórios do schema."""
    required_top = ["razao_social", "cnpj", "identificacao", "ativo_circulante",
                    "ativo_nao_circulante", "ativo_permanente", "ativo_total",
                    "passivo_circulante", "passivo_nao_circulante", "patrimonio_liquido",
                    "passivo_total", "dre"]
    if not isinstance(outputs, dict):
        return Feedback(value=False, rationale="Output não é um dict")
    missing = [f for f in required_top if f not in outputs]
    if missing:
        return Feedback(value=False, rationale=f"Campos ausentes: {missing}")
    return Feedback(value=True, rationale="Estrutura OK")


@scorer
def balanco_equilibrado(outputs):
    """Verifica se Ativo Total ≈ Passivo Total (tolerância 1%)."""
    if not isinstance(outputs, dict):
        return Feedback(value=None, rationale="Output inválido")
    try:
        ativo   = float(outputs.get("ativo_total") or 0)
        passivo = float(outputs.get("passivo_total") or 0)
        if ativo == 0:
            return Feedback(value=None, rationale="Ativo Total é zero")
        delta_pct = abs(ativo - passivo) / abs(ativo) * 100
        ok = delta_pct <= 1.0
        return Feedback(
            value=ok,
            rationale=f"Ativo: {ativo:,.2f} | Passivo: {passivo:,.2f} | Delta: {delta_pct:.2f}%"
        )
    except (TypeError, ValueError) as e:
        return Feedback(value=None, rationale=f"Erro ao calcular: {e}")

# COMMAND ----------

# Montar dataset de avaliação:
# - inputs: texto do documento
# - outputs: o que o modelo extraiu (já calculado no batch)
# - expectations: correções registradas (se houver)

corrections_df = spark.table(CORRECTIONS_TABLE).toPandas()
results_lookup = {r["document_name"]: json.loads(r["extracted_json"]) for r in results}

rows_df = spark.table(INPUT_TABLE).toPandas()

eval_data = []
for _, row in rows_df.iterrows():
    doc_name = row["document_name"]
    doc_text = row["document_text"]
    extracted = results_lookup.get(doc_name, {})

    # Pegar correção para este documento (se houver)
    doc_corrections = corrections_df[corrections_df["document_name"] == doc_name]
    expectations = None
    if not doc_corrections.empty:
        corr = doc_corrections.iloc[0]
        expectations = {
            "campo":         corr["campo"],
            "valor_correto": corr["valor_correto"],
        }

    record = {
        "inputs":  {"text": doc_text[:4000], "document_name": doc_name},  # truncar para eval rápida
        "outputs": extracted,
    }
    if expectations:
        record["expectations"] = expectations

    eval_data.append(record)

print(f"Dataset de avaliação: {len(eval_data)} docs, {len(corrections_df)} correções registradas")

# COMMAND ----------

# Rodar avaliação
def predict_fn(text, document_name=None):
    """Usado pelo mlflow.genai.evaluate() para re-executar o modelo se necessário."""
    return extract_document(document_name or "unknown", text)

eval_results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,       # re-executa o modelo para gerar novos traces
    scorers=[
        json_structure_valid,
        balanco_equilibrado,
        numeric_accuracy,
    ],
)

print("\n── Métricas Agregadas ──────────────────────────────")
for metric, value in eval_results.metrics.items():
    print(f"  {metric}: {value:.2%}" if isinstance(value, float) else f"  {metric}: {value}")

# COMMAND ----------

# Resultados detalhados por documento
display(eval_results.tables["eval_results"])

# COMMAND ----------

# MAGIC %md ## Parte 4 — Melhorar o agente
# MAGIC
# MAGIC Com base nos erros encontrados, há 2 formas de melhorar:
# MAGIC
# MAGIC ### Opção A — Ajuste manual do system prompt (mais simples)
# MAGIC Edite o `SYSTEM_PROMPT` em `agent.py` com base nos padrões de erro observados
# MAGIC e faça um novo deploy. Repita a avaliação para confirmar a melhora.
# MAGIC
# MAGIC ### Opção B — Otimização automática com GEPA (MLflow >= 3.5)
# MAGIC Use `mlflow.genai.optimize_prompts()` para que o MLflow itere o prompt
# MAGIC automaticamente usando os pares (documento, correção) como exemplos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção B: GEPA — Otimização automática de prompt
# MAGIC
# MAGIC Para usar, você precisa ter correções suficientes registradas (mínimo 5-10).

# COMMAND ----------

# Montar dataset de otimização (inputs + expectations com expected_response)
corrections_for_opt = corrections_df.copy()
opt_data = []

for doc_name, group in corrections_for_opt.groupby("document_name"):
    doc_row = rows_df[rows_df["document_name"] == doc_name]
    if doc_row.empty:
        continue
    doc_text = doc_row.iloc[0]["document_text"]

    # Criar expected_response a partir das correções acumuladas
    correction_notes = "\n".join(
        f"- Campo '{r.campo}': deve ser '{r.valor_correto}' (era '{r.valor_extraido}'). {r.comentario or ''}"
        for _, r in group.iterrows()
    )
    expected = (
        f"A extração deve corrigir os seguintes campos:\n{correction_notes}\n"
        f"Os demais campos devem ser extraídos normalmente seguindo o schema."
    )

    opt_data.append({
        "inputs":       {"text": doc_text[:4000], "document_name": doc_name},
        "expectations": {"expected_response": expected},
    })

print(f"Dataset de otimização: {len(opt_data)} documentos com correções")

if len(opt_data) >= 5:
    print("\nPronto para rodar mlflow.genai.optimize_prompts() — descomente o bloco abaixo.")
else:
    print(f"\nAinda faltam {5 - len(opt_data)} documentos corrigidos para ativar a otimização automática.")
    print("Continue adicionando correções na Parte 2 e re-execute este notebook.")

# COMMAND ----------

# ── Descomente quando tiver correções suficientes ─────────────────────────────
# from mlflow.genai.optimizers import GepaPromptOptimizer
#
# result = mlflow.genai.optimize_prompts(
#     predict_fn=predict_fn,
#     train_data=opt_data,
#     prompt_uris=[],           # Registre o system_prompt no MLflow Prompt Registry primeiro
#     optimizer=GepaPromptOptimizer(
#         reflection_model="databricks:/databricks-claude-3-7-sonnet",
#         max_metric_calls=30,
#     ),
#     scorers=[json_structure_valid, balanco_equilibrado, numeric_accuracy],
# )
# print(f"Score inicial: {result.initial_eval_score:.2%}")
# print(f"Score final:   {result.final_eval_score:.2%}")
# print("\nPrompt otimizado:")
# print(result.optimized_prompts[0].template)
