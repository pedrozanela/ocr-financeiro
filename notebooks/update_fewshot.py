# Databricks notebook source
# MAGIC %md
# MAGIC # Atualizar Modelo com Correções (Few-Shot Feedback Loop)
# MAGIC
# MAGIC Lê correções da tabela, gera few-shot examples, loga nova versão do modelo e atualiza o endpoint.
# MAGIC Pode ser executado manualmente ou agendado como Job semanal.

# COMMAND ----------

# MAGIC %pip install openai>=1.0.0 mlflow>=2.10.0
# MAGIC %restart_python

# COMMAND ----------

import json
import os
from collections import defaultdict

# Parametros (injetados via DABs job ou widgets manuais)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("secret_scope", "ocr-financeiro")
dbutils.widgets.text("secret_key", "pat-servico")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

CORRECTIONS_TABLE = f"{catalog}.{schema}.correcoes"
RESULTS_TABLE = f"{catalog}.{schema}.resultados"
UC_MODEL_NAME = f"{catalog}.{schema}.extrator_financeiro"
ENDPOINT_NAME = f"extrator-financeiro"
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    WORKSPACE_PATH = "/Workspace" + _nb_path.rsplit("/", 2)[0] if _nb_path else None
except Exception:
    WORKSPACE_PATH = None
if not WORKSPACE_PATH:
    _ws_user = spark.sql("SELECT current_user()").collect()[0][0]
    WORKSPACE_PATH = f"/Workspace/Users/{_ws_user}/.bundle/ocr-financeiro/files"
MAX_EXAMPLES = 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verifica se há correções novas desde a última atualização

# COMMAND ----------

# Conta correções totais vs last model update
corrections_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CORRECTIONS_TABLE}").collect()[0]["cnt"]
print(f"Total de correções na tabela: {corrections_count}")

if corrections_count == 0:
    dbutils.notebook.exit("Nenhuma correção encontrada. Modelo não atualizado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gera few-shot examples

# COMMAND ----------

def categorize_error(campo, valor_extraido, valor_correto, comentario):
    try:
        v_ext = float(valor_extraido)
        v_cor = float(valor_correto)
        if v_cor == 0 and v_ext != 0:
            return "valor_deveria_ser_zero"
        if v_ext != 0 and v_cor != 0:
            ratio = v_ext / v_cor
            if abs(ratio - 1000) < 10 or abs(ratio - 0.001) < 0.0001:
                return "escala_errada"
            if abs(v_ext + v_cor) < 0.01:
                return "sinal_invertido"
    except (ValueError, ZeroDivisionError, TypeError):
        pass
    if comentario:
        lower = comentario.lower()
        if "somar" in lower or "somado" in lower:
            return "faltou_somar_subconta"
        if "subtrair" in lower or "subtraiu" in lower:
            return "faltou_subtrair"
        if "acumulado" in lower or "trimestre" in lower:
            return "periodo_errado"
    return "classificacao_incorreta"


def get_fonte(extracted_json_str, campo):
    if not extracted_json_str:
        return ""
    try:
        data = json.loads(extracted_json_str)
        fontes = data.get("fontes", {})
        if campo in fontes:
            return fontes[campo]
        last = campo.split(".")[-1]
        for k, v in fontes.items():
            if k.endswith(last):
                return v
    except Exception:
        pass
    return ""

# COMMAND ----------

# Query corrections with extracted_json context
rows = spark.sql(f"""
    SELECT
        c.campo, c.valor_extraido, c.valor_correto, c.comentario,
        c.document_name, c.tipo_entidade, c.periodo,
        r.extracted_json
    FROM {CORRECTIONS_TABLE} c
    LEFT JOIN {RESULTS_TABLE} r
        ON c.document_name = r.document_name
        AND COALESCE(c.tipo_entidade, '') = COALESCE(r.tipo_entidade, '')
        AND COALESCE(c.periodo, '') = COALESCE(r.periodo, '')
    ORDER BY c.campo, c.document_name
""").collect()

print(f"{len(rows)} correções com contexto")

# COMMAND ----------

# Agrupa por (campo, categoria)
groups = defaultdict(list)
for row in rows:
    cat = categorize_error(
        row["campo"],
        row["valor_extraido"] or "0",
        row["valor_correto"] or "0",
        row["comentario"] or "",
    )
    fonte = get_fonte(row["extracted_json"], row["campo"])
    groups[(row["campo"], cat)].append({
        "document_name": row["document_name"],
        "valor_extraido": row["valor_extraido"],
        "valor_correto": row["valor_correto"],
        "comentario": row["comentario"] or "",
        "fonte": fonte,
    })

# Seleciona top exemplos com diversidade de campos
sorted_groups = sorted(groups.items(), key=lambda x: -len(x[1]))
seen_campos = set()
selected = []
remaining = []

for (campo, cat), items in sorted_groups:
    if campo not in seen_campos and len(selected) < MAX_EXAMPLES:
        seen_campos.add(campo)
        selected.append(((campo, cat), items))
    else:
        remaining.append(((campo, cat), items))

for entry in remaining:
    if len(selected) >= MAX_EXAMPLES:
        break
    selected.append(entry)

# Formata exemplos
examples = []
for (campo, cat), items in selected:
    best = next((i for i in items if i["comentario"] and i["fonte"]), None)
    if not best:
        best = next((i for i in items if i["comentario"]), None)
    if not best:
        best = items[0]

    explicacao = best["comentario"]
    if not explicacao:
        labels = {
            "valor_deveria_ser_zero": f"O valor de {campo.split('.')[-1]} deve ser zero; reclassificar.",
            "faltou_somar_subconta": f"Faltou somar subcontas ao campo {campo}.",
            "escala_errada": f"Erro de escala no campo {campo}.",
            "sinal_invertido": f"Sinal invertido no campo {campo}.",
            "periodo_errado": "Usar valor acumulado, não trimestral.",
        }
        explicacao = labels.get(cat, f"Classificação incorreta no campo {campo}.")

    example = {
        "campo": campo,
        "categoria": cat,
        "frequencia": len(items),
        "valor_errado": best["valor_extraido"],
        "valor_correto": best["valor_correto"],
        "explicacao": explicacao[:200],
    }
    if best["fonte"]:
        example["fonte_doc"] = best["fonte"][:200]
    examples.append(example)

print(f"\n{len(examples)} exemplos gerados:")
for ex in examples:
    print(f"  [{ex['frequencia']}x] {ex['campo']} ({ex['categoria']})")

# COMMAND ----------

# Salva no Volume (compatível com Serverless — sem acesso ao filesystem local)
VOLUME_PATH = f"/Volumes/{catalog}/{schema}/documentos_pdf"
fewshot_json = json.dumps(examples, ensure_ascii=False, indent=2)

fewshot_volume_path = f"{VOLUME_PATH}/few_shot_examples.json"
dbutils.fs.put(fewshot_volume_path, fewshot_json, overwrite=True)
print(f"Few-shot examples salvos em {fewshot_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Loga nova versão do modelo

# COMMAND ----------

import mlflow
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

mlflow.set_registry_uri("databricks-uc")

_exp_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/ocr-financeiro"
_exp = mlflow.get_experiment_by_name(_exp_name)
if _exp is None:
    EXPERIMENT_ID = mlflow.create_experiment(_exp_name)
    print(f"Experimento criado: {_exp_name} (ID: {EXPERIMENT_ID})")
else:
    EXPERIMENT_ID = _exp.experiment_id
    print(f"Experimento existente: {_exp_name} (ID: {EXPERIMENT_ID})")

# Artifacts ficam no workspace path (acessível via /Workspace)
AGENT_FILE  = f"{WORKSPACE_PATH}/model/agent.py"
SCHEMA_FILE = f"{WORKSPACE_PATH}/model/output_schema.json"
DEPARA_FILE = f"{WORKSPACE_PATH}/model/depara.json"
REGRAS_FILE = f"{WORKSPACE_PATH}/model/regras_classificacao.json"

# Few-shot vem do Volume (salvo no step anterior)
FEWSHOT_FILE = fewshot_volume_path if examples else None

signature = ModelSignature(
    inputs=Schema([ColSpec(type="string", name="text")]),
    outputs=Schema([ColSpec(type="string", name="output")]),
)

artifacts = {
    "output_schema": SCHEMA_FILE,
    "depara": DEPARA_FILE,
    "regras_classificacao": REGRAS_FILE,
}
if FEWSHOT_FILE:
    artifacts["few_shot_examples"] = FEWSHOT_FILE

print(f"Logando modelo com {len(artifacts)} artifacts...")
with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name="auto-fewshot-update") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_FILE,
        artifacts=artifacts,
        pip_requirements=["openai>=1.0.0", "mlflow>=2.10.0", "databricks-sdk>=0.20.0"],
        registered_model_name=UC_MODEL_NAME,
        signature=signature,
    )
    print(f"Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Atualiza endpoint

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Nova versão: {latest_version}")

# COMMAND ----------

import requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

config = {
    "served_entities": [{
        "name": "techfin-ocr-v4",
        "entity_name": UC_MODEL_NAME,
        "entity_version": str(latest_version),
        "workload_size": "Large",
        "scale_to_zero_enabled": False,
        "environment_vars": {
            "DATABRICKS_TOKEN": f"{{{{secrets/{dbutils.widgets.get('secret_scope')}/{dbutils.widgets.get('secret_key')}}}}}"
        }
    }]
}

resp = requests.put(
    f"{host}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
    headers={"Authorization": f"Bearer {token}"},
    json=config,
)
print(f"Endpoint update: {resp.status_code}")
if resp.ok:
    print(f"Endpoint '{ENDPOINT_NAME}' atualizado para v{latest_version}")
else:
    print(f"Erro: {resp.text[:500]}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "ok",
    "version": latest_version,
    "examples": len(examples),
    "corrections": len(rows),
}))
