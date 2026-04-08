# Databricks notebook source
# MAGIC %md
# MAGIC # Registro do Modelo + Serving Endpoint — OCR Financeiro
# MAGIC
# MAGIC Loga versao do modelo no Unity Catalog e cria/atualiza o serving endpoint.
# MAGIC Executar no primeiro deploy e sempre que o modelo precisar ser atualizado.

# COMMAND ----------

# MAGIC %pip install openai>=1.0.0 mlflow>=2.10.0
# MAGIC %restart_python

# COMMAND ----------

import os
import json
import mlflow
from mlflow.models.signature import ModelSignature
from mlflow.models.resources import DatabricksServingEndpoint
from mlflow.types.schema import Schema, ColSpec

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("endpoint_name", "extrator-financeiro")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
UC_MODEL_NAME = f"{catalog}.{schema}.extrator_financeiro"

mlflow.set_registry_uri("databricks-uc")

# Resolve workspace path from notebook location
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    WORKSPACE_PATH = "/Workspace" + _nb_path.rsplit("/", 2)[0] if _nb_path else None
except Exception:
    WORKSPACE_PATH = None

if not WORKSPACE_PATH:
    _user = spark.sql("SELECT current_user()").collect()[0][0]
    WORKSPACE_PATH = f"/Workspace/Users/{_user}/.bundle/ocr-financeiro/files"

print(f"Workspace path: {WORKSPACE_PATH}")
print(f"Model name: {UC_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")

# COMMAND ----------

AGENT_FILE  = f"{WORKSPACE_PATH}/model/agent.py"
SCHEMA_FILE = f"{WORKSPACE_PATH}/model/output_schema.json"
DEPARA_FILE = f"{WORKSPACE_PATH}/model/depara.json"
REGRAS_FILE = f"{WORKSPACE_PATH}/model/regras_classificacao.json"
FEWSHOT_FILE = f"{WORKSPACE_PATH}/model/few_shot_examples.json"

# Verify files exist
for name, path in [("agent", AGENT_FILE), ("schema", SCHEMA_FILE), ("depara", DEPARA_FILE), ("regras", REGRAS_FILE)]:
    exists = os.path.exists(path)
    print(f"  {name}: {path} -> {'OK' if exists else 'MISSING'}")

signature = ModelSignature(
    inputs=Schema([ColSpec(type="string", name="text")]),
    outputs=Schema([ColSpec(type="string", name="output")]),
)

artifacts = {
    "output_schema": SCHEMA_FILE,
    "depara": DEPARA_FILE,
    "regras_classificacao": REGRAS_FILE,
}
if os.path.exists(FEWSHOT_FILE):
    artifacts["few_shot_examples"] = FEWSHOT_FILE
    print(f"  fewshot: {FEWSHOT_FILE} -> OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar/obter experiment e logar modelo

# COMMAND ----------

_user = spark.sql("SELECT current_user()").collect()[0][0]
_exp_name = f"/Users/{_user}/ocr-financeiro"
mlflow.set_experiment(_exp_name)
EXPERIMENT_ID = mlflow.get_experiment_by_name(_exp_name).experiment_id
print(f"Experimento: {_exp_name} (ID: {EXPERIMENT_ID})")

# COMMAND ----------

print(f"Logando modelo com {len(artifacts)} artifacts...")
with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name="model-registration") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_FILE,
        artifacts=artifacts,
        pip_requirements=["openai>=1.0.0", "mlflow>=2.10.0", "databricks-sdk>=0.20.0"],
        registered_model_name=UC_MODEL_NAME,
        signature=signature,
        resources=[
            DatabricksServingEndpoint(endpoint_name="databricks-claude-sonnet-4-6"),
            DatabricksServingEndpoint(endpoint_name="databricks-claude-sonnet-4-6-judge"),
        ],
    )
    print(f"Run ID: {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = str(max(int(v.version) for v in versions))
print(f"Modelo registrado: {UC_MODEL_NAME} v{latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar ou atualizar serving endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

# DATABRICKS_TOKEN and DATABRICKS_HOST are auto-injected by the platform into
# every serving endpoint container — no secrets needed.
_host = w.config.host
if _host and not _host.startswith("http"):
    _host = f"https://{_host}"

served_entity = ServedEntityInput(
    name=ENDPOINT_NAME,
    entity_name=UC_MODEL_NAME,
    entity_version=latest_version,
    workload_size="Large",
    scale_to_zero_enabled=False,
    environment_vars={
        "DATABRICKS_HOST": _host,
    },
)

config = EndpointCoreConfigInput(served_entities=[served_entity])

# Check if endpoint exists
try:
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    endpoint_exists = True
    print(f"Endpoint '{ENDPOINT_NAME}' existe — atualizando para v{latest_version}...")
except Exception:
    endpoint_exists = False
    print(f"Endpoint '{ENDPOINT_NAME}' nao existe — criando com v{latest_version}...")

# COMMAND ----------

if endpoint_exists:
    w.serving_endpoints.update_config(ENDPOINT_NAME, served_entities=[served_entity])
    print(f"Endpoint '{ENDPOINT_NAME}' atualizado. Aguardando deploy...")
else:
    w.serving_endpoints.create(name=ENDPOINT_NAME, config=config)
    print(f"Endpoint '{ENDPOINT_NAME}' criado. Aguardando deploy...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aguardar endpoint ficar READY

# COMMAND ----------

import time

for i in range(60):
    ep = w.serving_endpoints.get(ENDPOINT_NAME)
    state = ep.state
    ready = state.ready.value if state and state.ready else "UNKNOWN"
    updating = state.config_update.value if state and state.config_update else "UNKNOWN"
    print(f"  [{i+1}/60] ready={ready}  config_update={updating}")
    if ready == "READY" and updating == "NOT_UPDATING":
        print(f"\n✓ Endpoint '{ENDPOINT_NAME}' READY com modelo v{latest_version}")
        break
    time.sleep(15)
else:
    print(f"\n⚠ Timeout aguardando endpoint. Verifique manualmente em Serving → {ENDPOINT_NAME}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "model": UC_MODEL_NAME,
    "version": latest_version,
    "endpoint": ENDPOINT_NAME,
}))
