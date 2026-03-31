# Databricks notebook source
# MAGIC %md
# MAGIC # Registro Inicial do Modelo — OCR Financeiro
# MAGIC
# MAGIC Loga a primeira versão do modelo no Unity Catalog.
# MAGIC Executar uma vez no primeiro deploy.

# COMMAND ----------

# MAGIC %pip install openai>=1.0.0 mlflow>=2.10.0
# MAGIC %restart_python

# COMMAND ----------

import os
import mlflow
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

dbutils.widgets.text("catalog", "catalog_nqc8lc_8uoefp")
dbutils.widgets.text("schema", "ocr_financeiro")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
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

# Create or get experiment
_user = spark.sql("SELECT current_user()").collect()[0][0]
_exp_name = f"/Users/{_user}/ocr-financeiro"
_exp = mlflow.get_experiment_by_name(_exp_name)
if _exp is None:
    EXPERIMENT_ID = mlflow.create_experiment(_exp_name)
    print(f"Experimento criado: {_exp_name} (ID: {EXPERIMENT_ID})")
else:
    EXPERIMENT_ID = _exp.experiment_id
    print(f"Experimento existente: {_exp_name} (ID: {EXPERIMENT_ID})")

# COMMAND ----------

print(f"Logando modelo com {len(artifacts)} artifacts...")
with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name="initial-registration") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_FILE,
        artifacts=artifacts,
        pip_requirements=["openai>=1.0.0", "mlflow>=2.10.0", "databricks-sdk>=0.20.0"],
        registered_model_name=UC_MODEL_NAME,
        signature=signature,
    )
    print(f"Run ID: {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Modelo registrado: {UC_MODEL_NAME} v{latest_version}")

dbutils.notebook.exit(f"v{latest_version}")
