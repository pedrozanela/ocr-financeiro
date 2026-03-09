"""
Script para logar, registrar e deployar o TechFin OCR v4 agent no Databricks.
Executar: conda run -n base python deploy.py
"""
import json
import os
import mlflow
import databricks.agents

# ─── Config ────────────────────────────────────────────────────────────────────
PROFILE = "e2-demo-field-eng"
EXPERIMENT_ID = "933926791991461"  # experimento original do tile
UC_MODEL_NAME = "pedro_zanela.ia.techfin_ocr_v4"
ENDPOINT_NAME = "techfin-ocr-v4"

os.environ["DATABRICKS_CONFIG_PROFILE"] = PROFILE
mlflow.set_tracking_uri(f"databricks://{PROFILE}")
mlflow.set_registry_uri("databricks-uc")

# ─── Log model ─────────────────────────────────────────────────────────────────
print("Logando modelo no MLflow...")

AGENT_FILE = os.path.join(os.path.dirname(__file__), "agent.py")
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "output_schema.json")

from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

signature = ModelSignature(
    inputs=Schema([ColSpec(type="string", name="text")]),
    outputs=Schema([ColSpec(type="string", name="output")]),
)
input_example = {"text": "AMD TECNOLOGIA DA INFORMACAO E SISTEMAS LTDA CNPJ: 24.801.362/0001-40 Ativo Total: 2.156.906,51"}

with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name="techfin-ocr-v4-deploy") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_FILE,
        artifacts={"output_schema": SCHEMA_FILE},
        pip_requirements=["openai>=1.0.0", "mlflow>=2.10.0", "databricks-sdk>=0.20.0"],
        registered_model_name=UC_MODEL_NAME,
        signature=signature,
        input_example=input_example,
    )
    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
    print(f"Model URI: {model_info.model_uri}")

# ─── Pegar versão registrada ────────────────────────────────────────────────────
from mlflow.tracking import MlflowClient
client = MlflowClient(
    tracking_uri=f"databricks://{PROFILE}",
    registry_uri="databricks-uc"
)
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Model version: {latest_version}")

# ─── Deploy como serving endpoint ──────────────────────────────────────────────
print(f"Deployando como endpoint '{ENDPOINT_NAME}'...")
deployment = databricks.agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=latest_version,
    endpoint_name=ENDPOINT_NAME,
    scale_to_zero=True,
)
print(f"Deployment: {deployment}")
print(f"\nEndpoint URL: https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/{ENDPOINT_NAME}/invocations")
print("Aguarde ~5-10 min para o endpoint ficar READY.")
