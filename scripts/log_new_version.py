"""
Loga nova versão do modelo MLflow e atualiza o endpoint.
Executar: cd techfin && conda run -n base python scripts/log_new_version.py
"""
import json
import os
import sys
import subprocess

# Add project root to path so we can import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mlflow
from config import (
    DATABRICKS_PROFILE, MLFLOW_EXPERIMENT_ID, UC_MODEL_NAME,
    OCR_ENDPOINT, SECRET_SCOPE, SECRET_KEY,
)

os.environ["DATABRICKS_CONFIG_PROFILE"] = DATABRICKS_PROFILE
mlflow.set_tracking_uri(f"databricks://{DATABRICKS_PROFILE}")
mlflow.set_registry_uri("databricks-uc")

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MODEL_DIR = os.path.join(_ROOT, "model")
AGENT_FILE    = os.path.join(_MODEL_DIR, "agent.py")
SCHEMA_FILE   = os.path.join(_MODEL_DIR, "output_schema.json")
DEPARA_FILE   = os.path.join(_MODEL_DIR, "depara.json")
REGRAS_FILE   = os.path.join(_MODEL_DIR, "regras_classificacao.json")
FEWSHOT_FILE  = os.path.join(_MODEL_DIR, "few_shot_examples.json")

from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

signature = ModelSignature(
    inputs=Schema([ColSpec(type="string", name="text")]),
    outputs=Schema([ColSpec(type="string", name="output")]),
)
input_example = {"text": "Empresa XYZ LTDA — Balanço Patrimonial Consolidado 2024-12-31"}

# Build artifacts dict
artifacts = {
    "output_schema": SCHEMA_FILE,
    "depara": DEPARA_FILE,
    "regras_classificacao": REGRAS_FILE,
}
if os.path.exists(FEWSHOT_FILE):
    artifacts["few_shot_examples"] = FEWSHOT_FILE

print("Logando nova versão do modelo...")
with mlflow.start_run(experiment_id=MLFLOW_EXPERIMENT_ID, run_name="techfin-ocr-auto") as run:
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=AGENT_FILE,
        artifacts=artifacts,
        pip_requirements=["openai>=1.0.0", "mlflow>=2.10.0", "databricks-sdk>=0.20.0"],
        registered_model_name=UC_MODEL_NAME,
        signature=signature,
        input_example=input_example,
    )
    print(f"Run ID: {run.info.run_id}")
    print(f"Model URI: {model_info.model_uri}")

from mlflow.tracking import MlflowClient
client = MlflowClient(
    tracking_uri=f"databricks://{DATABRICKS_PROFILE}",
    registry_uri="databricks-uc"
)
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Nova versão registrada: {latest_version}")

# Atualiza endpoint preservando configuração
config = {
    "served_entities": [
        {
            "name": OCR_ENDPOINT,
            "entity_name": UC_MODEL_NAME,
            "entity_version": str(latest_version),
            "workload_size": "Large",
            "scale_to_zero_enabled": False,
            "environment_vars": {
                "DATABRICKS_TOKEN": f"{{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}"
            }
        }
    ]
}

config_file = "/tmp/techfin_endpoint_update.json"
with open(config_file, "w") as f:
    json.dump(config, f)

print(f"\nAtualizando endpoint '{OCR_ENDPOINT}' para versão {latest_version}...")
result = subprocess.run(
    ["databricks", "serving-endpoints", "update-config",
     OCR_ENDPOINT, "--json", f"@{config_file}",
     "--profile", DATABRICKS_PROFILE],
    capture_output=True, text=True
)
if result.returncode == 0:
    print("Endpoint atualizado com sucesso!")
    print(result.stdout)
else:
    print("Erro ao atualizar endpoint:")
    print(result.stderr)
    print("\nJSON para atualização manual:")
    print(json.dumps(config, indent=2))
