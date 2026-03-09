"""
Loga nova versão do modelo MLflow (sem alterar endpoint).
Depois do log, atualiza o endpoint via REST API preservando environment_vars.
Executar: conda run -n base python log_new_version.py
"""
import json
import os
import subprocess
import mlflow

PROFILE       = "e2-demo-field-eng"
EXPERIMENT_ID = "933926791991461"
UC_MODEL_NAME = "pedro_zanela.ia.techfin_ocr_v4"
ENDPOINT_NAME = "techfin-ocr-v4"
DATABRICKS_HOST = "https://e2-demo-field-eng.cloud.databricks.com"

os.environ["DATABRICKS_CONFIG_PROFILE"] = PROFILE
mlflow.set_tracking_uri(f"databricks://{PROFILE}")
mlflow.set_registry_uri("databricks-uc")

AGENT_FILE  = os.path.join(os.path.dirname(__file__), "agent.py")
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "output_schema.json")

from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

signature = ModelSignature(
    inputs=Schema([ColSpec(type="string", name="text")]),
    outputs=Schema([ColSpec(type="string", name="output")]),
)
input_example = {"text": "Empresa XYZ LTDA — Balanço Patrimonial Consolidado 2024-12-31"}

print("Logando nova versão do modelo...")
with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name="techfin-ocr-v4-option-b") as run:
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

from mlflow.tracking import MlflowClient
client = MlflowClient(
    tracking_uri=f"databricks://{PROFILE}",
    registry_uri="databricks-uc"
)
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
latest_version = max(int(v.version) for v in versions)
print(f"Nova versão registrada: {latest_version}")

# Atualiza endpoint via CLI preservando configuração existente (env vars, scale_to_zero, etc.)
config = {
    "served_entities": [
        {
            "name": "techfin-ocr-v4",
            "entity_name": UC_MODEL_NAME,
            "entity_version": str(latest_version),
            "workload_size": "Small",
            "scale_to_zero_enabled": True,
            "environment_vars": {
                "DATABRICKS_TOKEN": "{{secrets/pedro-zanela-scope/techfin-ocr-pat}}"
            }
        }
    ]
}

config_file = "/tmp/techfin_endpoint_update.json"
with open(config_file, "w") as f:
    json.dump(config, f)

print(f"\nAtualizando endpoint '{ENDPOINT_NAME}' para versão {latest_version}...")
result = subprocess.run(
    ["databricks", "serving-endpoints", "update-config",
     "--name", ENDPOINT_NAME,
     "--json", f"@{config_file}",
     "--profile", PROFILE],
    capture_output=True, text=True
)
if result.returncode == 0:
    print("Endpoint atualizado com sucesso!")
    print(result.stdout)
else:
    print("Erro ao atualizar endpoint:")
    print(result.stderr)
    print("\nUse este JSON manualmente via REST API:")
    print(json.dumps(config, indent=2))
