"""
OCR Financeiro — Configuração centralizada.
Todos os valores são lidos de variáveis de ambiente com fallbacks.

Em produção (Databricks App), os valores vêm do app.yaml.
Em jobs (DABs), os valores vêm dos base_parameters no databricks.yml.
Para dev local: exporte as variáveis de ambiente ou edite os defaults abaixo.
"""
import os

# ---------------------------------------------------------------------------
# Databricks Workspace
# ---------------------------------------------------------------------------
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
UC_CATALOG = os.environ.get("UC_CATALOG", "")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "ocr_financeiro")

# Tabelas
SOURCE_TABLE = os.environ.get("SOURCE_TABLE", f"{UC_CATALOG}.{UC_SCHEMA}.documentos")
RESULTS_TABLE = os.environ.get("RESULTS_TABLE", f"{UC_CATALOG}.{UC_SCHEMA}.resultados")
CORRECTIONS_TABLE = os.environ.get("CORRECTIONS_TABLE", f"{UC_CATALOG}.{UC_SCHEMA}.correcoes")
RESULTS_FINAL_TABLE = os.environ.get("RESULTS_FINAL_TABLE", f"{UC_CATALOG}.{UC_SCHEMA}.resultados_final")

# Modelo registrado
UC_MODEL_NAME = os.environ.get("UC_MODEL_NAME", f"{UC_CATALOG}.{UC_SCHEMA}.extrator_financeiro")

# Volume para PDFs
PDF_VOLUME_PATH = os.environ.get("PDF_VOLUME_PATH", f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/documentos_pdf")

# ---------------------------------------------------------------------------
# Serving Endpoint
# ---------------------------------------------------------------------------
OCR_ENDPOINT = os.environ.get("OCR_ENDPOINT", "extrator-financeiro")
OCR_MODEL = os.environ.get("OCR_MODEL", "databricks-claude-sonnet-4-6")
JUDGE_MODEL = os.environ.get("JUDGE_MODEL", "databricks-claude-opus-4-6")

# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
SECRET_SCOPE = os.environ.get("SECRET_SCOPE", "ocr-financeiro")
SECRET_KEY = os.environ.get("SECRET_KEY", "pat-servico")

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
MLFLOW_EXPERIMENT_ID = os.environ.get("MLFLOW_EXPERIMENT_ID", "")

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------
FEWSHOT_JOB_ID = int(os.environ.get("FEWSHOT_JOB_ID", "0"))

# ---------------------------------------------------------------------------
# Warehouses
# ---------------------------------------------------------------------------
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
SERVERLESS_WAREHOUSE_ID = os.environ.get("SERVERLESS_WAREHOUSE_ID", "")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def get_client():
    from databricks.sdk import WorkspaceClient
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    return WorkspaceClient(profile=DATABRICKS_PROFILE)
