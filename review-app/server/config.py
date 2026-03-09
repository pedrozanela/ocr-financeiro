import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "862f1d757f0424f7")
RESULTS_TABLE = "pedro_zanela.ia.new_ocr_techfin_results"
CORRECTIONS_TABLE = "pedro_zanela.ia.new_ocr_techfin_corrections"
PDF_VOLUME_PATH = "/Volumes/pedro_zanela/ia/dados/techfin/ocr"
OCR_ENDPOINT = os.environ.get("OCR_ENDPOINT", "techfin-ocr-v4")


def get_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    return WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "DEFAULT"))
