# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Permissions ao Service Principal da App
# MAGIC
# MAGIC Concede permissoes UC e de jobs ao SP da review app.
# MAGIC Executar uma vez apos o primeiro deploy (ou apos recriar a app).

# COMMAND ----------

dbutils.widgets.text("sp_client_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

sp = dbutils.widgets.get("sp_client_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"SP: {sp}")
print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Permissoes UC

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{sp}`",
    f"GRANT SELECT, MODIFY ON SCHEMA {catalog}.{schema} TO `{sp}`",
    f"GRANT ALL PRIVILEGES ON VOLUME {catalog}.{schema}.documentos_pdf TO `{sp}`",
]

for sql in grants:
    print(f"  {sql}")
    spark.sql(sql)
    print("  OK")

print("\nPermissoes UC concedidas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Permissoes de Jobs (para a app poder disparar batch_job)

# COMMAND ----------

import requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}"}

# Find all ocr-financeiro jobs and grant CAN_MANAGE_RUN to the app SP
resp = requests.get(f"{host}/api/2.0/jobs/list", headers=headers, params={"name": "ocr-financeiro"})
jobs = resp.json().get("jobs", []) if resp.ok else []
ocr_jobs = [j for j in jobs if j["settings"]["name"].startswith("ocr-financeiro-")]

print(f"\nEncontrados {len(ocr_jobs)} jobs OCR:")
for job in ocr_jobs:
    job_id = job["job_id"]
    job_name = job["settings"]["name"]
    r = requests.patch(
        f"{host}/api/2.0/permissions/jobs/{job_id}",
        headers=headers,
        json={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_MANAGE_RUN"}]},
    )
    status = "OK" if r.ok else f"ERRO: {r.text[:100]}"
    print(f"  {job_name} ({job_id}): {status}")

print("\nPermissoes de jobs concedidas.")
