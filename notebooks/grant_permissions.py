# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Permissions ao Service Principal da App
# MAGIC
# MAGIC Concede permissoes UC ao SP da review app.
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

print("\nPermissoes concedidas.")
