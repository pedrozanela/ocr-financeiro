# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Infraestrutura — OCR Financeiro
# MAGIC
# MAGIC Cria schema, tabelas e volume no Unity Catalog.
# MAGIC Se o SP da app for informado, concede permissoes automaticamente.
# MAGIC Executar uma vez no primeiro deploy de cada ambiente.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("sp_client_id", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
sp = dbutils.widgets.get("sp_client_id").strip()

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")
print(f"App SP:  {sp or '(nao informado — permissoes serao puladas)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema {catalog}.{schema} OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tabelas

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.documentos (
        document_name STRING,
        document_text STRING,
        ingested_at TIMESTAMP,
        atualizado_em TIMESTAMP,
        atualizado_por STRING
    ) USING DELTA
""")
# Migração: adiciona colunas novas se a tabela já existia sem elas
for col, dtype in [("ingested_at", "TIMESTAMP"), ("atualizado_em", "TIMESTAMP"), ("atualizado_por", "STRING")]:
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.documentos ADD COLUMN {col} {dtype}")
        print(f"  Coluna {col} adicionada")
    except Exception:
        pass  # coluna já existe
print("Tabela documentos OK")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.resultados (
        document_name STRING,
        tipo_entidade STRING,
        periodo STRING,
        extracted_json STRING,
        assessment_json STRING,
        token_usage_json STRING,
        razao_social STRING,
        cnpj STRING,
        tipo_demonstrativo STRING,
        moeda STRING,
        escala_valores STRING,
        processado_em TIMESTAMP,
        modelo_versao STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")
print("Tabela resultados OK")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.correcoes (
        document_name STRING,
        campo STRING,
        valor_extraido STRING,
        valor_correto STRING,
        comentario STRING,
        criado_em TIMESTAMP,
        tipo_entidade STRING,
        periodo STRING,
        status STRING,
        confirmado_em TIMESTAMP,
        confirmado_por STRING
    ) USING DELTA
""")
print("Tabela correcoes OK")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.resultados_final (
        document_name STRING,
        tipo_entidade STRING,
        periodo STRING,
        extracted_json STRING,
        razao_social STRING,
        cnpj STRING,
        tipo_demonstrativo STRING,
        moeda STRING,
        escala_valores STRING,
        atualizado_em TIMESTAMP,
        atualizado_por STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")
print("Tabela resultados_final OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.documentos_pdf")
print("Volume documentos_pdf OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Permissoes do SP da App

# COMMAND ----------

if sp:
    # UC grants
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

    # Job grants (CAN_MANAGE_RUN on all ocr-financeiro jobs)
    import requests
    host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(f"{host}/api/2.0/jobs/list", headers=headers, params={"name": "ocr-financeiro"})
    jobs = resp.json().get("jobs", []) if resp.ok else []
    ocr_jobs = [j for j in jobs if j["settings"]["name"].startswith("ocr-financeiro-")]
    for job in ocr_jobs:
        r = requests.patch(
            f"{host}/api/2.0/permissions/jobs/{job['job_id']}",
            headers=headers,
            json={"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_MANAGE_RUN"}]},
        )
        status = "OK" if r.ok else f"ERRO: {r.text[:100]}"
        print(f"  Job {job['settings']['name']}: {status}")

    print("\nPermissoes concedidas.")
else:
    print("SP nao informado — pulando permissoes. Rode novamente com sp_client_id para conceder.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print(f"""
Infraestrutura criada:
  Schema:  {catalog}.{schema}
  Tabelas: {catalog}.{schema}.documentos
           {catalog}.{schema}.resultados
           {catalog}.{schema}.resultados_final
           {catalog}.{schema}.correcoes
  Volume:  /Volumes/{catalog}/{schema}/documentos_pdf
  Permissoes SP: {'concedidas' if sp else 'nao concedidas (sp_client_id vazio)'}
""")

dbutils.notebook.exit("ok")
