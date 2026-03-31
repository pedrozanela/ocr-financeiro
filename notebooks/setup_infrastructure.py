# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Infraestrutura — OCR Financeiro
# MAGIC
# MAGIC Cria schema, tabelas e volume no Unity Catalog.
# MAGIC Executar uma vez no primeiro deploy de cada ambiente.

# COMMAND ----------

dbutils.widgets.text("catalog", "cedip_fevm_aws_classic_stable_catalog")
dbutils.widgets.text("schema", "ocr_financeiro")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

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
        ingested_at TIMESTAMP
    ) USING DELTA
""")
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

# MAGIC %md
# MAGIC ## 3. Volume

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

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.documentos_pdf")
print("Volume documentos_pdf OK")

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
""")

dbutils.notebook.exit("ok")
