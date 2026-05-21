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
        tipo_demonstrativo INT,
        tipo_documento INT,
        numeroMeses INT,
        moeda STRING,
        escala_valores STRING,
        processado_em TIMESTAMP,
        modelo_versao STRING,
        modo_extracao STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")
# Migrações idempotentes para ambientes existentes
for col_ddl in [
    "modo_extracao STRING",
    "tipo_documento INT",
    "numeroMeses INT",
]:
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados ADD COLUMN ({col_ddl})")
    except Exception:
        pass  # coluna já existe

# Garantir que tipo_demonstrativo é INT (era STRING em versões antigas)
try:
    cols = spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.resultados").collect()
    td = next((c for c in cols if c.col_name == "tipo_demonstrativo"), None)
    if td and td.data_type.lower() != "int":
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados DROP COLUMN tipo_demonstrativo")
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados ADD COLUMN (tipo_demonstrativo INT)")
        print("  Migrated tipo_demonstrativo: STRING -> INT")
except Exception as e:
    print(f"  Skipped tipo_demonstrativo migration: {e}")
print("Tabela resultados OK")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.correcoes_legado (
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
        confirmado_por STRING,
        resolvido_em TIMESTAMP
    ) USING DELTA
""")
print("Tabela correcoes_legado OK")

# Revisoes em andamento — estado transitorio do workflow de revisao
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.revisoes_em_andamento (
        document_name STRING NOT NULL,
        campo STRING NOT NULL,
        tipo_entidade STRING NOT NULL,
        periodo STRING NOT NULL,
        valor_extraido DOUBLE,
        valor_corrente DOUBLE,
        status STRING NOT NULL,
        tipo_erro STRING,
        tipo_erro_detalhe STRING,
        revisado_por STRING,
        revisado_em TIMESTAMP,
        criado_em TIMESTAMP
    ) USING DELTA
""")
print("Tabela revisoes_em_andamento OK")

# Feedback LLM — audit log append-only (snapshot no Submeter)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.feedback_llm (
        id BIGINT,
        document_name STRING NOT NULL,
        campo STRING NOT NULL,
        tipo_entidade STRING NOT NULL,
        periodo STRING NOT NULL,
        valor_llm DOUBLE,
        valor_final DOUBLE,
        acao STRING NOT NULL,
        tipo_erro STRING,
        tipo_erro_detalhe STRING,
        fonte_llm STRING,
        revisado_por STRING NOT NULL,
        revisado_em TIMESTAMP NOT NULL,
        submetido_em TIMESTAMP NOT NULL,
        modelo_versao STRING,
        prompt_versao STRING
    ) USING DELTA
""")
print("Tabela feedback_llm OK")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.resultados_final (
        document_name STRING,
        tipo_entidade STRING,
        periodo STRING,
        extracted_json STRING,
        razao_social STRING,
        cnpj STRING,
        tipo_demonstrativo INT,
        tipo_documento INT,
        numeroMeses INT,
        moeda STRING,
        escala_valores STRING,
        atualizado_em TIMESTAMP,
        atualizado_por STRING,
        status STRING,
        finalizado_em TIMESTAMP,
        finalizado_por STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")
# Migrações idempotentes
for col_ddl in [
    "tipo_documento INT",
    "numeroMeses INT",
    "status STRING",
    "finalizado_em TIMESTAMP",
    "finalizado_por STRING",
    "techfin_response STRING",
]:
    try:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados_final ADD COLUMN ({col_ddl})")
    except Exception:
        pass
try:
    cols = spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.resultados_final").collect()
    td = next((c for c in cols if c.col_name == "tipo_demonstrativo"), None)
    if td and td.data_type.lower() != "int":
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados_final DROP COLUMN tipo_demonstrativo")
        spark.sql(f"ALTER TABLE {catalog}.{schema}.resultados_final ADD COLUMN (tipo_demonstrativo INT)")
        print("  Migrated tipo_demonstrativo: STRING -> INT")
except Exception as e:
    print(f"  Skipped tipo_demonstrativo migration: {e}")
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
           {catalog}.{schema}.revisoes_em_andamento
           {catalog}.{schema}.feedback_llm
           {catalog}.{schema}.correcoes_legado
  Volume:  /Volumes/{catalog}/{schema}/documentos_pdf
  Permissoes SP: {'concedidas' if sp else 'nao concedidas (sp_client_id vazio)'}
""")

dbutils.notebook.exit("ok")
