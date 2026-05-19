# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Lakebase — OCR Financeiro
# MAGIC
# MAGIC Cria projeto Lakebase, database, tabelas e configura permissões do SP do app.
# MAGIC Executar **uma vez** no primeiro deploy de cada ambiente.
# MAGIC
# MAGIC ## Passo a passo completo (tudo pela UI)
# MAGIC
# MAGIC ### Primeiro deploy (sem Lakebase)
# MAGIC 1. No Databricks, vá em **Repos** → importe o repositório Git
# MAGIC 2. Faça deploy do bundle pela UI (ou via Workflows → Jobs)
# MAGIC 3. O app `ocr-financeiro` será criado e funcionará com Delta + SQL Warehouse
# MAGIC
# MAGIC ### Ativar Lakebase
# MAGIC 4. Vá em **Workflows → Jobs → ocr-financeiro-setup-lakebase** → Run
# MAGIC    - Parâmetro `sp_client_id`: deixe vazio na primeira execução
# MAGIC    - Aguarde completar (~2 min)
# MAGIC 5. No output do job, copie o valor de **Host** (ex: `ep-xxxxx.database.us-east-1.cloud.databricks.com`)
# MAGIC 6. Vá em **Apps → ocr-financeiro → Settings**:
# MAGIC    - Em **Service Principal**, copie o **Client ID** (ex: `5914cb1c-3156-41a7-b1ff-1aa9a0778977`)
# MAGIC    - Em **Resources**, clique **Add Resource → Lakebase**:
# MAGIC      - Project: `ocr-financeiro`
# MAGIC      - Branch: `production`
# MAGIC      - Permission: `CAN_CONNECT_AND_CREATE`
# MAGIC    - Em **Environment Variables**, adicione:
# MAGIC      - `LAKEBASE_HOST` = (o host copiado no passo 5)
# MAGIC      - `LAKEBASE_DB` = `ocr_financeiro`
# MAGIC      - `LAKEBASE_PROJECT` = `ocr-financeiro`
# MAGIC 7. Rode o job **setup-lakebase** novamente, agora com o `sp_client_id` do passo 6
# MAGIC    - Isso cria o role e permissões do SP no Lakebase
# MAGIC 8. Re-deploy o app (clique **Deploy** na UI do app)
# MAGIC
# MAGIC ### Pronto!
# MAGIC O app agora usa Lakebase. Correções manuais ficam ~50ms em vez de ~2s.
# MAGIC
# MAGIC **Pré-requisitos**: workspace com serverless habilitado.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("project_name", "ocr-financeiro")
dbutils.widgets.text("database_name", "ocr_financeiro")
dbutils.widgets.text("sp_client_id", "")

PROJECT = dbutils.widgets.get("project_name").strip()
DATABASE = dbutils.widgets.get("database_name").strip()
SP_CLIENT_ID = dbutils.widgets.get("sp_client_id").strip()

w = WorkspaceClient()
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]

print(f"Project:  {PROJECT}")
print(f"Database: {DATABASE}")
print(f"SP:       {SP_CLIENT_ID or '(não informado — configurar depois)'}")
print(f"User:     {CURRENT_USER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar Projeto Lakebase

# COMMAND ----------

project_path = f"projects/{PROJECT}"

# Check if project already exists
try:
    resp = w.api_client.do("GET", f"/api/2.0/postgres/{project_path}")
    print(f"✓ Projeto '{PROJECT}' já existe")
except Exception:
    print(f"Criando projeto '{PROJECT}'...")
    w.api_client.do("POST", "/api/2.0/postgres/projects", body={
        "project_id": PROJECT,
        "spec": {"display_name": PROJECT.replace("-", " ").title()},
    })
    print(f"✓ Projeto '{PROJECT}' criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aguardar Endpoint ACTIVE

# COMMAND ----------

branch_path = f"{project_path}/branches/production"
endpoint_path = f"{branch_path}/endpoints/primary"

print("Aguardando endpoint ficar ACTIVE...")
for i in range(60):
    resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
    endpoints = resp.get("endpoints", [])
    if endpoints:
        state = endpoints[0].get("status", {}).get("current_state", "")
        host = endpoints[0].get("status", {}).get("hosts", {}).get("host", "")
        if state == "ACTIVE" and host:
            print(f"✓ Endpoint ACTIVE: {host}")
            break
        print(f"  Estado: {state}... ({i*5}s)")
    time.sleep(5)
else:
    raise Exception("Timeout esperando endpoint ficar ACTIVE")

LAKEBASE_HOST = host

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gerar Credencial e Conectar

# COMMAND ----------

import psycopg2

def get_pg_connection(dbname="postgres"):
    """Conecta ao Lakebase usando credencial OAuth."""
    cred = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
    token = cred.get("token", "")
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=5432, dbname=dbname,
        user=CURRENT_USER, password=token, sslmode="require",
    )

# Test connection
conn = get_pg_connection()
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT version()")
print(f"✓ Conectado: {cur.fetchone()[0][:60]}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Database

# COMMAND ----------

conn = get_pg_connection("postgres")
conn.autocommit = True
cur = conn.cursor()

# Check if database exists
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,))
if cur.fetchone():
    print(f"✓ Database '{DATABASE}' já existe")
else:
    cur.execute(f'CREATE DATABASE "{DATABASE}"')
    print(f"✓ Database '{DATABASE}' criado")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Criar Tabelas

# COMMAND ----------

conn = get_pg_connection(DATABASE)
conn.autocommit = True
cur = conn.cursor()

DDL = """
CREATE TABLE IF NOT EXISTS documentos (
    document_name TEXT PRIMARY KEY,
    document_text TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_por TEXT
);

CREATE TABLE IF NOT EXISTS resultados (
    document_name TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    extracted_json JSONB,
    assessment_json JSONB,
    token_usage_json JSONB,
    razao_social TEXT,
    cnpj TEXT,
    tipo_demonstrativo TEXT,
    moeda TEXT,
    escala_valores TEXT,
    processado_em TIMESTAMPTZ DEFAULT NOW(),
    modelo_versao TEXT DEFAULT '',
    modo_extracao TEXT DEFAULT '',
    PRIMARY KEY (document_name, tipo_entidade, periodo)
);

CREATE TABLE IF NOT EXISTS correcoes (
    document_name TEXT NOT NULL,
    campo TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    valor_extraido TEXT,
    valor_correto TEXT,
    comentario TEXT,
    status TEXT DEFAULT 'pendente',
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    confirmado_em TIMESTAMPTZ,
    confirmado_por TEXT,
    resolvido_em TIMESTAMPTZ,
    PRIMARY KEY (document_name, campo, tipo_entidade, periodo)
);

CREATE TABLE IF NOT EXISTS resultados_final (
    document_name TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    extracted_json JSONB,
    razao_social TEXT,
    cnpj TEXT,
    tipo_demonstrativo TEXT,
    moeda TEXT,
    escala_valores TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_por TEXT,
    PRIMARY KEY (document_name, tipo_entidade, periodo)
);

CREATE INDEX IF NOT EXISTS idx_resultados_doc ON resultados (document_name);
CREATE INDEX IF NOT EXISTS idx_correcoes_doc ON correcoes (document_name);
CREATE INDEX IF NOT EXISTS idx_correcoes_status ON correcoes (status);
CREATE INDEX IF NOT EXISTS idx_resultados_final_doc ON resultados_final (document_name);
"""

cur.execute(DDL)
print("✓ Tabelas criadas")

# Verify
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
tables = [r[0] for r in cur.fetchall()]
print(f"  Tabelas: {tables}")

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configurar Permissões do SP do App

# COMMAND ----------

if not SP_CLIENT_ID:
    print("⚠ SP não informado — pule este passo e execute novamente após o primeiro deploy do app.")
    print("  Para obter o SP: databricks apps get ocr-financeiro -o json | jq '.service_principal_client_id'")
else:
    # Create Lakebase role for SP
    role_path = f"{branch_path}/roles/app-sp"
    try:
        w.api_client.do("GET", f"/api/2.0/postgres/{role_path}")
        print(f"✓ Role 'app-sp' já existe")
    except Exception:
        w.api_client.do("POST", f"/api/2.0/postgres/{branch_path}/roles", body={
            "role_id": "app-sp",
            "spec": {
                "postgres_role": SP_CLIENT_ID,
                "identity_type": "SERVICE_PRINCIPAL",
                "membership_roles": ["DATABRICKS_WRITER"],
            },
        })
        print(f"✓ Role 'app-sp' criado para SP {SP_CLIENT_ID}")

    # Grant SQL permissions
    conn = get_pg_connection(DATABASE)
    conn.autocommit = True
    cur = conn.cursor()

    sp_quoted = f'"{SP_CLIENT_ID}"'
    cur.execute(f'GRANT ALL ON DATABASE "{DATABASE}" TO {sp_quoted}')
    cur.execute(f'GRANT ALL ON SCHEMA public TO {sp_quoted}')
    cur.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO {sp_quoted}')
    cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {sp_quoted}')

    print(f"✓ Permissões SQL concedidas ao SP {SP_CLIENT_ID}")
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Relatório

# COMMAND ----------

report = {
    "status": "ok",
    "project": PROJECT,
    "database": DATABASE,
    "host": LAKEBASE_HOST,
    "endpoint": endpoint_path,
    "sp_configured": bool(SP_CLIENT_ID),
}

print("=" * 60)
print("  Setup Lakebase concluído!")
print(f"  Host:     {LAKEBASE_HOST}")
print(f"  Database: {DATABASE}")
print(f"  SP:       {SP_CLIENT_ID or 'não configurado'}")
print("=" * 60)
print()
print("Próximos passos:")
print(f"  1. Adicione no databricks.yml do target:")
print(f'     lakebase_host: "{LAKEBASE_HOST}"')
print(f"  2. Adicione app resource postgres no databricks.yml")
print(f"  3. Deploy: databricks bundle deploy --target <target>")
if not SP_CLIENT_ID:
    print(f"  4. Após deploy, pegue o SP e rode este notebook novamente com sp_client_id preenchido")

dbutils.notebook.exit(json.dumps(report, ensure_ascii=False))
