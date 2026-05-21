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
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("migrate_data", "true")

PROJECT = dbutils.widgets.get("project_name").strip()
DATABASE = dbutils.widgets.get("database_name").strip()
SP_CLIENT_ID = dbutils.widgets.get("sp_client_id").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
MIGRATE_DATA = dbutils.widgets.get("migrate_data").strip().lower() == "true"

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
    w.api_client.do("POST", f"/api/2.0/postgres/projects?project_id={PROJECT}", body={
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

# Get the database resource ID (needed for app resource in databricks.yml)
DATABASE_RESOURCE_ID = ""
try:
    resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_path}/databases")
    for db in resp.get("databases", []):
        db_name = db.get("name", "")
        if db_name and "databricks-postgres" not in db_name:
            DATABASE_RESOURCE_ID = db_name
            break
    if DATABASE_RESOURCE_ID:
        print(f"✓ Database resource ID: {DATABASE_RESOURCE_ID}")
    else:
        print("⚠ Database resource ID não encontrado — use o CLI: databricks postgres list-databases ...")
except Exception as e:
    print(f"⚠ Não foi possível obter database resource ID: {e}")

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
    tipo_demonstrativo INTEGER,
    tipo_documento INTEGER,
    numeroMeses INTEGER,
    moeda TEXT,
    escala_valores TEXT,
    processado_em TIMESTAMPTZ DEFAULT NOW(),
    modelo_versao TEXT DEFAULT '',
    modo_extracao TEXT DEFAULT '',
    PRIMARY KEY (document_name, tipo_entidade, periodo)
);

-- Estado transitorio da revisao em andamento (UI le e escreve, limpa no Submeter)
CREATE TABLE IF NOT EXISTS revisoes_em_andamento (
    document_name TEXT NOT NULL,
    campo TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    valor_extraido NUMERIC,
    valor_corrente NUMERIC,
    status TEXT NOT NULL DEFAULT 'llm',  -- 'llm' | 'corrigido' | 'confirmado'
    tipo_erro TEXT,
    tipo_erro_detalhe TEXT,
    revisado_por TEXT,
    revisado_em TIMESTAMPTZ,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (document_name, campo, tipo_entidade, periodo)
);

-- Audit log append-only (snapshot no Submeter; fonte para retreino/avaliacao)
CREATE TABLE IF NOT EXISTS feedback_llm (
    id BIGSERIAL PRIMARY KEY,
    document_name TEXT NOT NULL,
    campo TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    valor_llm NUMERIC,
    valor_final NUMERIC,
    acao TEXT NOT NULL,                   -- 'corrigido' | 'confirmado'
    tipo_erro TEXT,
    tipo_erro_detalhe TEXT,
    fonte_llm JSONB,
    revisado_por TEXT NOT NULL,
    revisado_em TIMESTAMPTZ NOT NULL,
    submetido_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modelo_versao TEXT,
    prompt_versao TEXT
);

CREATE TABLE IF NOT EXISTS resultados_final (
    document_name TEXT NOT NULL,
    tipo_entidade TEXT NOT NULL DEFAULT '',
    periodo TEXT NOT NULL DEFAULT '',
    extracted_json JSONB,
    razao_social TEXT,
    cnpj TEXT,
    tipo_demonstrativo INTEGER,
    tipo_documento INTEGER,
    numeroMeses INTEGER,
    moeda TEXT,
    escala_valores TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_por TEXT,
    status TEXT DEFAULT 'em_revisao',
    techfin_response JSONB,
    finalizado_em TIMESTAMPTZ,
    finalizado_por TEXT,
    PRIMARY KEY (document_name, tipo_entidade, periodo)
);

CREATE INDEX IF NOT EXISTS idx_resultados_doc ON resultados (document_name);
CREATE INDEX IF NOT EXISTS idx_revisoes_doc ON revisoes_em_andamento (document_name);
CREATE INDEX IF NOT EXISTS idx_revisoes_status ON revisoes_em_andamento (status);
CREATE INDEX IF NOT EXISTS idx_feedback_doc ON feedback_llm (document_name);
CREATE INDEX IF NOT EXISTS idx_feedback_campo ON feedback_llm (campo);
CREATE INDEX IF NOT EXISTS idx_feedback_tipo_erro ON feedback_llm (tipo_erro) WHERE tipo_erro IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_feedback_submetido ON feedback_llm (submetido_em);
CREATE INDEX IF NOT EXISTS idx_feedback_modelo ON feedback_llm (modelo_versao);
CREATE INDEX IF NOT EXISTS idx_resultados_final_doc ON resultados_final (document_name);
"""

cur.execute(DDL)
print("✓ Tabelas criadas")

# Migrations idempotentes para ambientes existentes
for tbl in ("resultados", "resultados_final"):
    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS tipo_documento INTEGER")
    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS numeroMeses INTEGER")
    # Convert tipo_demonstrativo from TEXT to INTEGER (idempotente: só altera se ainda for text)
    cur.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = '{tbl}' AND column_name = 'tipo_demonstrativo'
    """)
    row = cur.fetchone()
    if row and row[0] == "text":
        cur.execute(f"""
            ALTER TABLE {tbl} ALTER COLUMN tipo_demonstrativo TYPE INTEGER USING (
                CASE LOWER(TRIM(COALESCE(tipo_demonstrativo,'')))
                    WHEN 'anual' THEN 1
                    WHEN 'semestral' THEN 2
                    WHEN 'trimestral' THEN 3
                    WHEN 'mensal' THEN 4
                    WHEN '1' THEN 1
                    WHEN '2' THEN 2
                    WHEN '3' THEN 3
                    WHEN '4' THEN 4
                    ELSE NULL
                END
            )
        """)
        print(f"  Migrated {tbl}.tipo_demonstrativo: TEXT -> INTEGER")

# Migrations idempotentes para fluxo de finalização (apenas resultados_final)
cur.execute("ALTER TABLE resultados_final ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'em_revisao'")
cur.execute("ALTER TABLE resultados_final ADD COLUMN IF NOT EXISTS finalizado_em TIMESTAMPTZ")
cur.execute("ALTER TABLE resultados_final ADD COLUMN IF NOT EXISTS finalizado_por TEXT")
cur.execute("ALTER TABLE resultados_final ADD COLUMN IF NOT EXISTS techfin_response JSONB")
# Preencher status default em linhas legadas (criadas antes do default)
cur.execute("UPDATE resultados_final SET status = 'em_revisao' WHERE status IS NULL")
print("✓ Migrations aplicadas")

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
        w.api_client.do("POST", f"/api/2.0/postgres/{branch_path}/roles?role_id=app-sp", body={
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
    # BIGSERIAL columns (feedback_llm.id) precisam de privilégio na sequence
    cur.execute(f'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {sp_quoted}')
    cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {sp_quoted}')

    print(f"✓ Permissões SQL concedidas ao SP {SP_CLIENT_ID}")
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Migrar dados do Delta para Lakebase

# COMMAND ----------

if MIGRATE_DATA and CATALOG:
    import psycopg2.extras

    delta_tables = {
        "documentos": f"{CATALOG}.{SCHEMA}.documentos",
        "resultados": f"{CATALOG}.{SCHEMA}.resultados",
        "correcoes": f"{CATALOG}.{SCHEMA}.correcoes",
        "resultados_final": f"{CATALOG}.{SCHEMA}.resultados_final",
    }

    conn = get_pg_connection(DATABASE)
    conn.autocommit = True
    cur = conn.cursor()

    for pg_table, delta_table in delta_tables.items():
        try:
            rows = spark.sql(f"SELECT * FROM {delta_table}").collect()
        except Exception as e:
            print(f"  ⚠ {delta_table}: {e}")
            continue

        if not rows:
            print(f"  {pg_table}: 0 rows (vazia)")
            continue

        # Check how many already in Lakebase
        cur.execute(f"SELECT COUNT(*) FROM {pg_table}")
        existing = cur.fetchone()[0]

        cols = rows[0].asDict().keys()
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))

        migrated = 0
        for row in rows:
            vals = []
            for c in cols:
                v = row[c]
                # Convert Spark types
                if v is None:
                    vals.append(None)
                elif isinstance(v, str):
                    vals.append(v)
                else:
                    vals.append(str(v))
            try:
                # Use JSON cast for known JSONB columns
                insert_cols = []
                insert_vals = []
                insert_placeholders = []
                for i, c in enumerate(cols):
                    insert_cols.append(c)
                    if c in ("extracted_json", "assessment_json", "token_usage_json"):
                        insert_placeholders.append("%s::jsonb")
                    elif c in ("processado_em", "criado_em", "confirmado_em", "resolvido_em",
                               "ingested_at", "atualizado_em"):
                        insert_placeholders.append("%s::timestamptz")
                    else:
                        insert_placeholders.append("%s")
                    insert_vals.append(vals[i])

                cur.execute(
                    f"INSERT INTO {pg_table} ({', '.join(insert_cols)}) "
                    f"VALUES ({', '.join(insert_placeholders)}) "
                    f"ON CONFLICT DO NOTHING",
                    insert_vals,
                )
                migrated += 1
            except Exception as e:
                conn.rollback()
                conn.autocommit = True
                if migrated == 0:
                    print(f"  ⚠ {pg_table}: erro na primeira row: {str(e)[:100]}")
                    break

        print(f"  ✓ {pg_table}: {migrated} rows migradas (existiam {existing})")

    conn.close()
    print("✓ Migração concluída")
elif not CATALOG:
    print("⚠ catalog não informado — migração pulada")
else:
    print("⚠ migrate_data=false — migração pulada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Relatório

# COMMAND ----------

report = {
    "status": "ok",
    "project": PROJECT,
    "database": DATABASE,
    "host": LAKEBASE_HOST,
    "endpoint": endpoint_path,
    "database_resource_id": DATABASE_RESOURCE_ID,
    "sp_configured": bool(SP_CLIENT_ID),
}

print("=" * 60)
print("  Setup Lakebase concluído!")
print(f"  Host:                {LAKEBASE_HOST}")
print(f"  Database:            {DATABASE}")
print(f"  Database Resource:   {DATABASE_RESOURCE_ID}")
print(f"  SP:                  {SP_CLIENT_ID or 'não configurado'}")
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
