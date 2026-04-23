# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Setup Secret Scope
# MAGIC
# MAGIC Cria o secret scope e armazena um PAT que o endpoint `extrator-financeiro`
# MAGIC usa para chamar outros serving endpoints (Claude Sonnet, etc).
# MAGIC
# MAGIC **Quando rodar:** uma vez no setup inicial de um ambiente novo, antes do
# MAGIC `register_model`. Se o secret já existir, o notebook apenas atualiza.
# MAGIC
# MAGIC **Requisitos:**
# MAGIC - Usuário executor deve ter permissão de criar secret scopes no workspace
# MAGIC - O PAT gerado herda as permissões do usuário executor — idealmente
# MAGIC   executar com um Service Principal dedicado ao TechFin OCR
# MAGIC - Após criar o PAT, este notebook o remove da memória imediatamente

# COMMAND ----------

# MAGIC %md ## 1. Parâmetros

# COMMAND ----------

dbutils.widgets.text("secret_scope", "techfin-ocr", "Nome do secret scope")
dbutils.widgets.text("secret_key",   "pat",         "Nome da chave dentro do scope")
dbutils.widgets.text("app_sp_id",    "",            "Application ID do SP da app (opcional, para conceder READ)")
dbutils.widgets.text("pat_comment",  "techfin-ocr-pat", "Comentário do PAT criado")
dbutils.widgets.text("pat_lifetime_days", "365",    "Validade do PAT em dias (0 = nunca expira)")

SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY   = dbutils.widgets.get("secret_key").strip()
APP_SP_ID    = dbutils.widgets.get("app_sp_id").strip()
PAT_COMMENT  = dbutils.widgets.get("pat_comment").strip()
PAT_LIFETIME_DAYS = int(dbutils.widgets.get("pat_lifetime_days") or "365")

if not SECRET_SCOPE or not SECRET_KEY:
    dbutils.notebook.exit("ERRO: secret_scope e secret_key são obrigatórios")

print(f"Secret scope    : {SECRET_SCOPE}")
print(f"Secret key      : {SECRET_KEY}")
print(f"App SP ID       : {APP_SP_ID or '(não configurado)'}")
print(f"PAT comment     : {PAT_COMMENT}")
print(f"PAT lifetime    : {PAT_LIFETIME_DAYS} dias")

# COMMAND ----------

# MAGIC %md ## 2. Gerar PAT do usuário executor

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
lifetime_seconds = PAT_LIFETIME_DAYS * 24 * 3600 if PAT_LIFETIME_DAYS > 0 else None

token_resp = w.tokens.create(
    comment=PAT_LIFETIME_DAYS and PAT_COMMENT or f"{PAT_COMMENT} (no expiry)",
    lifetime_seconds=lifetime_seconds,
)
pat_value = token_resp.token_value
pat_id    = token_resp.token_info.token_id
print(f"PAT criado com sucesso (token_id={pat_id})")

# COMMAND ----------

# MAGIC %md ## 3. Criar secret scope (idempotente)

# COMMAND ----------

from databricks.sdk.errors import ResourceAlreadyExists

try:
    w.secrets.create_scope(scope=SECRET_SCOPE)
    print(f"Scope '{SECRET_SCOPE}' criado")
except ResourceAlreadyExists:
    print(f"Scope '{SECRET_SCOPE}' já existe — continuando")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"Scope '{SECRET_SCOPE}' já existe — continuando")
    else:
        raise

# COMMAND ----------

# MAGIC %md ## 4. Guardar o PAT no scope

# COMMAND ----------

w.secrets.put_secret(
    scope=SECRET_SCOPE,
    key=SECRET_KEY,
    string_value=pat_value,
)
# Apaga da memória imediatamente
del pat_value
del token_resp

print(f"PAT armazenado em '{SECRET_SCOPE}/{SECRET_KEY}'")

# COMMAND ----------

# MAGIC %md ## 5. (Opcional) Conceder READ ao SP da app

# COMMAND ----------

if APP_SP_ID:
    from databricks.sdk.service.workspace import AclPermission
    try:
        w.secrets.put_acl(
            scope=SECRET_SCOPE,
            principal=APP_SP_ID,
            permission=AclPermission.READ,
        )
        print(f"SP '{APP_SP_ID}' tem READ em '{SECRET_SCOPE}'")
    except Exception as e:
        print(f"⚠ Falha ao conceder ACL: {e}")
        print("   Conceda manualmente: databricks secrets put-acl {SECRET_SCOPE} {APP_SP_ID} READ")
else:
    print("app_sp_id não informado — pule este passo ou rode novamente informando o ID")

# COMMAND ----------

# MAGIC %md ## 6. Resumo

# COMMAND ----------

print(f"""
✓ Secret scope '{SECRET_SCOPE}' configurado com chave '{SECRET_KEY}'.

Atualize o target do cliente no databricks.yml:

  targets:
    <cliente>:
      variables:
        secret_scope: "{SECRET_SCOPE}"
        secret_key:   "{SECRET_KEY}"

Próximo passo: rodar o job 'ocr-financeiro-register-model' (cria o endpoint
com DATABRICKS_TOKEN injetado via {{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}).
""")

dbutils.notebook.exit(f"OK scope={SECRET_SCOPE} key={SECRET_KEY}")
