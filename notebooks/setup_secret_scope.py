# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Setup Secret Scope
# MAGIC
# MAGIC Configura os secrets necessários ao projeto em **2 scopes**:
# MAGIC
# MAGIC 1. **Scope do PAT do projeto** (`secret_scope` / `secret_key`) — guarda um PAT
# MAGIC    do usuário executor para o endpoint `extrator-financeiro` chamar outros
# MAGIC    serving endpoints (Claude Sonnet, etc).
# MAGIC 2. **Scope `techfin`** (opcional) — 4 credenciais OAuth da Techfin/PARC para
# MAGIC    a app submeter balanços. Só é criado se as 4 credenciais forem passadas
# MAGIC    via parâmetros.
# MAGIC
# MAGIC **Quando rodar:** uma vez no setup inicial de um ambiente novo, antes do
# MAGIC `register_model`. Se algum secret já existir, o notebook apenas atualiza.
# MAGIC
# MAGIC **Como passar credenciais Techfin sem deixar no YAML:**
# MAGIC ```bash
# MAGIC databricks bundle run setup_secret_scope --target <target> \
# MAGIC   --params parc_client_id=<id>,parc_client_secret=<secret>,parc_oauth_user=<user>,parc_oauth_password=<pass>
# MAGIC ```
# MAGIC
# MAGIC **Segurança:** valores nunca são impressos no log (só o comprimento). O PAT
# MAGIC é apagado da memória imediatamente após gravado.

# COMMAND ----------

# MAGIC %md ## 1. Parâmetros

# COMMAND ----------

dbutils.widgets.text("secret_scope", "techfin-ocr", "Nome do secret scope do PAT")
dbutils.widgets.text("secret_key",   "pat",         "Nome da chave do PAT no scope")
dbutils.widgets.text("app_sp_id",    "",            "App SP client_id (READ no scope)")
dbutils.widgets.text("pat_comment",  "techfin-ocr-pat", "Comentário do PAT criado")
dbutils.widgets.text("pat_lifetime_days", "365",    "Validade do PAT em dias (0 = nunca expira)")

# Credenciais Techfin/PARC (opcionais — só cria scope se as 4 vierem preenchidas)
dbutils.widgets.text("techfin_scope_name",     "techfin-ocr", "Nome do scope Techfin")
dbutils.widgets.text("parc_client_id",         "",        "PARC client_id (Techfin)")
dbutils.widgets.text("parc_client_secret",     "",        "PARC client_secret (Techfin)")
dbutils.widgets.text("parc_oauth_user",        "",        "PARC OAuth user (Techfin)")
dbutils.widgets.text("parc_oauth_password",    "",        "PARC OAuth password (Techfin)")

SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY   = dbutils.widgets.get("secret_key").strip()
APP_SP_ID    = dbutils.widgets.get("app_sp_id").strip()
PAT_COMMENT  = dbutils.widgets.get("pat_comment").strip()
PAT_LIFETIME_DAYS = int(dbutils.widgets.get("pat_lifetime_days") or "365")

TECHFIN_SCOPE = dbutils.widgets.get("techfin_scope_name").strip() or "techfin"
PARC_CLIENT_ID     = dbutils.widgets.get("parc_client_id").strip()
PARC_CLIENT_SECRET = dbutils.widgets.get("parc_client_secret").strip()
PARC_OAUTH_USER    = dbutils.widgets.get("parc_oauth_user").strip()
PARC_OAUTH_PASS    = dbutils.widgets.get("parc_oauth_password").strip()

if not SECRET_SCOPE or not SECRET_KEY:
    dbutils.notebook.exit("ERRO: secret_scope e secret_key são obrigatórios")

setup_techfin = bool(PARC_CLIENT_ID and PARC_CLIENT_SECRET and PARC_OAUTH_USER and PARC_OAUTH_PASS)

print(f"Secret scope PAT: {SECRET_SCOPE} / {SECRET_KEY}")
print(f"App SP ID       : {APP_SP_ID or '(não configurado)'}")
print(f"PAT comment     : {PAT_COMMENT}")
print(f"PAT lifetime    : {PAT_LIFETIME_DAYS} dias")
print(f"")
print(f"Techfin scope   : {TECHFIN_SCOPE}")
if setup_techfin:
    print(f"  client_id     : ({len(PARC_CLIENT_ID)} chars)")
    print(f"  client_secret : ({len(PARC_CLIENT_SECRET)} chars)")
    print(f"  oauth_user    : {PARC_OAUTH_USER}")
    print(f"  oauth_pass    : ({len(PARC_OAUTH_PASS)} chars)")
else:
    print(f"  (sem credenciais PARC — scope techfin será pulado)")

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

# MAGIC %md ## 6. Scope `techfin` (credenciais OAuth da PARC)
# MAGIC
# MAGIC Só roda se as 4 credenciais foram passadas via parâmetros. Cria/atualiza
# MAGIC os 4 secrets e concede ACL READ ao SP da app.

# COMMAND ----------

if setup_techfin:
    # Cria scope se não existe
    try:
        w.secrets.create_scope(scope=TECHFIN_SCOPE)
        print(f"Scope '{TECHFIN_SCOPE}' criado")
    except ResourceAlreadyExists:
        print(f"Scope '{TECHFIN_SCOPE}' já existe — continuando")
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Scope '{TECHFIN_SCOPE}' já existe — continuando")
        else:
            raise

    # Grava/atualiza as 4 credenciais
    for key, value in [
        ("parc_client_id",      PARC_CLIENT_ID),
        ("parc_client_secret",  PARC_CLIENT_SECRET),
        ("parc_oauth_user",     PARC_OAUTH_USER),
        ("parc_oauth_password", PARC_OAUTH_PASS),
    ]:
        w.secrets.put_secret(scope=TECHFIN_SCOPE, key=key, string_value=value)
        print(f"  ✓ {TECHFIN_SCOPE}/{key}")

    # Concede READ ao SP (se conhecido)
    if APP_SP_ID:
        from databricks.sdk.service.workspace import AclPermission
        try:
            w.secrets.put_acl(scope=TECHFIN_SCOPE, principal=APP_SP_ID, permission=AclPermission.READ)
            print(f"  ✓ READ concedido ao SP {APP_SP_ID} em '{TECHFIN_SCOPE}'")
        except Exception as e:
            print(f"  ⚠ Falha ao conceder ACL em '{TECHFIN_SCOPE}': {e}")

    # Limpa da memória
    del PARC_CLIENT_ID, PARC_CLIENT_SECRET, PARC_OAUTH_USER, PARC_OAUTH_PASS
else:
    print("Scope techfin pulado (sem credenciais PARC nos parâmetros).")
    print("Para configurar depois, rode novamente com:")
    print("  databricks bundle run setup_secret_scope --target <target> \\")
    print("    --params parc_client_id=...,parc_client_secret=...,parc_oauth_user=...,parc_oauth_password=...")

# COMMAND ----------

# MAGIC %md ## 7. Resumo

# COMMAND ----------

print(f"""
✓ Secret scope '{SECRET_SCOPE}' configurado com chave '{SECRET_KEY}' (PAT do projeto)
{('✓ Scope ' + repr(TECHFIN_SCOPE) + ' configurado com 4 credenciais PARC') if setup_techfin else ('· Scope ' + repr(TECHFIN_SCOPE) + ' não foi configurado (rode com --params se quiser)')}

Atualize o target do cliente no databricks.yml:

  targets:
    <cliente>:
      variables:
        secret_scope: "{SECRET_SCOPE}"
        secret_key:   "{SECRET_KEY}"

Próximo passo: rodar o job 'ocr-financeiro-register-model' (cria o endpoint
com DATABRICKS_TOKEN injetado via {{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}).
""")

dbutils.notebook.exit(f"OK scope={SECRET_SCOPE} key={SECRET_KEY} techfin={'yes' if setup_techfin else 'skipped'}")
