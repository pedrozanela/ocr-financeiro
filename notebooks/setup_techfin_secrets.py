# Databricks notebook source
# MAGIC %md
# MAGIC # Setup secret scope `techfin` para integração PARC
# MAGIC
# MAGIC Cria o scope `techfin` com as 4 credenciais OAuth da Techfin/PARC e concede
# MAGIC READ ao SP da app. Idempotente.
# MAGIC
# MAGIC ## Como rodar
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run setup_techfin_secrets --target <target> \
# MAGIC   --params parc_client_id=<id>,parc_client_secret=<secret>,parc_oauth_user=<user>,parc_oauth_password=<pass>
# MAGIC ```
# MAGIC
# MAGIC Ou abra o notebook no workspace e preencha os widgets manualmente.
# MAGIC
# MAGIC ## Segurança
# MAGIC
# MAGIC - As credenciais NÃO são impressas no log (só comprimento da string para validar)
# MAGIC - O notebook não persiste os valores em lugar nenhum além do próprio scope
# MAGIC - Após o run, recomenda-se limpar o output da run no workspace UI

# COMMAND ----------

dbutils.widgets.text("parc_client_id", "", label="PARC client_id")
dbutils.widgets.text("parc_client_secret", "", label="PARC client_secret")
dbutils.widgets.text("parc_oauth_user", "", label="PARC OAuth user")
dbutils.widgets.text("parc_oauth_password", "", label="PARC OAuth password")
dbutils.widgets.text("app_sp_id", "", label="SP client_id da app (pra ACL READ)")
dbutils.widgets.text("scope_name", "techfin", label="Scope name")

client_id     = dbutils.widgets.get("parc_client_id").strip()
client_secret = dbutils.widgets.get("parc_client_secret").strip()
oauth_user    = dbutils.widgets.get("parc_oauth_user").strip()
oauth_pass    = dbutils.widgets.get("parc_oauth_password").strip()
app_sp_id     = dbutils.widgets.get("app_sp_id").strip()
scope_name    = dbutils.widgets.get("scope_name").strip() or "techfin"

# Validação básica sem expor valores
missing = []
if not client_id:     missing.append("parc_client_id")
if not client_secret: missing.append("parc_client_secret")
if not oauth_user:    missing.append("parc_oauth_user")
if not oauth_pass:    missing.append("parc_oauth_password")
if missing:
    raise ValueError(f"Parâmetros faltando: {', '.join(missing)}")

print(f"Scope:          {scope_name}")
print(f"client_id:      ({len(client_id)} chars)")
print(f"client_secret:  ({len(client_secret)} chars)")
print(f"oauth_user:     {oauth_user}")
print(f"oauth_pass:     ({len(oauth_pass)} chars)")
print(f"App SP:         {app_sp_id or '(vazio — pula ACL)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar scope (se não existe)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Lista scopes existentes
existing_scopes = {s.name for s in w.secrets.list_scopes()}
if scope_name in existing_scopes:
    print(f"  · Scope '{scope_name}' já existe")
else:
    w.secrets.create_scope(scope=scope_name)
    print(f"  ✓ Scope '{scope_name}' criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gravar/atualizar as 4 credenciais

# COMMAND ----------

# put_secret é idempotente: cria ou sobrescreve
for key, value in [
    ("parc_client_id",      client_id),
    ("parc_client_secret",  client_secret),
    ("parc_oauth_user",     oauth_user),
    ("parc_oauth_password", oauth_pass),
]:
    w.secrets.put_secret(scope=scope_name, key=key, string_value=value)
    print(f"  ✓ {key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Conceder READ ao SP da app

# COMMAND ----------

if app_sp_id:
    from databricks.sdk.service.workspace import AclPermission
    try:
        w.secrets.put_acl(scope=scope_name, principal=app_sp_id, permission=AclPermission.READ)
        print(f"  ✓ READ concedido ao SP {app_sp_id}")
    except Exception as e:
        print(f"  ✗ Erro concedendo ACL: {e}")
        print(f"    (rode `databricks secrets put-acl {scope_name} {app_sp_id} READ` manualmente)")
else:
    print("  · app_sp_id vazio — pulando ACL. Concede depois com:")
    print(f"    databricks secrets put-acl {scope_name} <SP> READ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar estado final

# COMMAND ----------

print(f"\nKeys no scope '{scope_name}':")
for s in w.secrets.list_secrets(scope=scope_name):
    print(f"  · {s.key}")

print(f"\nACLs no scope '{scope_name}':")
for acl in w.secrets.list_acls(scope=scope_name):
    print(f"  · {acl.principal:50s} {acl.permission}")

dbutils.notebook.exit("ok")
