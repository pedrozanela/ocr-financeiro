# Databricks notebook source
# MAGIC %md
# MAGIC # Teste Local do Modelo — sem Serving Endpoint
# MAGIC
# MAGIC Carrega uma versão específica do modelo direto do MLflow e roda predict().
# MAGIC Útil para testar mudanças no depara/regras/prompt sem atualizar o endpoint.

# COMMAND ----------

# MAGIC %pip install openai>=1.0.0 mlflow>=2.10.0 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import mlflow
import pandas as pd

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("model_version", "")
dbutils.widgets.text("pdf_name", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
version = dbutils.widgets.get("model_version").strip()
pdf_name = dbutils.widgets.get("pdf_name").strip()

UC_MODEL = f"{catalog}.{schema}.extrator_financeiro"
SOURCE_TABLE = f"{catalog}.{schema}.documentos"

if not pdf_name:
    dbutils.notebook.exit("pdf_name não informado")

# Use latest version if not specified
if not version:
    from mlflow import MlflowClient
    client = MlflowClient(registry_uri="databricks-uc")
    versions = client.search_model_versions(f"name='{UC_MODEL}'")
    version = str(max(int(v.version) for v in versions))

print(f"Modelo: {UC_MODEL} v{version}")
print(f"PDF: {pdf_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar texto do documento

# COMMAND ----------

rows = spark.sql(f"SELECT document_text FROM {SOURCE_TABLE} WHERE document_name = '{pdf_name.replace(chr(39), chr(39)+chr(39))}'").collect()
if not rows:
    dbutils.notebook.exit(f"Documento '{pdf_name}' não encontrado em {SOURCE_TABLE}")

text = rows[0]["document_text"]
print(f"Texto: {len(text)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregar modelo e rodar predict

# COMMAND ----------

import os

# Inject auth tokens for Foundation Model API calls
_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
_host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not _host.startswith("http"):
    _host = f"https://{_host}"
os.environ["DATABRICKS_TOKEN"] = _token
os.environ["DATABRICKS_HOST"] = _host

model_uri = f"models:/{UC_MODEL}/{version}"
print(f"Carregando {model_uri}...")
model = mlflow.pyfunc.load_model(model_uri)
print("✓ Modelo carregado")

# COMMAND ----------

input_df = pd.DataFrame({"text": [text]})
print("Rodando predict()...")
output = model.predict(input_df)
print("✓ Predict concluído")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resultados

# COMMAND ----------

results = output[0] if isinstance(output, list) else output

if isinstance(results, str):
    results = json.loads(results)

if isinstance(results, list):
    for i, r in enumerate(results):
        print(f"\n{'='*60}")
        print(f"Registro {i+1}: {r.get('tipo_entidade', '?')} / {r.get('identificacao', {}).get('periodo', '?')}")
        print(f"{'='*60}")

        # PL check
        pl = r.get("patrimonio_liquido", {})
        if pl:
            print(f"\n  Patrimônio Líquido:")
            for k, v in sorted(pl.items()):
                if v and v != 0:
                    print(f"    {k}: {v}")

        # Fontes check
        fontes = r.get("fontes", {})
        if fontes:
            print(f"\n  Fontes relevantes:")
            for k, v in sorted(fontes.items()):
                if any(kw in k for kw in ["reserva", "lucro", "capital", "cc_socios", "conta_corrente"]):
                    print(f"    {k}: {v}")
                if "futuro" in str(v).lower() or "afac" in str(v).lower():
                    print(f"    {k}: {v}  ← AFAC!")

        # Postprocessed
        pp = r.get("_postprocessed", [])
        if pp:
            print(f"\n  Postprocessed ({len(pp)}):")
            for p in pp:
                print(f"    {p['campo']}: {p['original']} → {p['corrigido']} | {p['motivo'][:60]}")
else:
    print(json.dumps(results, indent=2, ensure_ascii=False)[:3000])

# Full JSON for inspection
dbutils.notebook.exit(json.dumps(results if isinstance(results, list) else [results], ensure_ascii=False))
