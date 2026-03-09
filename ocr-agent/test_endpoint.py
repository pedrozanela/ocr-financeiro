# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR v4 — Extração em Lote
# MAGIC Lê `pedro_zanela.ia.new_ocr_techfin`, chama o endpoint `techfin-ocr-v4` para cada documento
# MAGIC e salva os resultados em `pedro_zanela.ia.new_ocr_techfin_results`.

# COMMAND ----------

import json
import requests
from pyspark.sql import functions as F

# COMMAND ----------

WORKSPACE_URL = "https://e2-demo-field-eng.cloud.databricks.com"
ENDPOINT_NAME = "techfin-ocr-v4"
INPUT_TABLE  = "pedro_zanela.ia.new_ocr_techfin"
OUTPUT_TABLE = "pedro_zanela.ia.new_ocr_techfin_results"

TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

def call_endpoint(document_text: str) -> dict:
    """Chama o endpoint techfin-ocr-v4 e retorna o JSON extraído."""
    try:
        resp = requests.post(
            f"{WORKSPACE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json",
            },
            json={"dataframe_records": [{"text": document_text}]},
            timeout=180,
        )
        resp.raise_for_status()
        body = resp.json()

        # MLflow envolve a resposta em {"predictions": <result>}
        result = body.get("predictions", body)

        # Se vier como lista, pegar o primeiro elemento
        if isinstance(result, list):
            result = result[0]

        # Se vier como string JSON, parsear
        if isinstance(result, str):
            result = json.loads(result)

        return result
    except Exception as e:
        return {"error": str(e)}

# COMMAND ----------

# Ler a tabela de entrada
df = spark.table(INPUT_TABLE)
print(f"Total de documentos: {df.count()}")
display(df.select("document_name", F.substring("document_text", 1, 150).alias("preview")))

# COMMAND ----------

# Processar cada documento
rows = df.collect()
results = []

for i, row in enumerate(rows):
    print(f"[{i+1}/{len(rows)}] Processando: {row['document_name']}")
    extracted = call_endpoint(row["document_text"])
    results.append({
        "document_name": row["document_name"],
        "extracted_json": json.dumps(extracted, ensure_ascii=False),
    })
    if "error" in extracted:
        print(f"  ERRO: {extracted['error']}")
    else:
        razao = extracted.get("razao_social", "?")
        cnpj  = extracted.get("cnpj", "?")
        ativo = extracted.get("ativo_total", "?")
        print(f"  OK: {razao} | CNPJ: {cnpj} | Ativo Total: {ativo}")

print("\nExtração concluída!")

# COMMAND ----------

# Criar DataFrame com resultados e salvar
results_df = spark.createDataFrame(results)

results_df = (
    results_df
    .withColumn("razao_social",              F.get_json_object("extracted_json", "$.razao_social"))
    .withColumn("cnpj",                      F.get_json_object("extracted_json", "$.cnpj"))
    .withColumn("periodo",                   F.get_json_object("extracted_json", "$.identificacao.periodo"))
    .withColumn("tipo_demonstrativo",        F.get_json_object("extracted_json", "$.identificacao.tipo_demonstrativo"))
    .withColumn("moeda",                     F.get_json_object("extracted_json", "$.identificacao.moeda"))
    .withColumn("escala_valores",            F.get_json_object("extracted_json", "$.identificacao.escala_valores"))
    # Ativo
    .withColumn("ativo_circulante_total",    F.get_json_object("extracted_json", "$.ativo_circulante.total_ativo_circulante").cast("double"))
    .withColumn("ativo_nao_circulante_total",F.get_json_object("extracted_json", "$.ativo_nao_circulante.total_ativo_nao_circulante").cast("double"))
    .withColumn("ativo_permanente_total",    F.get_json_object("extracted_json", "$.ativo_permanente.total_ativo_permanente").cast("double"))
    .withColumn("ativo_total",               F.get_json_object("extracted_json", "$.ativo_total").cast("double"))
    # Passivo
    .withColumn("passivo_circulante_total",    F.get_json_object("extracted_json", "$.passivo_circulante.total_passivo_circulante").cast("double"))
    .withColumn("passivo_nao_circulante_total",F.get_json_object("extracted_json", "$.passivo_nao_circulante.total_passivo_nao_circulante").cast("double"))
    .withColumn("patrimonio_liquido_total",    F.get_json_object("extracted_json", "$.patrimonio_liquido.total_patrimonio_liquido").cast("double"))
    .withColumn("passivo_total",               F.get_json_object("extracted_json", "$.passivo_total").cast("double"))
    # DRE
    .withColumn("receita_operacional_bruta",         F.get_json_object("extracted_json", "$.dre.receita_operacional_bruta").cast("double"))
    .withColumn("receita_operacional_liquida",        F.get_json_object("extracted_json", "$.dre.receita_operacional_liquida").cast("double"))
    .withColumn("lucro_bruto",                        F.get_json_object("extracted_json", "$.dre.lucro_bruto").cast("double"))
    .withColumn("lucro_operacional",                  F.get_json_object("extracted_json", "$.dre.lucro_operacional").cast("double"))
    .withColumn("lucro_antes_imposto_de_renda",       F.get_json_object("extracted_json", "$.dre.lucro_antes_imposto_de_renda").cast("double"))
    .withColumn("lucro_liquido",                      F.get_json_object("extracted_json", "$.dre.lucro_liquido").cast("double"))
    .withColumn("despesas_financeiras",               F.get_json_object("extracted_json", "$.dre.despesas_financeiras").cast("double"))
    .withColumn("receitas_financeiras",               F.get_json_object("extracted_json", "$.dre.receitas_financeiras").cast("double"))
)

results_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)
print(f"Resultados salvos em: {OUTPUT_TABLE}")

# COMMAND ----------

display(
    spark.table(OUTPUT_TABLE).select(
        "document_name", "razao_social", "cnpj", "periodo",
        "tipo_demonstrativo", "escala_valores",
        "ativo_total", "passivo_total", "patrimonio_liquido_total",
        "receita_operacional_bruta", "lucro_bruto", "lucro_liquido"
    )
)
