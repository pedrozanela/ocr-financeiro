# Guia de Migracao — OCR Financeiro

Passo a passo para deployar o projeto em um novo workspace Databricks.

## Pre-requisitos

- Databricks CLI instalado e autenticado no workspace destino
- SQL Warehouse ativo (Pro ou Serverless)
- Foundation Model API habilitada (Claude Sonnet 4.6)
- Python 3.10+ com conda

## 1. Clonar o repositorio

```bash
git clone git@github.com:pedro-zanela_data/ocr-financeiro.git
cd ocr-financeiro
```

## 2. Autenticar no workspace destino

```bash
databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com --profile PROD
```

## 3. Criar schema, tabelas e volume

```sql
-- Executar no SQL Editor do workspace destino

-- Schema dedicado
CREATE SCHEMA IF NOT EXISTS SEU_CATALOGO.ocr_financeiro;

-- Tabela de documentos (texto extraido dos PDFs)
CREATE TABLE IF NOT EXISTS SEU_CATALOGO.ocr_financeiro.documentos (
    document_name STRING,
    document_text STRING,
    ingested_at TIMESTAMP
) USING DELTA;

-- Tabela de resultados (dados estruturados)
CREATE TABLE IF NOT EXISTS SEU_CATALOGO.ocr_financeiro.resultados (
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
    escala_valores STRING
) USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);

-- Tabela de correcoes (feedback humano)
CREATE TABLE IF NOT EXISTS SEU_CATALOGO.ocr_financeiro.correcoes (
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
) USING DELTA;

-- Volume para PDFs
CREATE VOLUME IF NOT EXISTS SEU_CATALOGO.ocr_financeiro.documentos_pdf;
```

## 4. Criar Service Principal

```bash
# Criar SP
databricks service-principals create --display-name "ocr-financeiro-sp" --profile PROD

# Anotar o applicationId retornado (ex: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee)
```

## 5. Gerar PAT e criar secret scope

```bash
# Gerar PAT (execute logado como admin)
databricks api post /api/2.0/token/create \
    --json '{"comment": "ocr-financeiro SP PAT", "lifetime_seconds": 0}' \
    --profile PROD

# Anotar o token_value retornado

# Criar scope
databricks secrets create-scope ocr-financeiro --profile PROD

# Armazenar PAT
databricks secrets put-secret ocr-financeiro pat-servico \
    --string-value "TOKEN_AQUI" --profile PROD
```

## 6. Conceder permissoes ao SP

```sql
-- Substituir SP_APP_ID pelo applicationId do passo 4

GRANT USE CATALOG ON CATALOG SEU_CATALOGO TO `SP_APP_ID`;
GRANT USE SCHEMA ON SCHEMA SEU_CATALOGO.ocr_financeiro TO `SP_APP_ID`;
GRANT ALL PRIVILEGES ON SCHEMA SEU_CATALOGO.ocr_financeiro TO `SP_APP_ID`;
GRANT ALL PRIVILEGES ON VOLUME SEU_CATALOGO.ocr_financeiro.documentos_pdf TO `SP_APP_ID`;
```

## 7. Upload dos PDFs

Copiar os PDFs para o volume:

```bash
# Um por um
databricks fs cp meu_arquivo.pdf \
    dbfs:/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/meu_arquivo.pdf \
    --profile PROD

# Pasta inteira
databricks fs cp -r ./pdfs/ \
    dbfs:/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/ \
    --profile PROD
```

## 8. Registrar o modelo no MLflow

```bash
# Criar experiment
databricks api post /api/2.0/mlflow/experiments/create \
    --json '{"name": "/Users/SEU_EMAIL/ocr-financeiro"}' \
    --profile PROD

# Anotar o experiment_id retornado
```

Editar `config.py` com os valores do novo ambiente:

```python
UC_CATALOG = "SEU_CATALOGO"
UC_SCHEMA = "ocr_financeiro"
MLFLOW_EXPERIMENT_ID = "EXPERIMENT_ID_ANOTADO"
DATABRICKS_PROFILE = "PROD"
```

Ou exportar como variaveis de ambiente:

```bash
export UC_CATALOG=SEU_CATALOGO
export MLFLOW_EXPERIMENT_ID=EXPERIMENT_ID_ANOTADO
export DATABRICKS_PROFILE=PROD
```

Logar o modelo:

```bash
conda run -n base python scripts/log_new_version.py
```

## 9. Verificar endpoint

O `log_new_version.py` cria o endpoint automaticamente. Se precisar criar manualmente:

```bash
cat > /tmp/endpoint.json << EOF
{
  "name": "extrator-financeiro",
  "config": {
    "served_entities": [{
      "name": "extrator-financeiro",
      "entity_name": "SEU_CATALOGO.ocr_financeiro.extrator_financeiro",
      "entity_version": "1",
      "workload_size": "Small",
      "scale_to_zero_enabled": false,
      "environment_vars": {
        "DATABRICKS_TOKEN": "{{secrets/ocr-financeiro/pat-servico}}"
      }
    }]
  }
}
EOF
databricks serving-endpoints create --json @/tmp/endpoint.json --profile PROD
```

Aguardar ~15min ate o endpoint ficar READY:

```bash
databricks api get /api/2.0/serving-endpoints/extrator-financeiro --profile PROD \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])"
```

## 10. Obter Warehouse ID

```bash
databricks warehouses list --profile PROD
```

Anotar o ID do warehouse que sera usado.

## 11. Criar a Databricks App

Editar `app.yaml` com os valores do novo ambiente:

```yaml
env:
  - name: WAREHOUSE_ID
    value: "SEU_WAREHOUSE_ID"
  - name: SERVERLESS_WAREHOUSE_ID
    value: "SEU_WAREHOUSE_ID"
  - name: UC_CATALOG
    value: "SEU_CATALOGO"
  - name: UC_SCHEMA
    value: "ocr_financeiro"
  - name: OCR_ENDPOINT
    value: "extrator-financeiro"
  - name: SECRET_SCOPE
    value: "ocr-financeiro"
  - name: SECRET_KEY
    value: "pat-servico"
  - name: FEWSHOT_JOB_ID
    value: "JOB_ID_DO_PASSO_12"
```

Tambem atualizar o `resources:` com o scope correto:

```yaml
resources:
  - name: pat-servico
    secret:
      scope: ocr-financeiro
      key: pat-servico
      permission: READ
```

## 12. Build e deploy da app

```bash
# Build frontend
cd frontend && npm install && npx vite build && cd ..

# Sync para o workspace
databricks sync . /Workspace/Users/SEU_EMAIL/techfin --profile PROD --watch=false

# Upload do dist (necessario na primeira vez)
databricks workspace import-dir frontend/dist \
    /Workspace/Users/SEU_EMAIL/techfin/frontend/dist \
    --overwrite --profile PROD

# Criar a app
databricks apps create --name ocr-financeiro \
    --description "Extracao de Balancos Patrimoniais" --profile PROD

# Deploy
databricks apps deploy ocr-financeiro \
    --source-code-path /Workspace/Users/SEU_EMAIL/techfin \
    --profile PROD
```

## 13. Conceder permissoes ao SP da App

Apos o primeiro deploy, obter o SP da app:

```bash
databricks apps get ocr-financeiro --profile PROD | grep service_principal
```

Conceder permissoes (substituir APP_SP_ID):

```sql
GRANT USE CATALOG ON CATALOG SEU_CATALOGO TO `APP_SP_ID`;
GRANT USE SCHEMA ON SCHEMA SEU_CATALOGO.ocr_financeiro TO `APP_SP_ID`;
GRANT SELECT, MODIFY ON SCHEMA SEU_CATALOGO.ocr_financeiro TO `APP_SP_ID`;
GRANT ALL PRIVILEGES ON VOLUME SEU_CATALOGO.ocr_financeiro.documentos_pdf TO `APP_SP_ID`;
```

## 14. Criar job de atualizacao do modelo

```bash
cat > /tmp/job.json << EOF
{
  "name": "ocr-financeiro-atualizar-modelo",
  "tasks": [{
    "task_key": "update_fewshot",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/SEU_EMAIL/techfin/notebooks/update_fewshot",
      "source": "WORKSPACE"
    },
    "environment_key": "Default"
  }],
  "environments": [{"environment_key": "Default", "spec": {"client": "1"}}],
  "schedule": {
    "quartz_cron_expression": "0 0 6 ? * MON",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "timeout_seconds": 3600
}
EOF
databricks jobs create --json @/tmp/job.json --profile PROD
```

Anotar o job_id e atualizar no `app.yaml` (variavel `FEWSHOT_JOB_ID`), re-deploy:

```bash
databricks sync . /Workspace/Users/SEU_EMAIL/techfin --profile PROD --watch=false
databricks apps deploy ocr-financeiro \
    --source-code-path /Workspace/Users/SEU_EMAIL/techfin --profile PROD
```

## 15. Atualizar o notebook com valores do ambiente

Editar `notebooks/update_fewshot.py` linhas 19-24:

```python
CORRECTIONS_TABLE = "SEU_CATALOGO.ocr_financeiro.correcoes"
RESULTS_TABLE = "SEU_CATALOGO.ocr_financeiro.resultados"
UC_MODEL_NAME = "SEU_CATALOGO.ocr_financeiro.extrator_financeiro"
ENDPOINT_NAME = "extrator-financeiro"
WORKSPACE_PATH = "/Workspace/Users/SEU_EMAIL/techfin"
```

E a celula de volume (linha ~183):

```python
catalog = "SEU_CATALOGO"
schema = "ocr_financeiro"
```

E o secret na celula do endpoint (linha ~270):

```python
"DATABRICKS_TOKEN": "{{secrets/ocr-financeiro/pat-servico}}"
```

Re-sync:

```bash
databricks sync . /Workspace/Users/SEU_EMAIL/techfin --profile PROD --watch=false
```

## 16. Testar

1. Acessar a URL da app (retornada no `databricks apps get ocr-financeiro`)
2. Fazer upload de um PDF de balanco patrimonial
3. Verificar se a extracao funciona (deve levar 1-3 minutos)
4. Testar correcao manual de um campo
5. Clicar "Atualizar Modelo" e verificar que o job roda com sucesso
6. Exportar Excel e validar

## Checklist

- [ ] Workspace autenticado
- [ ] Schema + tabelas + volume criados
- [ ] Service Principal criado
- [ ] PAT gerado e armazenado no secret scope
- [ ] Permissoes do SP concedidas
- [ ] PDFs no volume
- [ ] Modelo registrado no MLflow
- [ ] Endpoint READY
- [ ] App deployada e acessivel
- [ ] Permissoes do SP da App concedidas
- [ ] Job de atualizacao criado
- [ ] Notebook atualizado com valores do ambiente
- [ ] Upload de PDF testado com sucesso
