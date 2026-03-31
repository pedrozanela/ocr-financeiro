# Guia de Deploy — OCR Financeiro

Deploy do projeto em um novo workspace Databricks usando Databricks Asset Bundles (DABs).

## Pre-requisitos

- Databricks CLI v0.230+ instalado (`databricks --version`)
- CLI autenticado no workspace destino (`databricks auth login`)
- SQL Warehouse disponivel (Pro ou Serverless)
- Foundation Model API habilitada (Claude Sonnet 4.6)
- Node.js 18+ (para build do frontend)
- Python 3.10+ com pip (para `log_new_version.py` local — opcional)

## Visao Geral

O deploy usa DABs (`databricks.yml`) para gerenciar jobs, sync de codigo e parametrizacao.
A app Databricks e o serving endpoint sao criados via CLI separadamente.

```
1. Configurar variaveis     →  databricks.yml, config.py, app.yaml
2. Build frontend           →  npm install && npx vite build
3. Criar secret scope + PAT →  databricks secrets ...
4. Deploy bundle            →  databricks bundle deploy
5. Setup infraestrutura     →  databricks bundle run setup_infrastructure
6. Registrar modelo         →  databricks bundle run register_model
7. Criar serving endpoint   →  databricks serving-endpoints create ...
8. Upload PDFs              →  databricks fs cp ...
9. Criar e deployar app     →  databricks apps create + deploy
10. Conceder permissoes     →  databricks bundle run grant_permissions
11. Ingerir texto + extrair →  SQL + databricks bundle run run_llm_extraction
```

## 1. Configurar variaveis

Editar `databricks.yml` — substituir as variaveis `default` pelo seu ambiente:

```yaml
variables:
  catalog:
    default: SEU_CATALOGO             # ex: meu_catalogo
  schema:
    default: ocr_financeiro           # pode manter
  warehouse_id:
    default: "SEU_WAREHOUSE_ID"       # databricks warehouses list
  serverless_warehouse_id:
    default: "SEU_SERVERLESS_WH_ID"   # pode ser o mesmo
  secret_scope:
    default: "ocr-financeiro"         # pode manter
  secret_key:
    default: "pat-servico"            # pode manter
  endpoint_name:
    default: "extrator-financeiro"    # pode manter

workspace:
  root_path: /Workspace/Users/SEU_EMAIL/.bundle/ocr-financeiro

targets:
  meu_target:
    default: true
    workspace:
      profile: MEU_PROFILE
```

Editar `config.py` — atualizar os defaults (usado como fallback no dev local):

```python
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://SEU-WORKSPACE.cloud.databricks.com")
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "MEU_PROFILE")
UC_CATALOG = os.environ.get("UC_CATALOG", "SEU_CATALOGO")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "SEU_WAREHOUSE_ID")
SERVERLESS_WAREHOUSE_ID = os.environ.get("SERVERLESS_WAREHOUSE_ID", "SEU_SERVERLESS_WH_ID")
```

Editar `app.yaml` — atualizar env vars (usado pela Databricks App):

```yaml
env:
  - name: WAREHOUSE_ID
    value: "SEU_WAREHOUSE_ID"
  - name: SERVERLESS_WAREHOUSE_ID
    value: "SEU_SERVERLESS_WH_ID"
  - name: UC_CATALOG
    value: "SEU_CATALOGO"
```

> **Nota**: `MLFLOW_EXPERIMENT_ID` e `FEWSHOT_JOB_ID` serao atualizados nos passos seguintes.

> **Azure vs AWS**: O `databricks.yml` usa a variavel `node_type_id` para o tipo de instancia
> dos clusters. O default e `Standard_DS3_v2` (Azure). Para AWS, use `i3.xlarge`.
> O target `fevm` no bundle ja faz esse override automaticamente.

## 2. Build do frontend

```bash
cd frontend && npm install && npx vite build && cd ..
```

O diretorio `frontend/dist/` sera sincronizado pelo bundle. Os fontes React ficam no `.databricksignore`.

## 3. Criar secret scope e PAT

O PAT e usado pelos notebooks e pelo serving endpoint para chamar a Foundation Model API.

```bash
# Criar scope
databricks secrets create-scope ocr-financeiro --profile MEU_PROFILE

# Gerar PAT (90 dias)
databricks tokens create --comment "ocr-financeiro" --lifetime-seconds 7776000 --profile MEU_PROFILE
# Anotar o token_value retornado

# Armazenar no scope
databricks secrets put-secret ocr-financeiro pat-servico \
    --string-value "TOKEN_AQUI" --profile MEU_PROFILE
```

> **Importante**: O PAT precisa ter acesso a Foundation Model API. Teste com:
> ```bash
> curl -s -X POST "https://SEU-WORKSPACE.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations" \
>   -H "Authorization: Bearer TOKEN_AQUI" \
>   -H "Content-Type: application/json" \
>   -d '{"messages":[{"role":"user","content":"hi"}],"max_tokens":5}'
> ```

## 4. Deploy do bundle

```bash
# Validar configuracao
databricks bundle validate --target meu_target

# Deploy (sync de arquivos + criacao de jobs)
databricks bundle deploy --target meu_target
```

Isso cria 7 jobs no workspace e sincroniza todo o codigo.

Anotar o job_id de `ocr-financeiro-atualizar-modelo` (visivel na saida ou via `databricks jobs list`),
e atualizar em `app.yaml` (`FEWSHOT_JOB_ID`) e `config.py` (`FEWSHOT_JOB_ID`). Re-deploy:

```bash
databricks bundle deploy --target meu_target
```

## 5. Setup da infraestrutura

```bash
databricks bundle run setup_infrastructure --target meu_target
```

Cria: schema, 4 tabelas (documentos, resultados, resultados_final, correcoes) e volume (documentos_pdf).

Tempo: ~5-7min (inclui provisionamento de cluster).

## 6. Registrar modelo no MLflow

```bash
databricks bundle run register_model --target meu_target
```

Registra `SEU_CATALOGO.ocr_financeiro.extrator_financeiro` v1 no Unity Catalog.
O notebook resolve o experiment automaticamente (cria se nao existir).

Tempo: ~5-7min.

> **Alternativa local** (se quiser rodar da maquina — requer `pip install mlflow boto3 databricks-sdk openai`):
> ```bash
> DATABRICKS_CONFIG_PROFILE=MEU_PROFILE python scripts/log_new_version.py
> ```
> Nota: pode falhar com erro de S3 se o workspace nao permitir uploads diretos.
> Nesse caso, use o job do bundle (metodo acima).

## 7. Criar serving endpoint

Substituir `SEU_CATALOGO` e a versao do modelo:

```bash
cat > /tmp/endpoint.json << 'EOF'
{
  "name": "extrator-financeiro",
  "config": {
    "served_entities": [{
      "name": "extrator-financeiro",
      "entity_name": "SEU_CATALOGO.ocr_financeiro.extrator_financeiro",
      "entity_version": "1",
      "workload_size": "Small",
      "scale_to_zero_enabled": true,
      "environment_vars": {
        "OCR_PAT": "{{secrets/ocr-financeiro/pat-servico}}",
        "DATABRICKS_HOST": "https://SEU-WORKSPACE.cloud.databricks.com"
      }
    }]
  }
}
EOF
databricks serving-endpoints create --json @/tmp/endpoint.json --profile MEU_PROFILE
```

Aguardar ate READY (~10-15min):

```bash
# Polling
databricks serving-endpoints get extrator-financeiro --profile MEU_PROFILE | grep ready
```

> **IMPORTANTE — `OCR_PAT` e `DATABRICKS_HOST`**:
> - Use `OCR_PAT` (nao `DATABRICKS_TOKEN`) para injetar o PAT. A plataforma tambem define
>   `DATABRICKS_TOKEN` com a credencial do endpoint, que pode nao ter acesso a FMAPI.
> - `DATABRICKS_HOST` e obrigatorio — sem ele, o modelo nao sabe para qual workspace direcionar
>   as chamadas FMAPI. Use a URL completa com `https://`.

## 8. Upload de PDFs

```bash
# Um arquivo
databricks fs cp "meu_balanco.pdf" \
    "dbfs:/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/meu_balanco.pdf" \
    --profile MEU_PROFILE

# Pasta inteira
databricks fs cp -r ./pdfs/ \
    "dbfs:/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/" \
    --profile MEU_PROFILE
```

## 9. Criar e deployar a Databricks App

```bash
# Criar a app (aguarda provisionamento ~2min)
databricks apps create NOME-DA-APP --description "Extrator de dados financeiros" \
    --no-wait --profile MEU_PROFILE

# Aguardar status ACTIVE
databricks apps get NOME-DA-APP --profile MEU_PROFILE | grep state

# Deploy do codigo
databricks apps deploy NOME-DA-APP \
    --source-code-path /Workspace/Users/SEU_EMAIL/.bundle/ocr-financeiro/files \
    --profile MEU_PROFILE
```

> **Gotcha**: Se `databricks apps create` falhar com "App creation failed unexpectedly",
> delete e tente novamente com um nome diferente. Pode haver conflito com apps deletadas recentemente.

## 10. Conceder permissoes ao SP da App

Obter o `service_principal_client_id` da app:

```bash
databricks apps get NOME-DA-APP --profile MEU_PROFILE | grep service_principal_client_id
```

Rodar o job de grant (substituir `SP_CLIENT_ID`):

```bash
databricks jobs run-now --json '{"job_id": JOB_ID_GRANT_PERMISSIONS, "notebook_params": {"sp_client_id": "SP_CLIENT_ID", "catalog": "SEU_CATALOGO", "schema": "ocr_financeiro"}}' --profile MEU_PROFILE
```

Conceder acesso adicional ao SP:

```bash
# Acesso ao serving endpoint
databricks serving-endpoints update-permissions ENDPOINT_ID \
    --json '{"access_control_list":[{"service_principal_name":"SP_CLIENT_ID","permission_level":"CAN_QUERY"}]}' \
    --profile MEU_PROFILE

# Acesso ao secret scope
databricks secrets put-acl ocr-financeiro "SP_CLIENT_ID" READ --profile MEU_PROFILE

# Acesso ao warehouse
databricks api patch /api/2.0/permissions/sql/warehouses/SEU_WAREHOUSE_ID \
    --json '{"access_control_list":[{"service_principal_name":"SP_CLIENT_ID","permission_level":"CAN_USE"}]}' \
    --profile MEU_PROFILE
```

## 11. Ingerir texto e extrair dados

Os PDFs estao no volume mas precisam ter o texto extraido e inserido na tabela `documentos`.

### Opcao A: Via SQL (recomendado para poucos PDFs)

Executar no SQL Editor ou via API — substituir nomes:

```sql
INSERT INTO SEU_CATALOGO.ocr_financeiro.documentos (document_name, document_text, ingested_at)
SELECT
    'meu_balanco.pdf',
    concat_ws('\n\n',
        transform(
            try_cast(ai_parse_document(content):document:elements AS ARRAY<VARIANT>),
            element -> try_cast(element:content AS STRING)
        )
    ),
    current_timestamp()
FROM read_files('/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/meu_balanco.pdf', format => 'binaryFile')
```

> **Nota**: `ai_parse_document` requer Serverless SQL Warehouse. Use o Serverless Starter Warehouse.

Depois, rodar a extracao LLM:

```bash
databricks bundle run run_llm_extraction --target meu_target
```

### Opcao B: Via app (recomendado para uso continuo)

Abrir a URL da app no navegador e usar o botao "Upload" — a app faz tudo automaticamente
(ai_parse_document + endpoint OCR + salva resultados).

## 12. Verificar

1. Acessar a URL da app (retornada por `databricks apps get NOME-DA-APP`)
2. Verificar que o documento aparece na lista lateral
3. Navegar pelas abas: Identificacao, Ativo, Passivo, DRE, Fontes, Pontos de Atencao
4. Testar correcao manual de um campo
5. Exportar Excel e validar formato

## Checklist

```
[ ] Variaveis configuradas (databricks.yml, config.py, app.yaml)
[ ] Frontend buildado (frontend/dist/)
[ ] Secret scope criado com PAT valido
[ ] Bundle deployado (databricks bundle deploy)
[ ] FEWSHOT_JOB_ID atualizado e re-deployado
[ ] Infraestrutura criada (setup_infrastructure)
[ ] Modelo registrado (register_model)
[ ] Serving endpoint READY
[ ] PDFs no volume
[ ] App criada e deployada
[ ] Permissoes do SP da App concedidas (UC, endpoint, secrets, warehouse)
[ ] Texto extraido e LLM rodou com sucesso
[ ] App acessivel e mostrando dados
```

## Troubleshooting

### Endpoint retorna 403 "Invalid access token"
O modelo nao consegue chamar a Foundation Model API. Verificar:
1. O PAT no secret scope esta valido? Testar com `curl` (passo 3).
2. O endpoint usa `OCR_PAT` (nao `DATABRICKS_TOKEN`)? A plataforma injeta `DATABRICKS_TOKEN`
   com a credencial do endpoint, que pode nao ter acesso FMAPI. Usar `OCR_PAT` evita colisao.
3. O endpoint tem `DATABRICKS_HOST` definido? Sem ele, o modelo nao sabe o host para FMAPI.
4. Se atualizou o secret, force um restart: atualize a versao do modelo no endpoint config.

### Endpoint retorna "Connection error"
O modelo nao consegue resolver o host do workspace. Adicionar `DATABRICKS_HOST` nas
env vars do endpoint com a URL completa (ex: `https://adb-XXX.Y.azuredatabricks.net`).

### `ai_parse_document` falha no job `reprocess_all`
Esta funcao so roda em Serverless SQL Warehouse, nao em clusters comuns.
Use a Opcao A (SQL direto) ou a app para ingestao de texto.

### App retorna lista vazia de documentos
1. Verificar que existem linhas em `resultados`: `SELECT COUNT(*) FROM SEU_CATALOGO.ocr_financeiro.resultados`
2. Verificar que o SP da app tem SELECT no schema: re-rodar o grant_permissions job.
3. Verificar que o warehouse esta ativo e o SP tem CAN_USE.

### `databricks apps create` falha repetidamente
Pode haver conflito de nome com app deletada recentemente. Tente um nome diferente
ou aguarde ~5min apos a delecao.

### Modelo nao registra via `log_new_version.py` local (erro S3 AccessDenied)
Workspaces com restricoes de rede nao permitem upload direto de artifacts S3 da maquina local.
Use o job `register_model` do bundle (roda no cluster dentro do workspace).

### Job `grant_permissions` ignora o `sp_client_id`
O job usa `base_parameters` do `databricks.yml` (vazio por padrao). Para injetar o SP,
use `notebook_params` no `run-now`:
```bash
databricks jobs run-now --json '{"job_id": ID, "notebook_params": {"sp_client_id": "SP_ID", ...}}'
```
