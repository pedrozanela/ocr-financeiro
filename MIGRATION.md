# Guia de Deploy — OCR Financeiro

Deploy do projeto em um novo workspace Databricks usando Databricks Asset Bundles (DABs).

## Pre-requisitos

- Databricks CLI v0.230+ instalado (`databricks --version`)
- CLI autenticado no workspace destino (`databricks auth login`)
- SQL Warehouse disponivel (Pro ou Serverless)
- Foundation Model API habilitada (Claude Sonnet 4.6)
- Node.js 18+ (para build do frontend)

## Visao Geral

O deploy usa DABs (`databricks.yml`) para gerenciar jobs, sync de codigo e parametrizacao.
A app Databricks e o serving endpoint sao criados via CLI separadamente.

**Tempo total estimado: ~40min** (a maior parte e espera por provisionamento de endpoints/clusters).

```
1. Configurar variaveis     →  databricks.yml, app.yaml
2. Build frontend           →  npm install && npx vite build
3. Criar secret scope + PAT →  databricks secrets ...
4. Deploy bundle            →  databricks bundle deploy
5. Setup infraestrutura     →  databricks bundle run setup_infrastructure     (~5min)
6. Registrar modelo         →  databricks bundle run register_model           (~5min)
7. Criar serving endpoint   →  databricks serving-endpoints create ...        (~12min)
8. Upload PDFs              →  databricks fs cp ...
9. Criar e deployar app     →  databricks apps create + deploy               (~3min)
10. Conceder permissoes     →  grant_permissions job + CLI
11. Ingerir texto + extrair →  SQL + databricks bundle run run_llm_extraction (~5min)
12. Verificar               →  abrir a app no browser
```

## 1. Configurar variaveis

Apenas **dois arquivos** precisam ser editados. Os notebooks sao parametrizados pelo DABs
automaticamente — nao precisam de edição manual.

### 1a. `databricks.yml`

Editar as variaveis `default` e o target:

```yaml
variables:
  catalog:
    default: SEU_CATALOGO             # ex: meu_catalogo
  schema:
    default: ocr_financeiro           # pode manter ou trocar
  warehouse_id:
    default: "SEU_WAREHOUSE_ID"       # databricks warehouses list --profile PERFIL
  serverless_warehouse_id:
    default: "SEU_SERVERLESS_WH_ID"   # pode ser o mesmo warehouse
  node_type_id:
    default: "Standard_DS3_v2"        # Azure: Standard_DS3_v2 | AWS: i3.xlarge

targets:
  meu_target:
    default: true
    workspace:
      profile: MEU_PROFILE
```

> O `workspace.root_path` usa `${workspace.current_user.userName}` — nao precisa editar.

### 1b. `app.yaml`

Atualizar as env vars da Databricks App:

```yaml
env:
  - name: WAREHOUSE_ID
    value: "SEU_WAREHOUSE_ID"
  - name: SERVERLESS_WAREHOUSE_ID
    value: "SEU_SERVERLESS_WH_ID"
  - name: UC_CATALOG
    value: "SEU_CATALOGO"
  - name: UC_SCHEMA
    value: "ocr_financeiro"
```

> `config.py` nao precisa ser editado para deploy — os valores vem de `app.yaml` (app) e
> `databricks.yml` (jobs). So edite se for usar scripts locais.

## 2. Build do frontend

```bash
cd frontend && npm install && npx vite build && cd ..
```

O diretorio `frontend/dist/` sera sincronizado pelo bundle. Os fontes React ficam no `.databricksignore`.

## 3. Criar secret scope e PAT

O PAT e usado pelo serving endpoint para chamar a Foundation Model API (Claude).

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

**Validar** que o PAT tem acesso a Foundation Model API:

```bash
curl -s -X POST "https://SEU-WORKSPACE/serving-endpoints/databricks-claude-sonnet-4-6/invocations" \
  -H "Authorization: Bearer TOKEN_AQUI" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"hi"}],"max_tokens":5}'
```

Se retornar JSON com `choices`, o PAT esta OK. Se retornar 403, o PAT nao tem acesso FMAPI.

## 4. Deploy do bundle

```bash
databricks bundle validate --target meu_target
databricks bundle deploy --target meu_target
```

Isso cria 7 jobs no workspace e sincroniza todo o codigo.

## 5. Setup da infraestrutura

```bash
databricks bundle run setup_infrastructure --target meu_target
```

Cria: schema, 4 tabelas (documentos, resultados, resultados_final, correcoes) e volume (documentos_pdf).

## 6. Registrar modelo no MLflow

```bash
databricks bundle run register_model --target meu_target
```

Registra `SEU_CATALOGO.SEU_SCHEMA.extrator_financeiro` v1 no Unity Catalog.
O notebook resolve o experiment automaticamente (cria se nao existir).

## 7. Criar serving endpoint

Substituir `SEU_CATALOGO`, `SEU_SCHEMA`, e `SEU_WORKSPACE`:

```bash
cat > /tmp/endpoint.json << 'EOF'
{
  "name": "extrator-financeiro",
  "config": {
    "served_entities": [{
      "name": "extrator-financeiro",
      "entity_name": "SEU_CATALOGO.SEU_SCHEMA.extrator_financeiro",
      "entity_version": "1",
      "workload_size": "Small",
      "scale_to_zero_enabled": true,
      "environment_vars": {
        "OCR_PAT": "{{secrets/ocr-financeiro/pat-servico}}",
        "DATABRICKS_HOST": "https://SEU-WORKSPACE"
      }
    }]
  }
}
EOF
databricks serving-endpoints create --json @/tmp/endpoint.json --profile MEU_PROFILE
```

Aguardar ate READY (~10-15min):

```bash
databricks serving-endpoints get extrator-financeiro --profile MEU_PROFILE | grep ready
```

> **CRITICO — `OCR_PAT` e `DATABRICKS_HOST`**:
> - Use `OCR_PAT` (nao `DATABRICKS_TOKEN`) para injetar o PAT. A plataforma define
>   `DATABRICKS_TOKEN` automaticamente com a credencial do endpoint, que nao tem acesso FMAPI.
>   Se usar `DATABRICKS_TOKEN`, o model code vai pegar a credencial errada.
> - `DATABRICKS_HOST` e obrigatorio — sem ele, o modelo nao sabe para qual workspace direcionar
>   as chamadas FMAPI. Use a URL completa com `https://`.

## 8. Upload de PDFs

```bash
# Um arquivo
databricks fs cp "meu_balanco.pdf" \
    "dbfs:/Volumes/SEU_CATALOGO/SEU_SCHEMA/documentos_pdf/meu_balanco.pdf" \
    --profile MEU_PROFILE

# Pasta inteira
databricks fs cp -r ./pdfs/ \
    "dbfs:/Volumes/SEU_CATALOGO/SEU_SCHEMA/documentos_pdf/" \
    --profile MEU_PROFILE
```

## 9. Criar e deployar a Databricks App

```bash
# Criar a app (aguarda provisionamento ~2-3min)
databricks apps create NOME-DA-APP --description "Extrator de dados financeiros" \
    --no-wait --profile MEU_PROFILE

# Aguardar status ACTIVE (poll a cada 10s)
databricks apps get NOME-DA-APP --profile MEU_PROFILE | grep state

# Deploy do codigo (usa os arquivos ja sincronizados pelo bundle)
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

### 10a. Permissoes UC (via job)

```bash
# Obter job_id do grant_permissions
databricks jobs list --profile MEU_PROFILE | grep grant

# Rodar com o SP da app
databricks jobs run-now --json '{
  "job_id": JOB_ID,
  "notebook_params": {
    "sp_client_id": "SP_CLIENT_ID",
    "catalog": "SEU_CATALOGO",
    "schema": "SEU_SCHEMA"
  }
}' --profile MEU_PROFILE
```

### 10b. Permissoes adicionais (via CLI)

```bash
# Endpoint — obter ID primeiro
EP_ID=$(databricks serving-endpoints get extrator-financeiro --profile MEU_PROFILE \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

databricks serving-endpoints update-permissions "$EP_ID" \
  --json '{"access_control_list":[{"service_principal_name":"SP_CLIENT_ID","permission_level":"CAN_QUERY"}]}' \
  --profile MEU_PROFILE

# Secret scope
databricks secrets put-acl ocr-financeiro "SP_CLIENT_ID" READ --profile MEU_PROFILE

# Warehouse
databricks api patch /api/2.0/permissions/sql/warehouses/SEU_WAREHOUSE_ID \
  --json '{"access_control_list":[{"service_principal_name":"SP_CLIENT_ID","permission_level":"CAN_USE"}]}' \
  --profile MEU_PROFILE
```

## 11. Ingerir texto e extrair dados

Os PDFs estao no volume mas precisam ter o texto extraido e inserido na tabela `documentos`.

### Opcao A: Via SQL (recomendado para poucos PDFs)

Executar no SQL Editor ou via API (requer Serverless SQL Warehouse):

```sql
INSERT INTO SEU_CATALOGO.SEU_SCHEMA.documentos (document_name, document_text, ingested_at)
SELECT
    'meu_balanco.pdf',
    concat_ws('\n\n',
        transform(
            try_cast(ai_parse_document(content):document:elements AS ARRAY<VARIANT>),
            element -> try_cast(element:content AS STRING)
        )
    ),
    current_timestamp()
FROM read_files('/Volumes/SEU_CATALOGO/SEU_SCHEMA/documentos_pdf/meu_balanco.pdf', format => 'binaryFile')
```

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
[ ] databricks.yml configurado (catalog, schema, warehouse IDs, node_type_id, target/profile)
[ ] app.yaml configurado (warehouse IDs, catalog, schema)
[ ] Frontend buildado (frontend/dist/ existe)
[ ] Secret scope criado com PAT valido (testado com curl no FMAPI)
[ ] Bundle deployado (databricks bundle deploy)
[ ] Infraestrutura criada (setup_infrastructure)
[ ] Modelo registrado (register_model)
[ ] Serving endpoint READY (com OCR_PAT + DATABRICKS_HOST)
[ ] PDFs no volume
[ ] App criada, deployada, e RUNNING
[ ] Permissoes do SP da App concedidas (UC, endpoint, secrets, warehouse)
[ ] Texto extraido e LLM rodou com sucesso
[ ] App acessivel e mostrando dados
```

## Troubleshooting

### Endpoint retorna 403 "Invalid access token"
O modelo nao consegue chamar a Foundation Model API. Verificar:
1. O PAT no secret scope esta valido? Testar com `curl` (passo 3).
2. O endpoint usa `OCR_PAT` (nao `DATABRICKS_TOKEN`)? A plataforma injeta `DATABRICKS_TOKEN`
   com a credencial do endpoint, que nao tem acesso FMAPI. Usar `OCR_PAT` evita colisao.
3. O endpoint tem `DATABRICKS_HOST` definido? Sem ele, o modelo nao sabe o host para FMAPI.
4. Se atualizou o secret, force um restart: atualize a versao do modelo no endpoint config.

### Endpoint retorna "Connection error"
O modelo nao consegue resolver o host do workspace. Adicionar `DATABRICKS_HOST` nas
env vars do endpoint com a URL completa (ex: `https://adb-XXX.Y.azuredatabricks.net`
ou `https://workspace.cloud.databricks.com`).

### `ai_parse_document` falha no job `reprocess_all`
Esta funcao so roda em Serverless SQL Warehouse, nao em clusters comuns.
Use a Opcao A (SQL direto) ou a app para ingestao de texto.

### App retorna lista vazia de documentos
1. Verificar que existem linhas em `resultados`: `SELECT COUNT(*) FROM SEU_CATALOGO.SEU_SCHEMA.resultados`
2. Verificar que o SP da app tem SELECT no schema: re-rodar o grant_permissions job.
3. Verificar que o warehouse esta ativo e o SP tem CAN_USE.

### `databricks apps create` falha repetidamente
Pode haver conflito de nome com app deletada recentemente. Tente um nome diferente
ou aguarde ~5min apos a delecao.

### Modelo nao registra via `log_new_version.py` local (erro S3/Azure AccessDenied)
Workspaces com restricoes de rede nao permitem upload direto de artifacts da maquina local.
Use o job `register_model` do bundle (roda no cluster dentro do workspace).

### Job `grant_permissions` ignora o `sp_client_id`
O job usa `base_parameters` do `databricks.yml` (vazio por padrao). Para injetar o SP,
use `notebook_params` no `run-now`:
```bash
databricks jobs run-now --json '{"job_id": ID, "notebook_params": {"sp_client_id": "SP_ID", ...}}'
```

### Azure vs AWS: node_type_id errado
O `databricks.yml` usa a variavel `node_type_id`. Default: `Standard_DS3_v2` (Azure).
Para AWS, override no target: `variables: { node_type_id: "i3.xlarge" }`.
