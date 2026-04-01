# Guia de Deploy — OCR Financeiro

Deploy do projeto em um novo workspace Databricks pela UI, sem necessidade de CLI local.

## Pre-requisitos

- Acesso de admin ao workspace Databricks destino
- SQL Warehouse disponivel (Pro ou Serverless)
- Foundation Model API habilitada (Claude Sonnet 4.6)
- Conta no GitHub com acesso de leitura ao repositorio

## Visao Geral

Todo o deploy e feito pelo browser. O bundle cria automaticamente os jobs e a app.
O notebook `register_model` registra o modelo e cria o serving endpoint.

Nao e necessario criar PATs, secret scopes, ou Service Principals manualmente.
A app e o endpoint usam suas identidades nativas (auto-gerenciadas pela plataforma).

**Tempo total estimado: ~35min** (a maior parte e espera por provisionamento).

```
1. Clonar repo via Git Folders
2. Configurar variaveis (databricks.yml, app.yaml)
3. Deploy do bundle (via UI)
4. Iniciar a app
5. Rodar job: setup_infrastructure          (~5min)
6. Rodar job: register_model                (~15min — modelo + endpoint)
7. Upload de PDFs (via UI Catalog)
8. Conceder permissoes (via job + UI)
9. Ingerir texto + extrair (SQL Editor + job)
10. Verificar (abrir a app)
```

## 1. Clonar o repositorio via Git Folders

1. No sidebar, clicar em **Workspace**
2. Navegar ate sua pasta de usuario (`/Users/seu_email`)
3. Clicar no menu **⋮** → **Create** → **Git folder**
4. Colar a URL do repositorio: `https://github.com/pedrozanela/ocr-financeiro.git`
5. Selecionar branch `main` e clicar **Create Git folder**

## 2. Configurar variaveis

Abrir os arquivos no editor do workspace e editar.

### 2a. `databricks.yml`

Adicionar um target para o ambiente no bloco `targets`:

```yaml
targets:
  meu_target:
    default: true
    workspace:
      profile: DEFAULT
    variables:
      catalog: SEU_CATALOGO
      warehouse_id: "SEU_WAREHOUSE_ID"
      serverless_warehouse_id: "SEU_WAREHOUSE_ID"
      node_type_id: "i3.xlarge"             # AWS
      # node_type_id: "Standard_DS3_v2"     # Azure
```

> **Dica**: Para encontrar o Warehouse ID, va em **SQL Warehouses** no sidebar,
> clique no warehouse, e copie o ID da URL ou do painel de detalhes.

### 2b. `app.yaml`

Atualizar as env vars da Databricks App:

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
```

## 3. Deploy do bundle

1. No workspace, abrir o arquivo `databricks.yml` dentro do Git Folder
2. Clicar no icone de **Deployments** (foguete) no painel lateral direito
3. Selecionar o target configurado (ex: `meu_target`)
4. Clicar **Validate and deploy**

Isso sincroniza os arquivos, cria 7 jobs e a app automaticamente.

> **Se o icone de Deployments nao aparecer**, abrir um terminal Web no workspace e rodar:
> ```
> cd /Workspace/Users/seu_email/ocr-financeiro
> databricks bundle deploy --target meu_target
> ```

## 4. Iniciar a App

O bundle cria a app mas nao a inicia automaticamente.

1. No sidebar, ir em **Apps**
2. Encontrar **ocr-financeiro**
3. Clicar em **Start** (ou **Create a deployment** se for a primeira vez)
   - **Source code path**: `/Workspace/Users/seu_email/.bundle/ocr-financeiro/files`
4. Aguardar status **Running** (~2-3min)

## 5. Rodar job: Setup da infraestrutura

1. No sidebar, ir em **Workflows** (ou **Jobs & Pipelines**)
2. Encontrar **ocr-financeiro-setup-infrastructure**
3. Clicar **Run now**

Cria: schema, 4 tabelas e volume. Tempo: ~5-7min.

## 6. Rodar job: Registrar modelo + criar endpoint

1. Em **Workflows**, encontrar **ocr-financeiro-register-model**
2. Clicar **Run now**

Este job faz tudo automaticamente:
- Registra o modelo `extrator_financeiro` no Unity Catalog
- Cria o serving endpoint `extrator-financeiro` (ou atualiza se ja existir)
- Aguarda o endpoint ficar READY

Tempo: ~15min (inclui provisionamento do endpoint).

> **Sobre autenticacao**: O endpoint usa a identidade do proprio sistema (auto-gerenciada
> pela plataforma). Nao precisa de PAT ou secret scope. O modelo usa `WorkspaceClient()`
> que resolve as credenciais automaticamente.

## 7. Upload de PDFs

1. No sidebar, ir em **Catalog**
2. Navegar ate `SEU_CATALOGO` → `ocr_financeiro` → **Volumes** → `documentos_pdf`
3. Clicar **Upload to this volume**
4. Arrastar os PDFs ou clicar para selecionar

## 8. Conceder permissoes ao SP da App

Ao criar a app, o Databricks cria um Service Principal (SP) automatico.

### 8a. Encontrar o SP da app

1. Em **Apps**, clicar no nome da app (**ocr-financeiro**)
2. No painel de detalhes, copiar o **Service Principal Client ID**

### 8b. Permissoes UC (via job)

1. Em **Workflows**, encontrar **ocr-financeiro-grant-permissions**
2. Clicar **Run now with different parameters**
3. Preencher os parametros:
   - `sp_client_id`: colar o SP Client ID
   - `catalog`: `SEU_CATALOGO`
   - `schema`: `ocr_financeiro`
4. Clicar **Run now**

### 8c. Permissoes adicionais (via UI)

**Serving endpoint:**

1. Em **Serving** → `extrator-financeiro` → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP (ex: `app-XXXX ocr-financeiro`)
3. Selecionar **Can query** → **Add**

**Warehouse:**

1. Em **SQL Warehouses** → selecionar o warehouse → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP da app
3. Selecionar **Can use** → **Add**

## 9. Ingerir texto e extrair dados

### 9a. Extrair texto — SQL Editor

1. No sidebar, ir em **SQL Editor**
2. Selecionar o Serverless SQL Warehouse
3. Executar (substituir nomes):

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
FROM read_files(
    '/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/meu_balanco.pdf',
    format => 'binaryFile'
)
```

### 9b. Rodar extracao LLM — via job

1. Em **Workflows**, encontrar **ocr-financeiro-run-llm**
2. Clicar **Run now**

### 9c. Alternativa: via app

Se a app ja esta rodando, basta abrir a URL e usar o botao **Upload**.
A app faz tudo automaticamente (ai_parse_document + endpoint OCR + salva resultados).

## 10. Verificar

1. Abrir a URL da app (visivel em **Apps** → **ocr-financeiro**)
2. Verificar que os documentos aparecem na lista lateral
3. Navegar pelas abas: Identificacao, Ativo, Passivo, DRE, Fontes, Pontos de Atencao
4. Testar correcao manual de um campo
5. Exportar Excel e validar formato

## Checklist

```
[ ] Repo clonado via Git Folders
[ ] databricks.yml: target configurado (catalog, warehouse IDs, node_type_id)
[ ] app.yaml: env vars atualizadas
[ ] Bundle deployado via UI
[ ] App iniciada e RUNNING
[ ] Job setup_infrastructure: sucesso
[ ] Job register_model: sucesso (modelo + endpoint READY)
[ ] PDFs no volume
[ ] Permissoes do SP da App:
    [ ] UC: grant_permissions job
    [ ] Endpoint: Can query
    [ ] Warehouse: Can use
[ ] Texto extraido e LLM rodou com sucesso
[ ] App mostrando dados extraidos
```

## Troubleshooting

### Endpoint retorna 403 "Invalid access token"
O modelo usa `WorkspaceClient()` que obtem a credencial da plataforma automaticamente.
Se retorna 403, a identidade do endpoint nao tem acesso a Foundation Model API.
Verificar que o endpoint FMAPI (`databricks-claude-sonnet-4-6`) permite acesso
de todos os usuarios ou que o sistema de serving tem permissao.

### `ai_parse_document` falha no SQL Editor
Requer Serverless SQL Warehouse. Verificar que o warehouse selecionado e Serverless.

### App retorna lista vazia de documentos
1. Verificar que `resultados` tem dados: `SELECT COUNT(*) FROM SEU_CATALOGO.ocr_financeiro.resultados`
2. Re-rodar o job grant_permissions com o SP da app.
3. Verificar que o warehouse esta ativo e o SP tem Can use.

### App creation falha ao deployar o bundle
Conflito de nome com app deletada recentemente. Editar o `name` em
`databricks.yml` → `resources.apps.ocr_financeiro_app.name`.

### Azure vs AWS: cluster falha ao iniciar
Verificar o `node_type_id` no target: AWS = `i3.xlarge`, Azure = `Standard_DS3_v2`.

### Nao vejo o icone de Deployments no databricks.yml
Abrir um terminal Web no workspace e rodar `databricks bundle deploy`.
