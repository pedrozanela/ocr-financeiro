# Guia de Deploy — OCR Financeiro

Deploy do projeto em um novo workspace Databricks pela UI, sem necessidade de CLI local.

## Pre-requisitos

- Acesso de admin ao workspace Databricks destino
- SQL Warehouse disponivel (Pro ou Serverless)
- Foundation Model API habilitada (Claude Sonnet 4.6)
- Conta no GitHub com acesso ao repositorio

## Visao Geral

Todo o deploy e feito pelo browser, usando Git Folders para clonar o codigo,
DABs para criar jobs, e a UI para configurar o endpoint e a app.

**Tempo total estimado: ~40min** (a maior parte e espera por provisionamento).

```
1. Clonar o repo via Git Folders
2. Configurar variaveis (databricks.yml, app.yaml)
3. Criar secret scope e PAT (via notebook)
4. Deploy do bundle (via UI)
5. Rodar job: setup_infrastructure                (~5min)
6. Rodar job: register_model                      (~5min)
7. Criar serving endpoint (via UI Serving)        (~12min)
8. Upload de PDFs (via UI Catalog)
9. Criar e deployar a app (via UI Apps)           (~3min)
10. Conceder permissoes (via job + UI)
11. Ingerir texto + extrair (SQL Editor + job)    (~5min)
12. Verificar (abrir a app)
```

## 1. Clonar o repositorio via Git Folders

1. No sidebar, clicar em **Workspace**
2. Navegar ate sua pasta de usuario (`/Users/seu_email`)
3. Clicar no menu **⋮** → **Create** → **Git folder**
4. Colar a URL do repositorio: `https://github.com/pedrozanela/ocr-financeiro.git`
5. Selecionar branch `main` e clicar **Create Git folder**

O repositorio aparece como uma pasta dentro do seu workspace.

## 2. Configurar variaveis

Abrir os arquivos diretamente no editor do workspace e editar:

### 2a. `databricks.yml`

No bloco `variables`, preencher ou adicionar um target para seu ambiente:

```yaml
variables:
  catalog:
    description: "Unity Catalog catalog name"
  schema:
    default: ocr_financeiro
  warehouse_id:
    description: "SQL Warehouse ID"
  # ... demais variaveis

targets:
  meu_target:
    default: true
    workspace:
      profile: DEFAULT          # usar DEFAULT para deploy do proprio workspace
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

Atualizar as env vars — estas sao injetadas na Databricks App em runtime:

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

### 2c. Commit das alteracoes

No Git Folder, clicar no icone Git (branch) no topo → **Commit & Push** com as alteracoes.
(Ou usar um branch separado se preferir.)

## 3. Criar secret scope e PAT

O secret scope armazena o PAT usado pelo serving endpoint para chamar a Foundation Model API.

### 3a. Criar o scope pela UI

Acessar diretamente no browser:

```
https://SEU-WORKSPACE#secrets/createScope
```

Substituir `SEU-WORKSPACE` pela URL do seu workspace (ex: `https://adb-123.4.azuredatabricks.net`).

Preencher:
- **Scope Name**: `ocr-financeiro`
- **Manage Principal**: `All Users` (ou restringir conforme necessario)

Clicar **Create**.

### 3b. Gerar PAT e armazenar no scope (via notebook)

A UI do Databricks nao permite armazenar secrets diretamente. Use um notebook temporario:

1. No workspace, criar um notebook novo (qualquer nome, ex: `_setup_secrets`)
2. Selecionar Python como linguagem
3. Colar e executar celula por celula:

**Celula 1 — Gerar PAT:**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

token = w.tokens.create(comment="ocr-financeiro", lifetime_seconds=7_776_000)
print(f"Token gerado: {token.token_info.token_id}")
print(f"COPIE ESTE VALOR (nao sera mostrado novamente):")
print(token.token_value)
```

**Celula 2 — Armazenar no scope** (colar o token da celula anterior):
```python
import requests

host = spark.conf.get("spark.databricks.workspaceUrl")
headers = {"Authorization": f"Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)}"}

resp = requests.post(
    f"https://{host}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "ocr-financeiro", "key": "pat-servico", "string_value": "COLAR_TOKEN_AQUI"}
)
print("OK" if resp.ok else f"Erro: {resp.text}")
```

**Celula 3 — Validar FMAPI** (substituir o token):
```python
resp = requests.post(
    f"https://{host}/serving-endpoints/databricks-claude-sonnet-4-6/invocations",
    headers={"Authorization": "Bearer COLAR_TOKEN_AQUI", "Content-Type": "application/json"},
    json={"messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
)
print("FMAPI OK" if resp.ok else f"Erro {resp.status_code}: {resp.text[:200]}")
```

> Apos confirmar que tudo funciona, pode deletar o notebook `_setup_secrets`.

### 3c. Conceder leitura do scope ao SP da App (feito no passo 10)

## 4. Deploy do bundle

1. No workspace, abrir o arquivo `databricks.yml` dentro do Git Folder
2. Clicar no icone de **Deployments** (foguete) no painel lateral direito
3. Selecionar o target configurado (ex: `meu_target`)
4. Clicar **Validate and deploy**

Isso sincroniza os arquivos e cria 7 jobs automaticamente.

> **Alternativa**: Se o icone de Deployments nao aparecer (workspaces mais antigos),
> abrir um terminal no workspace e rodar:
> ```bash
> cd /Workspace/Users/seu_email/ocr-financeiro
> databricks bundle deploy --target meu_target
> ```

## 5. Rodar job: Setup da infraestrutura

1. No sidebar, ir em **Workflows** (ou **Jobs & Pipelines**)
2. Encontrar o job **ocr-financeiro-setup-infrastructure**
3. Clicar **Run now**

Cria: schema, 4 tabelas e volume. Tempo: ~5-7min.

## 6. Rodar job: Registrar modelo

1. Em **Workflows**, encontrar **ocr-financeiro-register-model**
2. Clicar **Run now**

Registra o modelo `extrator_financeiro` v1 no Unity Catalog. Tempo: ~5-7min.

## 7. Criar serving endpoint

1. No sidebar, ir em **Serving**
2. Clicar **Create serving endpoint**
3. Preencher:
   - **Name**: `extrator-financeiro`
   - **Served entities**: clicar **Select entity**
     - Entity source: **Unity Catalog**
     - Selecionar o modelo: `SEU_CATALOGO.ocr_financeiro.extrator_financeiro`
     - Version: `1`
   - **Compute**: Size **Small**, marcar **Scale to zero**
4. Em **Advanced configuration** → **Environment variables**, adicionar:

   | Variavel | Valor |
   |----------|-------|
   | `OCR_PAT` | `{{secrets/ocr-financeiro/pat-servico}}` |
   | `DATABRICKS_HOST` | `https://SEU-WORKSPACE` (URL completa do workspace) |

5. Clicar **Create**

Aguardar ate o status ficar **Ready** (~10-15min).

> **CRITICO — `OCR_PAT` e `DATABRICKS_HOST`**:
> - Use `OCR_PAT` (nao `DATABRICKS_TOKEN`) para o PAT. A plataforma injeta
>   `DATABRICKS_TOKEN` automaticamente com uma credencial que nao tem acesso FMAPI.
> - `DATABRICKS_HOST` e obrigatorio — sem ele, o modelo nao sabe para onde enviar
>   as chamadas FMAPI. Use a URL completa com `https://`.

## 8. Upload de PDFs

### Opcao A: Via UI do Catalog

1. No sidebar, ir em **Catalog**
2. Navegar ate `SEU_CATALOGO` → `ocr_financeiro` → **Volumes** → `documentos_pdf`
3. Clicar **Upload to this volume**
4. Arrastar os PDFs ou clicar para selecionar

### Opcao B: Via notebook

```python
# Em um notebook, fazer upload programatico
dbutils.fs.cp("file:/path/local/meu_balanco.pdf",
              "/Volumes/SEU_CATALOGO/ocr_financeiro/documentos_pdf/meu_balanco.pdf")
```

## 9. Criar e deployar a Databricks App

1. No sidebar, ir em **Apps** (ou **New** → **App**)
2. Clicar **Create a custom app**
3. Preencher:
   - **Name**: `ocr-financeiro` (ou nome de sua preferencia)
   - **Description**: `Extrator de dados financeiros`
4. Clicar **Create**
5. Aguardar o status mudar para **Active** (~2-3min)
6. Clicar em **Create a deployment**
   - **Source code path**: navegar ate `/Workspace/Users/seu_email/.bundle/ocr-financeiro/files`
   - (este e o path onde o bundle sincronizou os arquivos no passo 4)
7. Clicar **Deploy**

Aguardar deploy concluir. A URL da app aparece na pagina da app.

> **Gotcha**: Se o nome da app conflitar com uma deletada recentemente,
> tente um nome diferente ou aguarde ~5min.

## 10. Conceder permissoes ao SP da App

Ao criar a app, o Databricks cria um Service Principal (SP) automatico para ela.

### 10a. Encontrar o SP da app

1. Em **Apps**, clicar no nome da app
2. No painel de detalhes, copiar o **Service Principal Client ID**

### 10b. Permissoes UC (via job)

1. Em **Workflows**, encontrar **ocr-financeiro-grant-permissions**
2. Clicar **Run now with different parameters**
3. Preencher os parametros:
   - `sp_client_id`: colar o SP Client ID copiado
   - `catalog`: `SEU_CATALOGO`
   - `schema`: `ocr_financeiro`
4. Clicar **Run now**

### 10c. Permissoes adicionais (via UI)

**Serving endpoint:**

1. Em **Serving** → `extrator-financeiro` → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP da app (ex: `app-XXXX ocr-financeiro`)
3. Selecionar **Can query** → **Add**

**Warehouse:**

1. Em **SQL Warehouses** → selecionar o warehouse → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP da app
3. Selecionar **Can use** → **Add**

**Secret scope** (via notebook — nao tem UI):

```python
import requests
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

resp = requests.post(
    f"https://{host}/api/2.0/secrets/acls/put",
    headers={"Authorization": f"Bearer {token}"},
    json={"scope": "ocr-financeiro", "principal": "SP_CLIENT_ID", "permission": "READ"}
)
print("OK" if resp.ok else f"Erro: {resp.text}")
```

## 11. Ingerir texto e extrair dados

Os PDFs estao no volume mas precisam ser processados em duas etapas:
extrair texto (ai_parse_document) e depois chamar o modelo (endpoint).

### 11a. Extrair texto — SQL Editor

1. No sidebar, ir em **SQL Editor**
2. Selecionar o Serverless Warehouse
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

> Repetir para cada PDF, ou adaptar o SELECT para processar todos de uma vez.

### 11b. Rodar extracao LLM — via job

1. Em **Workflows**, encontrar **ocr-financeiro-run-llm**
2. Clicar **Run now**

Tempo: ~3-5min por documento.

### 11c. Alternativa: via app

Se a app ja esta rodando (passo 9), basta abrir a URL e usar o botao **Upload** na interface.
A app faz tudo automaticamente (ai_parse_document + endpoint OCR + salva resultados).

## 12. Verificar

1. Abrir a URL da app (visivel em **Apps** → nome da app)
2. Verificar que o documento aparece na lista lateral
3. Navegar pelas abas: Identificacao, Ativo, Passivo, DRE, Fontes, Pontos de Atencao
4. Testar correcao manual de um campo
5. Exportar Excel e validar formato

## Checklist

```
[ ] Repo clonado via Git Folders
[ ] databricks.yml configurado (catalog, schema, warehouse IDs, node_type_id, target)
[ ] app.yaml configurado (warehouse IDs, catalog, schema)
[ ] Secret scope criado (ocr-financeiro) com PAT valido
[ ] PAT validado contra FMAPI (retorna JSON com choices)
[ ] Bundle deployado via UI (Deployments → target → Deploy)
[ ] Job setup_infrastructure rodou com sucesso
[ ] Job register_model rodou com sucesso
[ ] Serving endpoint READY (com OCR_PAT + DATABRICKS_HOST nas env vars)
[ ] PDFs no volume (upload via Catalog ou app)
[ ] App criada, deployada, e RUNNING
[ ] Permissoes do SP da App concedidas:
    [ ] UC: grant_permissions job rodou com SP
    [ ] Endpoint: Can query
    [ ] Warehouse: Can use
    [ ] Secret scope: READ (via notebook)
[ ] Texto extraido (SQL Editor) e LLM rodou com sucesso (run-llm job)
[ ] App acessivel e mostrando dados extraidos
```

## Troubleshooting

### Endpoint retorna 403 "Invalid access token"
O modelo nao consegue chamar a Foundation Model API. Verificar:
1. O PAT no secret scope esta valido? Re-executar a celula de validacao FMAPI (passo 3b).
2. O endpoint usa `OCR_PAT` (nao `DATABRICKS_TOKEN`) nas env vars?
3. O endpoint tem `DATABRICKS_HOST` definido nas env vars?
4. Se atualizou o secret, force um restart: edite o endpoint e mude a versao do modelo.

### Endpoint retorna "Connection error"
`DATABRICKS_HOST` nao esta configurado nas env vars do endpoint. Adicionar com a URL
completa do workspace (ex: `https://adb-XXX.Y.azuredatabricks.net`).

### `ai_parse_document` falha no SQL Editor
Esta funcao requer Serverless SQL Warehouse. Verificar que o warehouse selecionado
e do tipo Serverless (nao Classic/Pro).

### App retorna lista vazia de documentos
1. Verificar que `resultados` tem dados: `SELECT COUNT(*) FROM SEU_CATALOGO.ocr_financeiro.resultados`
2. Re-rodar o job grant_permissions com o SP da app.
3. Verificar que o warehouse esta ativo e o SP tem Can use.

### App creation falha repetidamente
Conflito de nome com app deletada recentemente. Usar um nome diferente
ou aguardar ~5min apos a delecao.

### Azure vs AWS: cluster falha ao iniciar
Verificar o `node_type_id` no target do `databricks.yml`:
- **AWS**: `i3.xlarge`
- **Azure**: `Standard_DS3_v2`

### Nao consigo ver o icone de Deployments no databricks.yml
A funcionalidade de DABs na UI pode nao estar habilitada no workspace.
Alternativa: abrir um terminal Web no workspace e rodar:
```bash
cd /Workspace/Users/seu_email/ocr-financeiro
databricks bundle deploy
```
