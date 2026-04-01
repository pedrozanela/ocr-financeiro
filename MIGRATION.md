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

**Tempo total estimado: ~30min** (a maior parte e espera por provisionamento).

```
1. Clonar repo via Git Folders
2. Configurar variaveis (databricks.yml, app.yaml)
3. Deploy do bundle (cria jobs + app)
4. Iniciar a app e copiar o SP
5. Setup infraestrutura + permissoes        (~5min)
6. Registrar modelo + criar endpoint        (~15min)
7. Upload de PDFs e verificar               (via app)
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

## 4. Iniciar a App e copiar o SP

O bundle cria a app mas nao a inicia automaticamente.

1. No sidebar, ir em **Apps**
2. Encontrar **ocr-financeiro**
3. Copiar o **Service Principal Client ID** do painel de detalhes
   (gerado automaticamente pelo bundle — sera usado no proximo passo)
4. Clicar em **Start** (ou **Create a deployment** se for a primeira vez)
   - **Source code path**: `/Workspace/Users/seu_email/.bundle/ocr-financeiro/files`
5. Aguardar status **Running** (~2-3min)

## 5. Setup da infraestrutura + permissoes

Este job cria o schema, tabelas e volume, e concede permissoes ao SP da app.

1. No sidebar, ir em **Workflows** (ou **Jobs & Pipelines**)
2. Encontrar **ocr-financeiro-setup-infrastructure**
3. Clicar **Run now with different parameters**
4. Preencher:
   - `sp_client_id`: colar o SP Client ID copiado no passo 4
   - `catalog` e `schema` ja vem preenchidos pelo bundle
5. Clicar **Run now**

O job cria a infraestrutura E concede permissoes (UC + jobs) em uma unica execucao.

Tempo: ~5-7min.

Apos o job concluir, conceder 2 permissoes adicionais pela UI:

**Warehouse:**

1. Em **SQL Warehouses** → selecionar o warehouse → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP (ex: `app-XXXX ocr-financeiro`)
3. Selecionar **Can use** → **Add**

> **Nota**: A permissao no serving endpoint sera concedida depois do passo 6
> (o endpoint ainda nao existe neste momento).

## 6. Registrar modelo + criar endpoint

1. Em **Workflows**, encontrar **ocr-financeiro-register-model**
2. Clicar **Run now**

Este job faz tudo automaticamente:
- Registra o modelo `extrator_financeiro` no Unity Catalog
- Cria o serving endpoint `extrator-financeiro` (ou atualiza se ja existir)
- Aguarda o endpoint ficar READY

Tempo: ~15min (inclui provisionamento do endpoint).

Apos o job concluir, conceder permissao no endpoint ao SP da app:

1. Em **Serving** → `extrator-financeiro` → aba **Permissions**
2. Clicar **Grant access** → buscar pelo nome do SP (ex: `app-XXXX ocr-financeiro`)
3. Selecionar **Can query** → **Add**

## 7. Upload de PDFs e verificar

A app ja esta rodando (passo 4) e com todas as permissoes configuradas.

1. Abrir a URL da app (visivel em **Apps** → **ocr-financeiro**)
2. Usar o botao **Upload** para enviar PDFs
   - A app salva o PDF no volume e dispara o job de processamento automaticamente
   - O processamento leva alguns minutos por PDF
3. Apos o processamento, o documento aparece na lista lateral
4. Navegar pelas abas: Identificacao, Ativo, Passivo, DRE, Fontes, Pontos de Atencao

> **Alternativa manual**: Tambem e possivel fazer upload via **Catalog** → Volume → Upload,
> e depois rodar o job **ocr-financeiro-batch-job** manualmente em **Workflows**.

## Checklist

```
[ ] Repo clonado via Git Folders
[ ] databricks.yml: target configurado (catalog, warehouse IDs)
[ ] app.yaml: env vars atualizadas
[ ] Bundle deployado via UI
[ ] App iniciada e RUNNING
[ ] SP da app copiado
[ ] Job setup_infrastructure rodou com SP (infraestrutura + permissoes UC + jobs)
[ ] Warehouse: SP tem Can use
[ ] Job register_model: sucesso (modelo + endpoint READY)
[ ] Endpoint: SP tem Can query
[ ] PDF uploadado via app e processado com sucesso
[ ] App mostrando dados extraidos
```

## Troubleshooting

### Endpoint retorna 403 "Invalid access token"
O modelo usa `WorkspaceClient()` que obtem a credencial da plataforma automaticamente.
Se retorna 403, a identidade do endpoint nao tem acesso a Foundation Model API.
Verificar que o endpoint FMAPI (`databricks-claude-sonnet-4-6`) permite acesso
de todos os usuarios ou que o sistema de serving tem permissao.

### Upload nao dispara o job de processamento
Verificar que o SP da app tem CAN_MANAGE_RUN nos jobs. Re-rodar o job
`setup_infrastructure` com o `sp_client_id` preenchido.

### App retorna lista vazia de documentos
1. Verificar que `resultados` tem dados: `SELECT COUNT(*) FROM SEU_CATALOGO.ocr_financeiro.resultados`
2. Re-rodar o job setup_infrastructure com o SP da app.
3. Verificar que o warehouse esta ativo e o SP tem Can use.

### App creation falha ao deployar o bundle
Conflito de nome com app deletada recentemente. Editar o `name` em
`databricks.yml` → `resources.apps.ocr_financeiro_app.name`.

### Nao vejo o icone de Deployments no databricks.yml
Abrir um terminal Web no workspace e rodar `databricks bundle deploy`.
