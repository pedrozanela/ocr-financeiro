# Deploy em novo ambiente (cliente)

Runbook completo para subir o TechFin OCR Financeiro num workspace Databricks novo. Idempotente: pode rodar em ambiente greenfield ou em update sobre um ambiente que já tinha a versão anterior (com `correcoes` no formato antigo).

## Pré-requisitos

- Workspace Databricks com Unity Catalog + Lakebase habilitados
- Profile Databricks CLI configurado com permissões de admin (criar catalog/scope/secrets/apps)
- Credenciais OAuth da Techfin (PARC) em mãos: `client_id`, `client_secret`, `oauth_user`, `oauth_password`
- Foundation Model API habilitada (Claude via endpoint serverless)

## Passo a passo

### 1. Clonar repo e configurar profile

```bash
git clone git@github.com:pedro-zanela_data/ocr-financeiro.git
cd ocr-financeiro
export DATABRICKS_PROFILE=<profile-do-cliente>
```

Editar `databricks.yml` para o **target do cliente**:

```yaml
targets:
  cliente_xyz:
    workspace:
      host: https://<cliente>.cloud.databricks.com
    variables:
      catalog: <catalog-do-cliente>
      schema: ocr_financeiro
      lakebase_host: <ep-xxxx.database.us-east-1.cloud.databricks.com>
      lakebase_db:   ocr_financeiro
```

### 2. Criar secret scope `techfin` + ACL

```bash
SP=<service_principal_client_id_da_app>  # depois do deploy do bundle, sair de `databricks apps get`

databricks secrets create-scope techfin --profile $DATABRICKS_PROFILE
echo -n "<parc_client_id>"     | databricks secrets put-secret techfin parc_client_id     --profile $DATABRICKS_PROFILE
echo -n "<parc_client_secret>" | databricks secrets put-secret techfin parc_client_secret --profile $DATABRICKS_PROFILE
echo -n "<oauth_user>"         | databricks secrets put-secret techfin parc_oauth_user    --profile $DATABRICKS_PROFILE
echo -n '<oauth_password>'     | databricks secrets put-secret techfin parc_oauth_password --profile $DATABRICKS_PROFILE
databricks secrets put-acl techfin "$SP" READ --profile $DATABRICKS_PROFILE
```

> **Importante**: na primeira execução o SP da app ainda não existe. Crie o scope com os 4 secrets, faça o deploy do bundle (passo 7) para criar a app e o SP, depois conceda READ ao SP.

### 3. Criar projeto Lakebase

Se ainda não existe Lakebase no workspace:

```bash
databricks postgres create-project ocr-financeiro --profile $DATABRICKS_PROFILE
databricks postgres create-database ocr_financeiro \
  --branch projects/ocr-financeiro/branches/production \
  --profile $DATABRICKS_PROFILE
```

Anote o `host` do endpoint primário — usado nos próximos passos.

### 4. Rodar `setup_infrastructure.py` (Delta no UC)

Cria schema + tabelas Delta + volume + grants no UC. Idempotente.

```bash
databricks workspace import notebooks/setup_infrastructure.py /path/to/setup_infrastructure --profile $DATABRICKS_PROFILE
# Ou via bundle (notebook é deployado como parte do bundle).

# Executar manualmente no workspace, passando:
#   catalog       = <catalog-do-cliente>
#   schema        = ocr_financeiro
#   sp_client_id  = <SP da app>  (opcional na 1ª vez)
```

Resultado: tabelas Delta criadas, schema OK.

### 5. Rodar `setup_lakebase.py` (Postgres)

Cria tabelas no Lakebase + role do SP da app + grants. Idempotente.

```bash
# Mesmo padrão: importa, abre no workspace, executa com:
#   lakebase_host = <host>
#   lakebase_db   = ocr_financeiro
#   sp_client_id  = <SP da app>
```

Concede inclusive os grants em sequences (necessário para `feedback_llm.id` BIGSERIAL).

### 6. Rodar `migrate_correcoes_to_feedback.py` (migração big-bang)

**Pula se ambiente greenfield**. Necessário se existir uma tabela `correcoes` no formato antigo (cliente que tinha a versão anterior).

```bash
# Executar com:
#   catalog       = <catalog>
#   schema        = ocr_financeiro
#   lakebase_host = <host>
#   lakebase_db   = ocr_financeiro
#   sp_client_id  = <SP>
```

O que faz:
- Renomeia `correcoes` → `correcoes_legado` (Lakebase e Delta)
- Cria `revisoes_em_andamento` + `feedback_llm` se ausentes
- Backfill: `correcoes_legado` → `feedback_llm` (acao='corrigido', modelo_versao='pre-migration')
- Grants de sequence pro SP
- **É seguro re-executar**: usa `NOT EXISTS` e `ON CONFLICT DO NOTHING`

### 7. Deploy do bundle (app + jobs)

```bash
databricks bundle deploy --target cliente_xyz
databricks bundle run ocr_financeiro_app --target cliente_xyz --no-wait
```

A primeira execução cria a app. Anote o `service_principal_client_id` (em `databricks apps get ocr-financeiro -o json`) e volte ao passo 2 para conceder o READ no scope `techfin`.

### 8. Registrar o modelo (modelo MLflow + endpoint)

```bash
databricks jobs run-now <register-model-job-id> --profile $DATABRICKS_PROFILE
```

Aguarde ~5-10 min para o endpoint `extrator-financeiro` atualizar.

### 9. Validações finais

```bash
APP_URL=$(databricks apps get ocr-financeiro --profile $DATABRICKS_PROFILE -o json | jq -r .url)
TOKEN=$(databricks auth token --profile $DATABRICKS_PROFILE | jq -r .access_token)

# Sidebar — deve retornar lista de docs com status
curl -s -H "Authorization: Bearer $TOKEN" "$APP_URL/api/documentos/sidebar-state" | jq '.documentos | length'

# Metrics — deve retornar 3 blocos
curl -s -H "Authorization: Bearer $TOKEN" "$APP_URL/api/metrics" | jq '.validacoes.total, .acuracia.cobertura_pct, .atividade.docs_revisados'

# Preview Techfin — confirma que mapper funciona
curl -s -H "Authorization: Bearer $TOKEN" "$APP_URL/api/finalize/<doc>/preview" | jq '.records[0].payload.cnpj'

# Dry-run submit — confirma fluxo sem chamar Techfin
curl -s -X POST -H "Authorization: Bearer $TOKEN" "$APP_URL/api/finalize/<doc>?dry_run=true" | jq '.dry_run, .techfin_results | length'
```

Sanity checks no Lakebase:

```sql
SELECT
  (SELECT COUNT(*) FROM revisoes_em_andamento) AS rev,
  (SELECT COUNT(*) FROM feedback_llm)          AS fb,
  (SELECT COUNT(*) FROM correcoes_legado)      AS leg;
```

Esperado: `rev` baixo (idealmente 0 no início), `fb` = N de linhas migradas + novas, `leg` preservado intacto.

### 10. Liberar pro cliente

- App URL final: `https://ocr-financeiro-XXX.cloud.databricksapps.com`
- Conceder `CAN_VIEW` na app aos usuários do cliente
- Subir 1-3 PDFs de teste pelo upload da UI → confirmar fluxo de revisão → submit dry-run → submit real (quando Techfin destravar URL)

## Troubleshooting

### `permission denied for sequence feedback_llm_id_seq`
SP não tem grant na sequence. Rode no Lakebase:
```sql
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO "<sp-client-id>";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO "<sp-client-id>";
```
Ou re-execute `setup_lakebase.py` com `sp_client_id` preenchido.

### `Techfin retorna 404`
URL do endpoint pode estar errada. Atualmente apontamos para `https://parc.supplierapi.com.br/databricks/v1/balanco`. Confirme com a Techfin antes de submeter em produção. Use `?dry_run=true` para validar o resto do fluxo sem depender da API.

### `Documento já está finalizado` ao tentar re-submeter
Esperado em re-submissão idempotente — a 409 da Techfin é tratada como sucesso silencioso, mas se o app ainda bloqueia, reabra após próxima edição. Não é mais bloqueante na versão atual.

### Sidebar não carrega / 500
Verifique o log: `databricks apps logs ocr-financeiro --profile $DATABRICKS_PROFILE | tail -50`. Causas comuns:
- `relation "correcoes_legado" does not exist` → falta rodar migration notebook
- `permission denied` → grants do SP no Lakebase
- `Techfin secrets missing` → scope `techfin` sem ACL para o SP

### Diff entre `feedback_llm` e `revisoes_em_andamento`
- `revisoes_em_andamento`: estado **transitório**, limpo no Submeter
- `feedback_llm`: append-only, **snapshot** no Submeter
- Doc em revisão ativa = entradas em `revisoes_em_andamento`
- Doc submetido = entradas em `feedback_llm`, sem entradas em `revisoes_em_andamento`

## Rollback de emergência

Se a migração der ruim:

```sql
-- Lakebase
DROP TABLE revisoes_em_andamento;
DROP TABLE feedback_llm;
ALTER TABLE correcoes_legado RENAME TO correcoes;
```

App vai parar de funcionar (espera novas tabelas). Reverte o deploy do bundle para o commit anterior à migração e re-deploya.

## Resumo dos arquivos

| Caminho | O que é |
|---|---|
| `notebooks/setup_infrastructure.py` | Cria schema/tabelas Delta + grants UC |
| `notebooks/setup_lakebase.py` | Cria projeto Lakebase + tabelas Postgres + role do SP + grants |
| `notebooks/migrate_correcoes_to_feedback.py` | Migração big-bang da tabela legada |
| `databricks.yml` | Bundle config (jobs, app, postgres resources) |
| `server/integrations/techfin/` | Cliente OAuth + mapper para API PARC |
| `app.py` + `server/routes/` | FastAPI backend |
| `frontend/src/` | React UI |

## Próximos passos pós-deploy

- Configurar agenda do `batch_job` para processar PDFs novos automaticamente
- Setup do `vision_extraction` para PDFs com imagem
- Treinar usuários do cliente no fluxo de revisão (caminho rápido + classificação de erros)
- Após 2-4 semanas estável: `DROP TABLE correcoes_legado` (backup já não necessário)
