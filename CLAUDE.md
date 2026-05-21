# CLAUDE.md — TechFin OCR Financeiro

## Visão Geral
Sistema de extração automatizada de dados financeiros (Balanço Patrimonial + DRE) de PDFs usando LLM (Claude via Databricks Model Serving). Inclui app de revisão humana com correções manuais.

## Arquitetura

### Stack
- **Backend**: FastAPI (Python) — `app.py` + `server/routes/`
- **Frontend**: React + TypeScript + Tailwind + Vite — `frontend/src/`
- **Modelo**: MLflow PythonModel (`model/agent.py`) registrado em Unity Catalog, servido via Model Serving endpoint `extrator-financeiro`
- **Banco**: Lakebase (PostgreSQL gerenciado) — migrado de Delta Lake para latência baixa
- **Deploy**: Databricks Asset Bundles (DABs) → Databricks Apps
- **PDFs**: armazenados em UC Volume `/Volumes/{catalog}/{schema}/documentos_pdf`

### Workspace
- **FEVM**: `fevm-fevm-pzanela-classic-aws.cloud.databricks.com`
- **Profile**: `fe-vm-fevm-pzanela-classic-aws`
- **Catalog**: `fevm_pzanela_classic_aws_catalog`
- **Schema**: `ocr_financeiro`
- **App URL**: `https://ocr-financeiro-7474649924416232.aws.databricksapps.com`
- **App SP**: `5914cb1c-3156-41a7-b1ff-1aa9a0778977` (ID: 78499696348760)

### Lakebase
- **Projeto**: `ocr-financeiro`
- **Host**: `ep-raspy-credit-d2j0p33d.database.us-east-1.cloud.databricks.com`
- **Database**: `ocr_financeiro`
- **Branch**: `production`, **Endpoint**: `primary`
- **Lakebase Role do SP**: `app-sp` (auth_method: LAKEBASE_OAUTH_V1, identity_type: SERVICE_PRINCIPAL)
- **IMPORTANTE**: O `databricks.yml` DEVE ter `resources` com o postgres branch+database para não apagar o App Resource no deploy:
  ```yaml
  resources:
    - name: "postgres"
      postgres:
        branch: "projects/ocr-financeiro/branches/production"
        database: "projects/ocr-financeiro/branches/production/databases/db-w3xq-63lbn7fhdv"
        permission: "CAN_CONNECT_AND_CREATE"
  ```

### Tabelas (Lakebase PostgreSQL)
- `documentos` — texto OCR extraído (PK: document_name)
- `resultados` — JSON financeiro extraído (LLM bruto) + assessment (PK: document_name, tipo_entidade, periodo)
- `revisoes_em_andamento` — estado transitório da UI durante revisão. Cada linha é (doc, campo, te, per) com `status` ∈ {`llm`, `corrigido`, `confirmado`} + `tipo_erro` (8 categorias). Limpa no Submeter.
- `feedback_llm` — append-only audit log. Snapshot de cada revisão ao Submeter (acao='corrigido'|'confirmado'), com `modelo_versao` + `prompt_versao` + `tipo_erro` + `fonte_llm` (JSONB). Source of truth para retreino/avaliação.
- `resultados_final` — JSON consolidado pós-Submeter (`status='finalizado'`) + `techfin_response` (JSONB com resposta da API ou flag `dry_run`)
- `correcoes_legado` — tabela renomeada da versão antiga `correcoes`. Read-only, mantida como backup pós-migração.
- **Migração**: feita uma vez via `notebooks/migrate_correcoes_to_feedback.py` (idempotente). Detalhes em `DEPLOY.md`.
- As mesmas tabelas existem em Delta (`{catalog}.{schema}.*`) mas o app só escreve no Lakebase. Delta serve para audit/análise via Spark/SQL warehouse.

### Modelo (model/agent.py)
- MLflow PythonModel que chama Claude via Foundation Model API
- **Artefatos**: `depara.json` (176 aliases de contas), `regras_classificacao.json` (23 regras), `output_schema.json`
- **Pós-processamento** (em ordem de execução):
  1. `_postprocess_outros` — recalcula `outros_*` como resíduo (total - soma específicos)
  2. `_postprocess_cascata_dre` — recalcula totais da DRE em cascata:
     - `_TOTAL_TO_PRIMARY` — move valor de total para componente primário quando LLM ignora de-para
     - `_CASCADE_PHASE1` — ROL, LB, LO, LF, LAIR (soma simples, Regra 22)
     - Verificação de sinal IRPJ
     - `_CASCADE_PHASE2` — Lucro antes participações, antes minoritária, LL
  3. `_postprocessed` — metadata retornada no JSON com todos os campos recalculados (campo, original, corrigido, motivo)
- **Versão atual no endpoint**: v15

### Regras Importantes
- **Regra 22** — Sinais do documento: extrair com sinal original do PDF. Fórmulas são soma simples.
- **Regra 23** — Fonte da verdade é o balanço/DRE principal, NÃO notas explicativas. Auto-verificação: se soma > total, identificar dupla contagem.
- **Regra 21** — De-para é fonte da verdade para mapeamento de contas
- **Regra 12** — Cascata DRE (soma simples com sinais)

### Frontend (FieldSection.tsx)
- **Totais**: valor principal = soma calculada dos sub-itens (não extração do LLM)
- **Badge amarelo LLM**: aparece quando valor original do LLM diverge da soma (usa `_postprocessed` para obter valor pré-recalculado)
- **Ícone "i"**: tooltip com texto fonte do PDF (`data.fontes[field.path]`)
- **CASCADE_FORMULAS**: soma simples (sinais já vêm do documento)
- **Cascata propaga**: `computedMap` pré-calculado em 2 fases para que edição manual de sub-item propague para todos os totais em real-time
- **Escala**: input de correção mostra valor em unidade (multiplicado), salva na escala do JSON (dividido)
- **Groups** (Ativo/Passivo): header sem soma, total usa `computedTotal`, root total = soma dos sub-groups

### db.py (Camada de Dados)
- Usa `psycopg2` direto no Lakebase (sem SQL warehouse)
- Converte parâmetros Databricks (`[{"name":"x","value":"y"}]`) para `%(x)s` do psycopg2
- Regex `CAST(... AS STRING)` → `(...)::text`
- Serializa `Decimal`/`datetime` para JSON-safe
- Token via `WorkspaceClient().api_client.do("POST", "/api/2.0/postgres/credentials")` (app) ou CLI (local)
- Token cache: 40min

### Notebooks
- `batch_job.py` — processa PDFs novos via ai_parse_document + endpoint OCR. Dual-write (Delta + Lakebase)
- `vision_extraction.py` — OCR via Claude Vision para PDFs com imagens. Dual-write
- Ambos usam `WorkspaceClient()` para gerar database credential (não CLI — serverless não tem CLI)
- Widget `lakebase_host` controla se escreve no Lakebase

### Deploy
```bash
cd ~/code/techfin
databricks bundle deploy --target fevm_pzanela
databricks bundle run ocr_financeiro_app --target fevm_pzanela --no-wait  # app
databricks jobs run-now <JOB_ID> --profile fe-vm-fevm-pzanela-classic-aws --no-wait  # jobs
```
- **Register model job**: 829535901995319
- **Batch job**: 494297694677265
- **Vision job**: 564671429208244
- Frontend build: `cd frontend && rm -rf dist && npm run build` (clean build necessário para novas classes Tailwind)
- Após deploy do bundle, usar `databricks apps deploy ocr-financeiro --source-code-path ...` para forçar re-read do app.yaml
- Para subir em ambiente novo (cliente), seguir `DEPLOY.md` (runbook completo)

### Integração Techfin (PARC)
- Cliente em `server/integrations/techfin/`: `client.py` (OAuth password grant + cache token) + `mapper.py` (extracted_json → payload Techfin)
- Secrets no scope `techfin`: `parc_client_id`, `parc_client_secret`, `parc_oauth_user`, `parc_oauth_password`
- App SP precisa READ no scope
- Endpoint: `POST /databricks/v1/balanco` (mas a URL atual retorna 404 — confirmar com Techfin)
- `POST /api/finalize/{doc}?dry_run=true` simula submit sem chamar Techfin — payload salvo em `resultados_final.techfin_response.dry_run=true`
- 409 da Techfin tratado como sucesso silencioso (upsert idempotente)
- Re-submeter doc finalizado é permitido (UI: hover em Concluídos → ícone send)

### Sidebar / DocumentList
- `GET /api/documentos/sidebar-state` retorna `{version, documentos}` com status agregado: `nao_revisado` | `em_revisao` | `submetido` | `erro_submissao`
- Frontend faz polling de 15s (60s quando aba oculta); só re-renderiza se `version` mudou
- Status calculado em SQL: tem revisao com status != 'llm' → em_revisao; tem resultados_final.status='finalizado' → submetido
- Sidebar tem 2 abas (Pendentes/Concluídos) com sort + agrupamento temporal (Hoje/Ontem/Esta semana/Mais antigos em America/Sao_Paulo)

### Métricas (3 blocos)
- `GET /api/metrics` (global) ou `/api/metrics/{doc}` (por documento) — estrutura unificada `{validacoes, acuracia, atividade}`
- **Bloco 1**: 23 validações contábeis (Ativo=Passivo, somas, cascata DRE). Roda sobre `extracted_json`. Mede integridade dos PDFs, não acurácia do LLM.
- **Bloco 2**: cobertura (revisado/total_campos) + taxa de confirmação (na amostra revisada). Comparação por `modelo_versao` com Δpp. Top campos com taxa de correção ≥ 5 revisões. Tipos de erro classificados (8 categorias).
- **Bloco 3**: atividade da equipe. Tempo médio/mediana de revisão (`ingested_at` → `finalizado_em`). Por revisor (confirmações/correções separadas). Atividade recente com `modelo_versao` por linha.
- Sem percentuais "tautológicos" (não mostra 100% acurácia em amostra pequena). Disclaimers em todos os blocos.

### Editor inline (FieldSection.tsx)
- Estado expandido opcional via link "Adicionar contexto": 8 pílulas de `tipo_erro` (radio) + campo `tipo_erro_detalhe` com placeholder dinâmico por categoria
- Confirmar sem mudar valor não grava `tipo_erro` (status='confirmado' com `tipo_erro=NULL`)
- Re-edit de campo classificado abre já expandido com pílula e detalhe preenchidos (puxado de `GET /api/corrections/{doc}`)
- Input aceita formato BR (23.868.692,18) ou US (23868692.18); `parseNumericBR` normaliza, `onBlur` reformata
- `auto_confirm: true` no POST `/api/corrections` → grava direto como 'confirmado' em revisoes_em_andamento

### Problemas Conhecidos
- **Dupla contagem (Regra 23)**: O LLM às vezes extrai sub-itens de notas explicativas que já estão contidos em outro campo do balanço (ex: "Adiantamentos a fornecedores" dentro de Estoques). A Regra 23 no prompt instrui o modelo a evitar, mas nem sempre funciona. Pós-processamento programático foi tentado mas é difícil distinguir campos legítimos de duplicados apenas por aritmética. Abordagem atual: confiar no modelo + regra, e o frontend mostra a divergência ao usuário via badge amarelo.
- **Endpoint update demora ~5-10min**: cada novo registro de modelo força rolling update do endpoint. Para testar pós-processamento localmente sem re-registrar, reverter `_postprocessed` do JSON e aplicar as funções Python direto.

### Testes Locais de Pós-Processamento
Para testar mudanças no `agent.py` sem re-registrar o modelo:
```python
# Pegar dado do Lakebase, reverter _postprocessed, aplicar novo pós-processamento
import psycopg2, json
conn = psycopg2.connect(host="ep-raspy-credit-d2j0p33d...", dbname="ocr_financeiro", ...)
cur.execute("SELECT extracted_json FROM resultados WHERE document_name LIKE '%HITACHI%' AND periodo = '2024-12-31'")
data = cur.fetchone()[0]
# Reverter pós-processamento
for p in reversed(data.pop('_postprocessed', [])):
    parts = p['campo'].split('.')
    obj = data
    for part in parts[:-1]: obj = obj.get(part, {})
    obj[parts[-1]] = p['original']
# Aplicar novo código (copiar funções do agent.py)
```
