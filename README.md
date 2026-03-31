# OCR Financeiro

Extracao automatica de dados financeiros de Balancos Patrimoniais e DRE em PDF, com revisao humana e feedback loop.

Usa Claude Sonnet 4.6 via Foundation Model API no Databricks. Modelo customizado (MLflow PythonModel) servido como endpoint, com aplicacao web (FastAPI + React) para revisao e correcao dos resultados.

## Arquitetura

```
PDF  ──>  ai_parse_document  ──>  documentos (texto)
                                       |
                                       v
                              extrator-financeiro (Serving Endpoint)
                              |  Claude Sonnet 4.6
                              |  DE-PARA + Regras + Few-shot
                              |
                              v
                          resultados (JSON estruturado)
                              |
                              v
                    Review App (FastAPI + React)
                    |  Revisao por abas (Ativo, Passivo, DRE, Fontes, Pontos de Atencao)
                    |  Correcoes manuais ──> correcoes (Delta)
                    |  Export Excel (formato Supplier)
                    |
                    v
              Feedback Loop
              |  correcoes ──> generate_fewshot ──> few_shot_examples.json
              |  log_new_version ──> nova versao MLflow ──> endpoint atualizado
              +  Automatico (job semanal) ou manual (botao na UI)
```

## Estrutura

```
.
├── databricks.yml                   # DABs bundle config (jobs, variaveis, targets)
├── config.py                        # Configuracao centralizada (todas as env vars)
├── app.py                           # FastAPI entrypoint
├── app.yaml                         # Databricks App config
├── requirements.txt
├── .databricksignore                # Arquivos excluidos do sync
│
├── model/                           # MLflow PythonModel + artifacts
│   ├── agent.py                     # Modelo de extracao
│   ├── output_schema.json           # Schema dos campos financeiros
│   ├── depara.json                  # Dicionario de aliases contabeis (73 campos)
│   ├── regras_classificacao.json    # 10 regras contabeis obrigatorias
│   └── few_shot_examples.json       # Exemplos de correcoes (gerado automaticamente)
│
├── server/                          # Backend FastAPI
│   ├── config.py                    # Re-export do config raiz
│   ├── db.py                        # SQL via Statement Execution API
│   └── routes/
│       ├── documents.py             # Listagem, detalhe, reprocessamento, PDF
│       ├── corrections.py           # CRUD de correcoes
│       ├── upload.py                # Upload PDF + processamento async
│       ├── export.py                # Export Excel (formato Supplier)
│       ├── metrics.py               # Dashboard de metricas
│       └── admin.py                 # Trigger de atualizacao do modelo
│
├── frontend/                        # React + TypeScript + Vite + Tailwind
│   └── src/components/
│       ├── FinancialReview.tsx       # Editor multi-aba
│       ├── FieldSection.tsx          # Campos editaveis
│       ├── PontosDeAtencao.tsx       # Validacoes contabeis automaticas
│       ├── FontesPanel.tsx          # Audit trail
│       ├── fieldDefinitions.ts       # 44 campos DRE + Ativo/Passivo
│       ├── DocumentList.tsx          # Sidebar de documentos
│       └── MetricsDashboard.tsx      # Metricas
│
├── scripts/                         # Execucao local (alternativa aos notebooks)
│   ├── log_new_version.py           # Registra modelo + atualiza endpoint
│   ├── generate_fewshot.py          # Gera few-shot a partir das correcoes
│   └── update_fewshot_and_deploy.py # Pipeline completo (fewshot + log + deploy)
│
├── notebooks/                       # Execucao no Databricks (via DABs jobs)
│   ├── setup_infrastructure.py      # Cria schema, tabelas e volume (1x)
│   ├── register_model.py           # Registra modelo no MLflow UC (1x)
│   ├── grant_permissions.py         # Concede permissoes ao SP da app (1x)
│   ├── run_llm_from_table.py        # Batch: texto ja extraido -> endpoint -> resultados
│   ├── update_fewshot.py            # Job agendado: atualiza modelo com correcoes
│   ├── batch_job.py                 # Batch: PDF -> texto -> endpoint -> resultados
│   ├── reprocess_all.py             # Reprocessa todos os documentos
│   └── reprocess_failed.py          # Retry de documentos com erro
│
└── MIGRATION.md                     # Guia completo para deploy em novo ambiente
```

## Modelo

MLflow PythonModel registrado no Unity Catalog como `extrator_financeiro`.

Recebe texto pre-extraido de um PDF e retorna JSON estruturado com dados de Balanco Patrimonial e DRE. Cada versao do modelo carrega 4 artifacts:

| Artifact | Descricao |
|---|---|
| `output_schema.json` | Schema JSON com ~70 campos financeiros |
| `depara.json` | 73 campos com conceito e lista de aliases contabeis |
| `regras_classificacao.json` | 10 regras de classificacao obrigatorias |
| `few_shot_examples.json` | Top 20 exemplos de correcoes (gerado de `correcoes`) |

A separacao em artifacts permite que um contador edite o DE-PARA ou as regras sem mexer no codigo.

### Regras de Classificacao

1. Direito de Uso / Arrendamento (IFRS 16) → Imobilizado
2. Aplicacoes Financeiras e Consorcios (ANC) → Investimentos
3. Provisao de Contingencias → Provisoes (LP)
4. Ajuste de Avaliacao Patrimonial → Reservas de Reavaliacao
5. Despesas Administrativas = liquido de sub-itens
6. Fornecedores — incluir Fornecedores-Convenio
7. DRE — sempre usar valor acumulado do periodo
8. IRPJ e CSLL — separar em dois campos
9. Emprestimos a socios → C/C Socios
10. Receitas/Despesas nao-operacionais → campos proprios

## Feedback Loop

Correcoes feitas na UI sao salvas na tabela `correcoes`. O modelo incorpora essas correcoes automaticamente:

1. `generate_fewshot.py` le as correcoes, agrupa por padrao de erro, gera os 20 exemplos mais representativos
2. `log_new_version.py` registra nova versao MLflow com o artifact atualizado e atualiza o endpoint
3. O modelo passa a usar os exemplos no prompt, melhorando em erros recorrentes

**Automatico**: job semanal (segunda 6h BRT)
**Manual**: botao "Atualizar Modelo" na UI

```bash
# Pipeline completo via CLI
python scripts/update_fewshot_and_deploy.py --all
```

## Review App

Aplicativo Databricks (FastAPI + React).

### Funcionalidades

- Listagem de documentos com razao social, CNPJ, ativo total, lucro liquido
- Abas: Identificacao, Ativo, Passivo, DRE, Fontes, Pontos de Atencao
- Correcao inline com comentario
- Upload de PDFs com processamento assincrono
- Reprocessamento individual
- Export Excel identico ao Plano de Contas Supplier
- Visualizacao do PDF original
- Pontos de Atencao: validacoes contabeis (Ativo=Passivo, DRE cascata, etc.)
- Botao para atualizar o modelo com correcoes acumuladas

### Endpoints

| Rota | Metodo | Descricao |
|---|---|---|
| `/api/documents` | GET | Lista documentos |
| `/api/documents/{name}` | GET | Detalhe com JSON |
| `/api/documents/{name}/pdf` | GET | PDF original |
| `/api/documents/{name}/reprocess` | POST | Reprocessa via endpoint |
| `/api/documents/upload` | POST | Upload + processamento async |
| `/api/documents/{name}/status` | GET | Status do processamento |
| `/api/corrections/{name}` | GET | Correcoes do documento |
| `/api/corrections` | POST | Salvar correcao |
| `/api/corrections/{name}/{campo}` | DELETE | Remover correcao |
| `/api/metrics` | GET | Metricas agregadas |
| `/api/export/excel` | GET | Download Excel |
| `/api/admin/update-model` | POST | Dispara atualizacao do modelo |
| `/api/admin/update-model/status/{id}` | GET | Status da atualizacao |

## Tabelas

Todas no schema `{UC_CATALOG}.{UC_SCHEMA}`.

| Tabela | Descricao |
|---|---|
| `documentos` | Texto extraido dos PDFs (via `ai_parse_document`) |
| `resultados` | Dados estruturados — 1 linha por (documento, tipo_entidade, periodo) |
| `resultados_final` | Resultados com correcoes aplicadas (para export) |
| `correcoes` | Correcoes humanas (campo, valor extraido, valor correto, comentario, status) |

**Volume**: `documentos_pdf` — PDFs originais dos balancos.

**Modelo**: `extrator_financeiro` — MLflow PythonModel registrado no Unity Catalog.

### Schema de `resultados`

| Coluna | Tipo | Descricao |
|---|---|---|
| `document_name` | STRING | Nome do PDF (PK) |
| `tipo_entidade` | STRING | CONSOLIDADO, CONTROLADORA ou INDIVIDUAL (PK) |
| `periodo` | STRING | YYYY-MM-DD (PK) |
| `extracted_json` | STRING | JSON com todos os campos financeiros |
| `assessment_json` | STRING | Avaliacao de qualidade do LLM Judge |
| `token_usage_json` | STRING | Tokens consumidos (input, output, custo) |
| `razao_social` | STRING | Nome da empresa |
| `cnpj` | STRING | CNPJ |
| `tipo_demonstrativo` | STRING | INDIVIDUAL ANUAL, CONSOLIDADO TRIMESTRAL, etc. |
| `moeda` | STRING | REAL, USD |
| `escala_valores` | STRING | UNIDADE, MILHARES, MILHOES |
| `processado_em` | TIMESTAMP | Data/hora do processamento |
| `modelo_versao` | STRING | Versao do endpoint usado |

## Configuracao

Tres arquivos controlam a configuracao:

| Arquivo | Escopo | Quando usar |
|---|---|---|
| `databricks.yml` | Jobs, variaveis, targets | Parametros do DABs bundle (catalogo, warehouse IDs, secrets) |
| `config.py` | Fallbacks para dev local | Defaults usados quando env vars nao estao definidas |
| `app.yaml` | Databricks App | Env vars injetadas no runtime da app |

Todas as variaveis sao lidas de environment com fallbacks em `config.py`:

| Variavel | Descricao |
|---|---|
| `UC_CATALOG` | Catalogo Unity Catalog |
| `UC_SCHEMA` | Schema (default: `ocr_financeiro`) |
| `OCR_ENDPOINT` | Serving endpoint (default: `extrator-financeiro`) |
| `OCR_MODEL` | LLM de extracao (default: `databricks-claude-sonnet-4-6`) |
| `SECRET_SCOPE` | Scope de secrets (default: `ocr-financeiro`) |
| `SECRET_KEY` | Chave do PAT no scope (default: `pat-servico`) |
| `WAREHOUSE_ID` | SQL Warehouse para queries da app |
| `SERVERLESS_WAREHOUSE_ID` | Serverless Warehouse para `ai_parse_document` |
| `MLFLOW_EXPERIMENT_ID` | MLflow experiment (criado automaticamente pelo notebook) |
| `FEWSHOT_JOB_ID` | Job de atualizacao do modelo |

## Deploy via DABs

O projeto usa Databricks Asset Bundles para deploy. Os jobs e notebooks sao parametrizados via `databricks.yml`.

```bash
# 1. Configurar variaveis em databricks.yml, config.py e app.yaml

# 2. Build frontend
cd frontend && npm install && npx vite build && cd ..

# 3. Criar secret scope + PAT
databricks secrets create-scope ocr-financeiro --profile <profile>
databricks tokens create --comment "ocr-financeiro" --profile <profile>
databricks secrets put-secret ocr-financeiro pat-servico --string-value "<token>" --profile <profile>

# 4. Deploy bundle (sync + jobs)
databricks bundle deploy --target <target>

# 5. Criar infraestrutura, registrar modelo
databricks bundle run setup_infrastructure --target <target>
databricks bundle run register_model --target <target>

# 6. Criar serving endpoint + app (ver MIGRATION.md para detalhes)

# 7. Upload PDFs + extrair
databricks fs cp meu_balanco.pdf "dbfs:/Volumes/<catalog>/ocr_financeiro/documentos_pdf/" --profile <profile>
databricks bundle run run_llm_extraction --target <target>
```

Para o guia completo passo a passo, veja [MIGRATION.md](MIGRATION.md).

## DRE

44 campos identicos ao Plano de Contas Supplier:

Receita (produto/mercadoria, servicos) → Deducoes → Incentivos → Receita Liquida → Custo (CMV + Superveniencias) → Lucro Bruto → Despesas Operacionais (vendas, PDD, administrativas, tributarias, gerais, depreciacao, amortizacao) → Lucro Operacional → Resultado Financeiro (encargos, descontos, variacao cambial) → Lucro Financeiro → Equivalencia Patrimonial, Receitas/Despesas nao operacionais, Correcao monetaria, Alienacao de ativos → LAIR → IR + CSLL → Participacoes → Lucro Liquido.

O DE-PARA (`depara.json`) mapeia 73 campos com centenas de aliases encontrados em balancos brasileiros.
