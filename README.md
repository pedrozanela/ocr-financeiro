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
├── config.py                        # Configuracao centralizada (todas as env vars)
├── app.py                           # FastAPI entrypoint
├── app.yaml                         # Databricks App config
├── requirements.txt
├── .env.example                     # Template de variaveis de ambiente
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
│       ├── FontesPanel.tsx           # Audit trail
│       ├── fieldDefinitions.ts       # 44 campos DRE + Ativo/Passivo
│       ├── DocumentList.tsx          # Sidebar de documentos
│       └── MetricsDashboard.tsx      # Metricas
│
├── scripts/                         # Execucao local
│   ├── log_new_version.py           # Registra modelo + atualiza endpoint
│   ├── generate_fewshot.py          # Gera few-shot a partir das correcoes
│   └── update_fewshot_and_deploy.py # Pipeline completo (fewshot + log + deploy)
│
├── notebooks/                       # Execucao no Databricks (Jobs)
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

Todas no schema `{UC_CATALOG}.{UC_SCHEMA}` (default: `pedro_zanela.ocr_financeiro`).

| Tabela | Descricao |
|---|---|
| `documentos` | Texto extraido dos PDFs (via `ai_parse_document`) |
| `resultados` | Dados estruturados — 1 linha por (documento, tipo_entidade, periodo) |
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
| `assessment_json` | STRING | Reservado para avaliacao futura |
| `token_usage_json` | STRING | Tokens consumidos (input, output, custo) |
| `razao_social` | STRING | Nome da empresa |
| `cnpj` | STRING | CNPJ |
| `tipo_demonstrativo` | STRING | INDIVIDUAL ANUAL, CONSOLIDADO TRIMESTRAL, etc. |
| `moeda` | STRING | REAL, USD |
| `escala_valores` | STRING | UNIDADE, MILHARES, MILHOES |

## Configuracao

Todas as variaveis em `config.py`, lidas de variaveis de ambiente com fallbacks:

| Variavel | Default | Descricao |
|---|---|---|
| `UC_CATALOG` | `pedro_zanela` | Catalogo Unity Catalog |
| `UC_SCHEMA` | `ocr_financeiro` | Schema |
| `OCR_ENDPOINT` | `extrator-financeiro` | Serving endpoint |
| `OCR_MODEL` | `databricks-claude-sonnet-4-6` | LLM de extracao |
| `SECRET_SCOPE` | `ocr-financeiro` | Scope do Service Principal |
| `SECRET_KEY` | `pat-servico` | Chave do PAT |
| `WAREHOUSE_ID` | — | SQL Warehouse |
| `SERVERLESS_WAREHOUSE_ID` | — | Serverless Warehouse (ai_parse_document) |
| `MLFLOW_EXPERIMENT_ID` | — | MLflow experiment |
| `FEWSHOT_JOB_ID` | — | Job de atualizacao do modelo |

Para outro ambiente: altere `app.yaml` e/ou exporte as variaveis. Veja [MIGRATION.md](MIGRATION.md) para o guia completo.

## Deploy Rapido

```bash
# Build frontend
cd frontend && npm install && npx vite build && cd ..

# Sync + deploy
databricks sync . /Workspace/Users/<user>/techfin --profile <profile> --watch=false
databricks workspace import-dir frontend/dist /Workspace/Users/<user>/techfin/frontend/dist --overwrite --profile <profile>
databricks apps deploy <app-name> --source-code-path /Workspace/Users/<user>/techfin --profile <profile>
```

Para migracao completa em novo ambiente, veja [MIGRATION.md](MIGRATION.md).

## DRE

44 campos identicos ao Plano de Contas Supplier:

Receita (produto/mercadoria, servicos) → Deducoes → Incentivos → Receita Liquida → Custo (CMV + Superveniencias) → Lucro Bruto → Despesas Operacionais (vendas, PDD, administrativas, tributarias, gerais, depreciacao, amortizacao) → Lucro Operacional → Resultado Financeiro (encargos, descontos, variacao cambial) → Lucro Financeiro → Equivalencia Patrimonial, Receitas/Despesas nao operacionais, Correcao monetaria, Alienacao de ativos → LAIR → IR + CSLL → Participacoes → Lucro Liquido.

O DE-PARA (`depara.json`) mapeia 73 campos com centenas de aliases encontrados em balancos brasileiros.
