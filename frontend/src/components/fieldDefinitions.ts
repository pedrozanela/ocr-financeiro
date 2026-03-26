export interface FieldDef {
  label: string
  path: string          // dot-separated path in the JSON
  type: 'number' | 'text' | 'date'
  isTotal?: boolean
}

export interface SectionDef {
  label: string
  fields: FieldDef[]
}

export const SECTIONS: SectionDef[] = [
  {
    label: 'Identificação',
    fields: [
      { label: 'Razão Social',           path: 'razao_social',                        type: 'text' },
      { label: 'CNPJ',                   path: 'cnpj',                                type: 'text' },
      { label: 'Período',                path: 'identificacao.periodo',               type: 'date' },
      { label: 'Tipo de Demonstrativo',  path: 'identificacao.tipo_demonstrativo',    type: 'text' },
      { label: 'Moeda',                  path: 'identificacao.moeda',                 type: 'text' },
      { label: 'Escala de Valores',      path: 'identificacao.escala_valores',        type: 'text' },
    ],
  },
  {
    label: 'Ativo',
    fields: [
      // Circulante
      { label: 'Disponibilidades',                      path: 'ativo_circulante.disponibilidades',                        type: 'number' },
      { label: 'Títulos a Receber',                     path: 'ativo_circulante.titulos_a_receber',                       type: 'number' },
      { label: 'Estoques',                              path: 'ativo_circulante.estoques',                                type: 'number' },
      { label: 'Adiantamentos',                         path: 'ativo_circulante.adiantamentos',                           type: 'number' },
      { label: 'Impostos a Recuperar (CP)',             path: 'ativo_circulante.impostos_a_recuperar',                    type: 'number' },
      { label: 'Outros Ativos Circulantes',             path: 'ativo_circulante.outros_ativos_circulantes',               type: 'number' },
      { label: 'C/C Sócios / Coligadas (AC)',           path: 'ativo_circulante.conta_corrente_socios_control_colig',     type: 'number' },
      { label: 'Outros Ativos Financeiros (AC)',        path: 'ativo_circulante.outros_ativos_financeiros',               type: 'number' },
      { label: '▶ Total Ativo Circulante',              path: 'ativo_circulante.total_ativo_circulante',                  type: 'number', isTotal: true },
      // Não Circulante
      { label: 'Títulos a Receber (LP)',                path: 'ativo_nao_circulante.titulos_a_receber',                   type: 'number' },
      { label: 'Estoques (LP)',                         path: 'ativo_nao_circulante.estoques',                            type: 'number' },
      { label: 'Adiantamentos (LP)',                    path: 'ativo_nao_circulante.adiantamentos',                       type: 'number' },
      { label: 'Impostos a Recuperar (LP)',             path: 'ativo_nao_circulante.impostos_a_recuperar',                type: 'number' },
      { label: 'Despesas Pagas Antecipadamente',        path: 'ativo_nao_circulante.despesas_pagas_antecipadamente',      type: 'number' },
      { label: 'C/C Sócios / Coligadas (ANC)',          path: 'ativo_nao_circulante.conta_corrente_socios_control_colig', type: 'number' },
      { label: 'Outros Realizável LP',                  path: 'ativo_nao_circulante.outros_realizavel_a_longo_prazo',    type: 'number' },
      { label: '▶ Total Ativo Não Circulante',          path: 'ativo_nao_circulante.total_ativo_nao_circulante',          type: 'number', isTotal: true },
      // Permanente
      { label: 'Investimentos',                         path: 'ativo_permanente.investimentos',                           type: 'number' },
      { label: 'Imobilizado',                           path: 'ativo_permanente.imobilizado',                             type: 'number' },
      { label: 'Intangível / Diferido',                 path: 'ativo_permanente.intangivel_diferido',                     type: 'number' },
      { label: '▶ Total Ativo Permanente',              path: 'ativo_permanente.total_ativo_permanente',                  type: 'number', isTotal: true },
      // Total
      { label: '★ Ativo Total',                         path: 'ativo_total',                                              type: 'number', isTotal: true },
    ],
  },
  {
    label: 'Passivo',
    fields: [
      // Circulante
      { label: 'Fornecedores (CP)',                     path: 'passivo_circulante.fornecedores',                                        type: 'number' },
      { label: 'Financiamentos / Empréstimos (CP)',     path: 'passivo_circulante.financiamentos_com_instituicoes_de_credito',          type: 'number' },
      { label: 'Salários e Contribuições (CP)',         path: 'passivo_circulante.salarios_contribuicoes',                              type: 'number' },
      { label: 'Tributos (CP)',                         path: 'passivo_circulante.tributos',                                            type: 'number' },
      { label: 'Adiantamentos de Clientes (CP)',        path: 'passivo_circulante.adiantamentos',                                       type: 'number' },
      { label: 'C/C Sócios / Coligadas (PC)',           path: 'passivo_circulante.conta_corrente_socios_coligadas_controladas',         type: 'number' },
      { label: 'Outros Passivos Circulantes',           path: 'passivo_circulante.outros_passivos_circulante',                          type: 'number' },
      { label: 'Provisões (CP)',                        path: 'passivo_circulante.provisoes',                                           type: 'number' },
      { label: 'Outros Passivos Financeiros (CP)',      path: 'passivo_circulante.outros_passivos_financeiros',                         type: 'number' },
      { label: '▶ Total Passivo Circulante',            path: 'passivo_circulante.total_passivo_circulante',                            type: 'number', isTotal: true },
      // Não Circulante
      { label: 'Fornecedores (LP)',                     path: 'passivo_nao_circulante.fornecedores',                                    type: 'number' },
      { label: 'Financiamentos / Empréstimos (LP)',     path: 'passivo_nao_circulante.financiamentos_com_instituicoes_de_credito',      type: 'number' },
      { label: 'Salários e Contribuições (LP)',         path: 'passivo_nao_circulante.salarios_contribuicoes',                          type: 'number' },
      { label: 'Tributos (LP)',                         path: 'passivo_nao_circulante.tributos',                                        type: 'number' },
      { label: 'Adiantamentos de Clientes (LP)',        path: 'passivo_nao_circulante.adiantamentos',                                   type: 'number' },
      { label: 'C/C Sócios / Coligadas (PNC)',          path: 'passivo_nao_circulante.conta_corrente_socios_coligadas_controladas',     type: 'number' },
      { label: 'Outros Passivos Não Circulantes',       path: 'passivo_nao_circulante.outros_passivos_nao_circulantes',                 type: 'number' },
      { label: 'Provisões (LP)',                        path: 'passivo_nao_circulante.provisoes',                                       type: 'number' },
      { label: '▶ Total Passivo Não Circulante',        path: 'passivo_nao_circulante.total_passivo_nao_circulante',                    type: 'number', isTotal: true },
      // Patrimônio Líquido
      { label: 'Capital Social',                        path: 'patrimonio_liquido.capital_social',                                      type: 'number' },
      { label: 'Reserva de Capital',                    path: 'patrimonio_liquido.reserva_de_capital',                                  type: 'number' },
      { label: 'Reservas de Lucro',                     path: 'patrimonio_liquido.reservas_de_lucro',                                   type: 'number' },
      { label: 'Reservas de Reavaliação',               path: 'patrimonio_liquido.reservas_de_reavaliacao',                             type: 'number' },
      { label: 'Outras Reservas',                       path: 'patrimonio_liquido.outras_reservas',                                     type: 'number' },
      { label: 'Lucros / Prejuízos Acumulados',         path: 'patrimonio_liquido.lucros_ou_prejuizos_acumulados',                      type: 'number' },
      { label: 'Ações em Tesouraria',                   path: 'patrimonio_liquido.acoes_em_tesouraria',                                 type: 'number' },
      { label: '▶ Total Patrimônio Líquido',            path: 'patrimonio_liquido.total_patrimonio_liquido',                            type: 'number', isTotal: true },
      // Total
      { label: '★ Passivo Total',                       path: 'passivo_total',                                                          type: 'number', isTotal: true },
    ],
  },
  {
    label: 'DRE',
    fields: [
      // Receita
      { label: 'Receita de venda produto/mercadoria',       path: 'dre.receita_venda_produto_mercadoria',              type: 'number' },
      { label: 'Receita serviços/arrendamento',             path: 'dre.receita_servicos_arrendamento',                 type: 'number' },
      { label: '▶ Receita operacional bruta',               path: 'dre.receita_operacional_bruta',                     type: 'number', isTotal: true },
      // Deduções
      { label: 'Vendas anuladas',                           path: 'dre.vendas_anuladas',                               type: 'number' },
      { label: 'Abatimentos',                               path: 'dre.abatimentos',                                   type: 'number' },
      { label: 'Impostos incidentes sobre vendas',          path: 'dre.impostos_incidentes_sobre_vendas',              type: 'number' },
      { label: '▶ Deduções de receita bruta',               path: 'dre.total_deducoes',                                type: 'number', isTotal: true },
      { label: 'Incentivos a exportações',                  path: 'dre.incentivos_a_exportacoes',                      type: 'number' },
      { label: '▶ Receita operacional líquida',             path: 'dre.receita_operacional_liquida',                   type: 'number', isTotal: true },
      // Custo
      { label: 'Custo Serviços/Produtos/Mercadorias Vendidas', path: 'dre.custo_servicos_produtos_mercadorias_vendidas', type: 'number' },
      { label: 'Superveniências ativas',                    path: 'dre.superveniencias_ativas',                        type: 'number' },
      { label: '▶ Custo',                                   path: 'dre.total_custo',                                   type: 'number', isTotal: true },
      { label: '▶ Lucro Bruto',                             path: 'dre.lucro_bruto',                                   type: 'number', isTotal: true },
      // Despesas operacionais
      { label: 'Despesas com vendas',                       path: 'dre.despesas_com_vendas',                           type: 'number' },
      { label: 'Provisão para devedores duvidosos',         path: 'dre.provisao_para_devedores_duvidosos',             type: 'number' },
      { label: 'Outras Receitas(-)/ Despesas Operacionais', path: 'dre.outras_receitas_despesas_operacionais',         type: 'number' },
      { label: 'Despesas administrativas',                  path: 'dre.despesas_administrativas',                      type: 'number' },
      { label: 'Despesas tributarias',                      path: 'dre.despesas_tributarias',                          type: 'number' },
      { label: 'Despesas gerais',                           path: 'dre.despesas_gerais',                               type: 'number' },
      { label: 'Depreciação',                               path: 'dre.depreciacao',                                   type: 'number' },
      { label: 'Amortização',                               path: 'dre.amortizacao',                                   type: 'number' },
      { label: '▶ Despesas operacionais',                   path: 'dre.total_despesas_operacionais',                   type: 'number', isTotal: true },
      { label: '▶ Lucro operacional',                       path: 'dre.lucro_operacional',                             type: 'number', isTotal: true },
      // Resultado financeiro
      { label: 'Encargos Financeiros',                      path: 'dre.encargos_financeiros',                          type: 'number' },
      { label: 'Descontos concedidos',                      path: 'dre.descontos_concedidos',                          type: 'number' },
      { label: 'Variação cambial/Corr. Monetária não paga', path: 'dre.variacao_cambial_nao_paga',                     type: 'number' },
      { label: '▶ Despesas financeiras',                    path: 'dre.despesas_financeiras',                          type: 'number', isTotal: true },
      { label: 'Receitas financeiras',                      path: 'dre.receitas_financeiras',                          type: 'number' },
      { label: 'Variação cambial/Corr. Monetária não recebida', path: 'dre.variacao_cambial_nao_recebida',             type: 'number' },
      { label: '▶ Receitas financeiras (total)',            path: 'dre.total_receitas_financeiras',                    type: 'number', isTotal: true },
      { label: '▶ Lucro Financeiro',                        path: 'dre.lucro_financeiro',                              type: 'number', isTotal: true },
      // Pós-financeiro
      { label: '(-/+)Resultado de equivalência patrimonial', path: 'dre.resultado_de_equivalencia_patrimonial',        type: 'number' },
      { label: 'Receita não operacional',                   path: 'dre.receita_nao_operacional',                       type: 'number' },
      { label: '(-)Despesa não operacional',                path: 'dre.despesa_nao_operacional',                       type: 'number' },
      { label: '(-/+)Saldo de correção monetária',          path: 'dre.saldo_correcao_monetaria',                      type: 'number' },
      { label: 'Resultado de alienação de ativos',          path: 'dre.resultado_alienacao_ativos',                    type: 'number' },
      { label: '▶ Lucro antes do imposto de renda',         path: 'dre.lucro_antes_imposto_de_renda',                  type: 'number', isTotal: true },
      // IR e CSLL
      { label: '(-)Provisão para imposto de renda',         path: 'dre.provisao_imposto_de_renda',                     type: 'number' },
      { label: '(-)CSLL',                                   path: 'dre.csll',                                          type: 'number' },
      { label: '▶ Lucro antes das participações',           path: 'dre.lucro_antes_participacoes',                     type: 'number', isTotal: true },
      // Participações
      { label: 'Partic. e gratificações estatutárias',      path: 'dre.participacoes_gratificacoes_estatutarias',      type: 'number' },
      { label: '▶ Lucro antes da participação minoritária', path: 'dre.lucro_antes_participacao_minoritaria',          type: 'number', isTotal: true },
      { label: 'Participação de minoritários',              path: 'dre.participacao_minoritarios',                     type: 'number' },
      { label: '★ Lucro líquido',                           path: 'dre.lucro_liquido',                                 type: 'number', isTotal: true },
    ],
  },
]
