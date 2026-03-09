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
      { label: 'Receita Operacional Bruta',             path: 'dre.receita_operacional_bruta',                  type: 'number' },
      { label: 'Vendas Anuladas',                       path: 'dre.vendas_anuladas',                            type: 'number' },
      { label: 'Abatimentos',                           path: 'dre.abatimentos',                                type: 'number' },
      { label: 'Impostos sobre Vendas',                 path: 'dre.impostos_incidentes_sobre_vendas',           type: 'number' },
      { label: '▶ Total Deduções',                      path: 'dre.total_deducoes',                             type: 'number', isTotal: true },
      { label: '▶ Receita Operacional Líquida',         path: 'dre.receita_operacional_liquida',                type: 'number', isTotal: true },
      { label: 'Custo dos Serv./Prod./Merc. Vendidos',  path: 'dre.custo_servicos_produtos_mercadorias_vendidas', type: 'number' },
      { label: '▶ Lucro Bruto',                         path: 'dre.lucro_bruto',                                type: 'number', isTotal: true },
      { label: 'Despesas com Vendas',                   path: 'dre.despesas_com_vendas',                        type: 'number' },
      { label: 'Provisão p/ Devedores Duvidosos',       path: 'dre.provisao_para_devedores_duvidosos',          type: 'number' },
      { label: 'Outras Receitas/Despesas Operacionais', path: 'dre.outras_receitas_despesas_operacionais',      type: 'number' },
      { label: 'Despesas Administrativas',              path: 'dre.despesas_administrativas',                   type: 'number' },
      { label: 'Despesas Tributárias',                  path: 'dre.despesas_tributarias',                       type: 'number' },
      { label: 'Despesas Gerais',                       path: 'dre.despesas_gerais',                            type: 'number' },
      { label: 'Depreciação',                           path: 'dre.depreciacao',                                type: 'number' },
      { label: 'Amortização',                           path: 'dre.amortizacao',                                type: 'number' },
      { label: '▶ Total Despesas Operacionais',         path: 'dre.total_despesas_operacionais',                type: 'number', isTotal: true },
      { label: '▶ Lucro Operacional (EBIT)',             path: 'dre.lucro_operacional',                          type: 'number', isTotal: true },
      { label: 'Despesas Financeiras',                  path: 'dre.despesas_financeiras',                       type: 'number' },
      { label: 'Receitas Financeiras',                  path: 'dre.receitas_financeiras',                       type: 'number' },
      { label: '▶ Resultado Financeiro',                path: 'dre.total_resultado_financeiro',                 type: 'number', isTotal: true },
      { label: 'Equivalência Patrimonial',              path: 'dre.resultado_de_equivalencia_patrimonial',      type: 'number' },
      { label: '▶ LAIR',                                path: 'dre.lucro_antes_imposto_de_renda',               type: 'number', isTotal: true },
      { label: 'Provisão IRPJ / CSLL',                  path: 'dre.provisao_imposto_de_renda_csll',             type: 'number' },
      { label: '★ Lucro Líquido',                       path: 'dre.lucro_liquido',                              type: 'number', isTotal: true },
    ],
  },
]
