interface Props {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
}

// Human-readable labels for field paths (matching fieldDefinitions)
const FIELD_LABELS: Record<string, string> = {
  'razao_social': 'Razão Social',
  'cnpj': 'CNPJ',
  'identificacao.periodo': 'Período',
  'identificacao.tipo_demonstrativo': 'Tipo Demonstrativo',
  'identificacao.moeda': 'Moeda',
  'identificacao.escala_valores': 'Escala de Valores',
  'ativo_circulante.disponibilidades': 'Disponibilidades (AC)',
  'ativo_circulante.titulos_a_receber': 'Títulos a Receber (AC)',
  'ativo_circulante.estoques': 'Estoques (AC)',
  'ativo_circulante.adiantamentos': 'Adiantamentos (AC)',
  'ativo_circulante.impostos_a_recuperar': 'Impostos a Recuperar (CP)',
  'ativo_circulante.outros_ativos_circulantes': 'Outros Ativos Circulantes',
  'ativo_circulante.conta_corrente_socios_control_colig': 'C/C Sócios / Coligadas (AC)',
  'ativo_circulante.outros_ativos_financeiros': 'Outros Ativos Financeiros (AC)',
  'ativo_circulante.total_ativo_circulante': 'Total Ativo Circulante',
  'ativo_nao_circulante.titulos_a_receber': 'Títulos a Receber (LP)',
  'ativo_nao_circulante.estoques': 'Estoques (LP)',
  'ativo_nao_circulante.adiantamentos': 'Adiantamentos (LP)',
  'ativo_nao_circulante.impostos_a_recuperar': 'Impostos a Recuperar (LP)',
  'ativo_nao_circulante.despesas_pagas_antecipadamente': 'Despesas Pagas Antecipadamente (LP)',
  'ativo_nao_circulante.conta_corrente_socios_control_colig': 'C/C Sócios / Coligadas (LP)',
  'ativo_nao_circulante.outros_realizavel_a_longo_prazo': 'Outros Realizável a Longo Prazo',
  'ativo_nao_circulante.total_ativo_nao_circulante': 'Total Ativo Não Circulante',
  'ativo_permanente.investimentos': 'Investimentos',
  'ativo_permanente.imobilizado': 'Imobilizado',
  'ativo_permanente.intangivel_diferido': 'Intangível / Diferido',
  'ativo_permanente.total_ativo_permanente': 'Total Ativo Permanente',
  'ativo_total': 'Ativo Total',
  'passivo_circulante.fornecedores': 'Fornecedores (PC)',
  'passivo_circulante.financiamentos_com_instituicoes_de_credito': 'Financiamentos (PC)',
  'passivo_circulante.salarios_contribuicoes': 'Salários e Contribuições (PC)',
  'passivo_circulante.tributos': 'Tributos (PC)',
  'passivo_circulante.adiantamentos': 'Adiantamentos de Clientes (PC)',
  'passivo_circulante.conta_corrente_socios_coligadas_controladas': 'C/C Sócios / Coligadas (PC)',
  'passivo_circulante.outros_passivos_circulante': 'Outros Passivos Circulantes',
  'passivo_circulante.provisoes': 'Provisões (PC)',
  'passivo_circulante.outros_passivos_financeiros': 'Outros Passivos Financeiros (PC)',
  'passivo_circulante.total_passivo_circulante': 'Total Passivo Circulante',
  'passivo_nao_circulante.fornecedores': 'Fornecedores (PNC)',
  'passivo_nao_circulante.financiamentos_com_instituicoes_de_credito': 'Financiamentos (PNC)',
  'passivo_nao_circulante.salarios_contribuicoes': 'Salários e Contribuições (PNC)',
  'passivo_nao_circulante.tributos': 'Tributos (PNC)',
  'passivo_nao_circulante.adiantamentos': 'Adiantamentos de Clientes (PNC)',
  'passivo_nao_circulante.conta_corrente_socios_coligadas_controladas': 'C/C Sócios / Coligadas (PNC)',
  'passivo_nao_circulante.outros_passivos_nao_circulantes': 'Outros Passivos Não Circulantes',
  'passivo_nao_circulante.provisoes': 'Provisões (PNC)',
  'passivo_nao_circulante.total_passivo_nao_circulante': 'Total Passivo Não Circulante',
  'patrimonio_liquido.capital_social': 'Capital Social',
  'patrimonio_liquido.reserva_de_capital': 'Reserva de Capital',
  'patrimonio_liquido.reservas_de_lucro': 'Reservas de Lucro',
  'patrimonio_liquido.reservas_de_reavaliacao': 'Reservas de Reavaliação',
  'patrimonio_liquido.outras_reservas': 'Outras Reservas',
  'patrimonio_liquido.lucros_ou_prejuizos_acumulados': 'Lucros / Prejuízos Acumulados',
  'patrimonio_liquido.acoes_em_tesouraria': 'Ações em Tesouraria',
  'patrimonio_liquido.total_patrimonio_liquido': 'Total Patrimônio Líquido',
  'passivo_total': 'Passivo Total',
  'dre.receita_operacional_bruta': 'Receita Operacional Bruta',
  'dre.vendas_anuladas': 'Vendas Anuladas',
  'dre.abatimentos': 'Abatimentos',
  'dre.impostos_incidentes_sobre_vendas': 'Impostos Incidentes sobre Vendas',
  'dre.total_deducoes': 'Total Deduções',
  'dre.receita_operacional_liquida': 'Receita Operacional Líquida',
  'dre.custo_servicos_produtos_mercadorias_vendidas': 'CMV / CPV / CSP',
  'dre.lucro_bruto': 'Lucro Bruto',
  'dre.despesas_com_vendas': 'Despesas com Vendas',
  'dre.provisao_para_devedores_duvidosos': 'Provisão para Devedores Duvidosos',
  'dre.outras_receitas_despesas_operacionais': 'Outras Receitas / Despesas Operacionais',
  'dre.despesas_administrativas': 'Despesas Administrativas',
  'dre.despesas_tributarias': 'Despesas Tributárias',
  'dre.despesas_gerais': 'Despesas Gerais',
  'dre.depreciacao': 'Depreciação',
  'dre.amortizacao': 'Amortização',
  'dre.total_despesas_operacionais': 'Total Despesas Operacionais',
  'dre.lucro_operacional': 'Lucro Operacional (EBIT)',
  'dre.encargos_financeiros': 'Encargos Financeiros',
  'dre.descontos_concedidos': 'Descontos Concedidos',
  'dre.variacao_cambial_nao_paga': 'Variação Cambial Não Paga',
  'dre.despesas_financeiras': 'Despesas Financeiras',
  'dre.receitas_financeiras': 'Receitas Financeiras',
  'dre.variacao_cambial_nao_recebida': 'Variação Cambial Não Recebida',
  'dre.total_receitas_financeiras': 'Total Receitas Financeiras',
  'dre.lucro_financeiro': 'Lucro Financeiro',
  'dre.resultado_de_equivalencia_patrimonial': 'Resultado de Equivalência Patrimonial',
  'dre.receita_nao_operacional': 'Receita Não Operacional',
  'dre.despesa_nao_operacional': 'Despesa Não Operacional',
  'dre.saldo_correcao_monetaria': 'Saldo de Correção Monetária',
  'dre.resultado_alienacao_ativos': 'Resultado de Alienação de Ativos',
  'dre.lucro_antes_imposto_de_renda': 'LAIR',
  'dre.provisao_imposto_de_renda': 'Provisão para Imposto de Renda',
  'dre.csll': 'CSLL',
  'dre.lucro_antes_participacoes': 'Lucro Antes das Participações',
  'dre.participacoes_gratificacoes_estatutarias': 'Participações Estatutárias',
  'dre.lucro_antes_participacao_minoritaria': 'Lucro Antes da Participação Minoritária',
  'dre.participacao_minoritarios': 'Participação de Minoritários',
  'dre.lucro_liquido': 'Lucro Líquido',
}

const SECTION_ORDER = ['identificacao', 'ativo_circulante', 'ativo_nao_circulante', 'ativo_permanente', 'passivo_circulante', 'passivo_nao_circulante', 'patrimonio_liquido', 'dre']
const SECTION_LABELS: Record<string, string> = {
  'identificacao': 'Identificação',
  'ativo_circulante': 'Ativo Circulante',
  'ativo_nao_circulante': 'Ativo Não Circulante',
  'ativo_permanente': 'Ativo Permanente',
  'passivo_circulante': 'Passivo Circulante',
  'passivo_nao_circulante': 'Passivo Não Circulante',
  'patrimonio_liquido': 'Patrimônio Líquido',
  'dre': 'DRE',
}

export default function FontesPanel({ data }: Props) {
  const fontes: Record<string, string> = data?.fontes ?? {}
  const entries = Object.entries(fontes)

  if (entries.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-400">
        <svg className="w-12 h-12 mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p className="text-sm">Nenhuma fonte disponível para este documento.</p>
        <p className="text-xs mt-1 text-gray-300">Reprocesse o PDF para gerar as fontes.</p>
      </div>
    )
  }

  // Group by section
  const grouped: Record<string, Array<[string, string]>> = {}
  const rootEntries: Array<[string, string]> = []

  for (const [key, val] of entries) {
    const dot = key.indexOf('.')
    if (dot === -1) {
      rootEntries.push([key, val])
    } else {
      const section = key.substring(0, dot)
      if (!grouped[section]) grouped[section] = []
      grouped[section].push([key, val])
    }
  }

  return (
    <div className="space-y-5">
      {/* Root-level fields */}
      {rootEntries.length > 0 && (
        <div>
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">Geral</h3>
          <div className="space-y-2">
            {rootEntries.map(([key, val]) => (
              <FonteRow key={key} path={key} fonte={val} />
            ))}
          </div>
        </div>
      )}

      {/* Grouped by section */}
      {SECTION_ORDER.filter(s => grouped[s]).map(section => (
        <div key={section}>
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">
            {SECTION_LABELS[section] ?? section}
          </h3>
          <div className="space-y-2">
            {grouped[section].map(([key, val]) => (
              <FonteRow key={key} path={key} fonte={val} />
            ))}
          </div>
        </div>
      ))}

      {/* Sections not in SECTION_ORDER */}
      {Object.keys(grouped).filter(s => !SECTION_ORDER.includes(s)).map(section => (
        <div key={section}>
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-2">{section}</h3>
          <div className="space-y-2">
            {grouped[section].map(([key, val]) => (
              <FonteRow key={key} path={key} fonte={val} />
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function FonteRow({ path, fonte }: { path: string; fonte: string }) {
  const label = FIELD_LABELS[path] ?? path
  return (
    <div className="flex gap-3 text-sm bg-white border border-gray-100 rounded-lg px-4 py-3 hover:border-gray-200 transition-colors">
      <div className="w-52 shrink-0 text-gray-700 font-medium text-xs leading-5 pt-0.5">{label}</div>
      <div className="flex-1 text-gray-500 text-xs leading-5">{fonte}</div>
    </div>
  )
}
