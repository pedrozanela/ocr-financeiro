import { useMemo, useState } from 'react'
import { IconCheck, IconX, IconAlertTriangle, IconChevronRight } from '@tabler/icons-react'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DataRecord = { tipo_entidade: string | null; periodo: string | null; data: any }

type Status = 'ok' | 'warning' | 'error' | 'info'
type Filter = 'attention' | 'all' | 'error' | 'warning' | 'ok'
type TargetSection = 'identificacao' | 'ativo' | 'passivo' | 'dre'

interface CheckResult { status: Status; details: string }

interface Validation {
  label: string
  description: string
  category: string         // 'Balanço' | 'DRE' | 'Alertas' | 'Fontes'
  target_section: TargetSection
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  check: (data: any) => CheckResult
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function n(data: any, path: string): number {
  const parts = path.split('.')
  let cur = data
  for (const p of parts) { if (cur == null) return 0; cur = cur[p] }
  if (cur == null) return 0
  const v = parseFloat(String(cur))
  return isNaN(v) ? 0 : v
}
function fmtN(v: number): string {
  return new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 2 }).format(v)
}
function diffPct(a: number, b: number): number {
  const diff = Math.abs(a - b)
  if (diff <= 1) return 0
  const base = Math.max(Math.abs(a), Math.abs(b), 1)
  return (diff / base) * 100
}

const TOL = 0.01

const VALIDATIONS: Validation[] = [
  { label: 'Equação Contábil: Ativo Total = Passivo Total', description: 'O Ativo Total deve ser exatamente igual ao Passivo Total.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const ativo = n(data, 'ativo_total'), passivo = n(data, 'passivo_total')
      if (ativo === 0 && passivo === 0) return { status: 'info', details: 'Ativo e Passivo zerados — dados não extraídos?' }
      const pct = diffPct(ativo, passivo)
      if (pct > TOL) return { status: 'error', details: `Ativo: ${fmtN(ativo)} | Passivo: ${fmtN(passivo)} | Dif: ${fmtN(Math.abs(ativo - passivo))} (${pct.toFixed(2)}%)` }
      return { status: 'ok', details: `Ativo: ${fmtN(ativo)} = Passivo: ${fmtN(passivo)}` }
  }},
  { label: 'Lucro Líquido refletido em Lucros Acumulados ou Reservas de Lucro', description: 'O LL do exercício costuma estar dentro do valor de Lucros Acumulados ou Reservas de Lucro. Apenas alertamos quando ambos estão zerados mas há LL.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const ll = n(data, 'dre.lucro_liquido')
      const lpa = n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados')
      const rl = n(data, 'patrimonio_liquido.reservas_de_lucro')
      if (Math.abs(ll) > 1 && Math.abs(lpa) < 1 && Math.abs(rl) < 1)
        return { status: 'warning', details: `LL: ${fmtN(ll)} | L/P Acumulados: ${fmtN(lpa)} | Reservas de Lucro: ${fmtN(rl)} — ambos zerados, verificar` }
      if (Math.abs(ll) > 1 && Math.abs(lpa) < 1 && Math.abs(rl) > 1)
        return { status: 'ok', details: `LL: ${fmtN(ll)} | Lucro alocado em Reservas de Lucro: ${fmtN(rl)}` }
      return { status: 'ok', details: `LL: ${fmtN(ll)} | L/P Acumulados: ${fmtN(lpa)}` }
  }},
  { label: 'Ativo Total: AC + ANC + AP', description: 'Ativo Total deve ser a soma dos três grupos.', category: 'Balanço', target_section: 'ativo', check: (data) => {
      const total = n(data, 'ativo_total')
      if (total === 0) return { status: 'info', details: 'Ativo Total zerado' }
      const sum = n(data, 'ativo_circulante.total_ativo_circulante') + n(data, 'ativo_nao_circulante.total_ativo_nao_circulante') + n(data, 'ativo_permanente.total_ativo_permanente')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'error', details: `AC+ANC+AP: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `AC+ANC+AP = ${fmtN(total)}` }
  }},
  { label: 'Passivo Total: PC + PNC + PL', description: 'Passivo Total deve ser a soma dos três grupos.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const total = n(data, 'passivo_total')
      if (total === 0) return { status: 'info', details: 'Passivo Total zerado' }
      const sum = n(data, 'passivo_circulante.total_passivo_circulante') + n(data, 'passivo_nao_circulante.total_passivo_nao_circulante') + n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'error', details: `PC+PNC+PL: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `PC+PNC+PL = ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Ativo Circulante', description: 'Soma dos itens do AC deve ser igual ao total.', category: 'Balanço', target_section: 'ativo', check: (data) => {
      const total = n(data, 'ativo_circulante.total_ativo_circulante')
      if (total === 0) return { status: 'info', details: 'AC zerado' }
      const sum = n(data, 'ativo_circulante.disponibilidades') + n(data, 'ativo_circulante.titulos_a_receber') + n(data, 'ativo_circulante.estoques') + n(data, 'ativo_circulante.adiantamentos') + n(data, 'ativo_circulante.impostos_a_recuperar') + n(data, 'ativo_circulante.outros_ativos_circulantes') + n(data, 'ativo_circulante.conta_corrente_socios_control_colig') + n(data, 'ativo_circulante.outros_ativos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total AC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total AC: ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Passivo Circulante', description: 'Soma dos itens do PC deve ser igual ao total.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const total = n(data, 'passivo_circulante.total_passivo_circulante')
      if (total === 0) return { status: 'info', details: 'PC zerado' }
      const sum = n(data, 'passivo_circulante.fornecedores') + n(data, 'passivo_circulante.financiamentos_com_instituicoes_de_credito') + n(data, 'passivo_circulante.salarios_contribuicoes') + n(data, 'passivo_circulante.tributos') + n(data, 'passivo_circulante.adiantamentos') + n(data, 'passivo_circulante.conta_corrente_socios_coligadas_controladas') + n(data, 'passivo_circulante.outros_passivos_circulante') + n(data, 'passivo_circulante.provisoes') + n(data, 'passivo_circulante.outros_passivos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PC: ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Patrimônio Líquido', description: 'Soma dos componentes do PL deve ser igual ao total.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const total = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (total === 0) return { status: 'info', details: 'PL zerado' }
      const sum = n(data, 'patrimonio_liquido.capital_social') + n(data, 'patrimonio_liquido.reserva_de_capital') + n(data, 'patrimonio_liquido.reservas_de_lucro') + n(data, 'patrimonio_liquido.reservas_de_reavaliacao') + n(data, 'patrimonio_liquido.outras_reservas') + n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados') + n(data, 'patrimonio_liquido.acoes_em_tesouraria')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PL: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PL: ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Ativo Não Circulante', description: 'Soma dos itens do ANC deve ser igual ao total.', category: 'Balanço', target_section: 'ativo', check: (data) => {
      const total = n(data, 'ativo_nao_circulante.total_ativo_nao_circulante')
      if (total === 0) return { status: 'info', details: 'ANC zerado' }
      const sum = n(data, 'ativo_nao_circulante.titulos_a_receber') + n(data, 'ativo_nao_circulante.estoques') + n(data, 'ativo_nao_circulante.adiantamentos') + n(data, 'ativo_nao_circulante.impostos_a_recuperar') + n(data, 'ativo_nao_circulante.despesas_pagas_antecipadamente') + n(data, 'ativo_nao_circulante.conta_corrente_socios_control_colig') + n(data, 'ativo_nao_circulante.outros_realizavel_a_longo_prazo')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total ANC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total ANC: ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Ativo Permanente', description: 'Investimentos + Imobilizado + Intangível = Total AP.', category: 'Balanço', target_section: 'ativo', check: (data) => {
      const total = n(data, 'ativo_permanente.total_ativo_permanente')
      if (total === 0) return { status: 'info', details: 'AP zerado' }
      const sum = n(data, 'ativo_permanente.investimentos') + n(data, 'ativo_permanente.imobilizado') + n(data, 'ativo_permanente.intangivel_diferido')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total AP: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total AP: ${fmtN(total)}` }
  }},
  { label: 'Consistência interna do Passivo Não Circulante', description: 'Soma dos itens do PNC deve ser igual ao total.', category: 'Balanço', target_section: 'passivo', check: (data) => {
      const total = n(data, 'passivo_nao_circulante.total_passivo_nao_circulante')
      if (total === 0) return { status: 'info', details: 'PNC zerado' }
      const sum = n(data, 'passivo_nao_circulante.fornecedores') + n(data, 'passivo_nao_circulante.financiamentos_com_instituicoes_de_credito') + n(data, 'passivo_nao_circulante.salarios_contribuicoes') + n(data, 'passivo_nao_circulante.tributos') + n(data, 'passivo_nao_circulante.adiantamentos') + n(data, 'passivo_nao_circulante.conta_corrente_socios_coligadas_controladas') + n(data, 'passivo_nao_circulante.outros_passivos_nao_circulantes') + n(data, 'passivo_nao_circulante.provisoes')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PNC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PNC: ${fmtN(total)}` }
  }},
  { label: 'DRE: Deduções = Vendas Anuladas + Abatimentos + Impostos', description: 'Total de deduções deve ser a soma dos sub-itens.', category: 'DRE', target_section: 'dre', check: (data) => {
      const ded = n(data, 'dre.total_deducoes')
      if (ded === 0) return { status: 'info', details: 'Deduções zeradas' }
      const va = n(data, 'dre.vendas_anuladas'), ab = n(data, 'dre.abatimentos'), imp = n(data, 'dre.impostos_incidentes_sobre_vendas')
      if (va === 0 && ab === 0 && imp === 0) return { status: 'info', details: 'Sub-itens zerados (não detalhado)' }
      const sum = va + ab + imp, pct = diffPct(ded, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Deduções: ${fmtN(ded)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Deduções: ${fmtN(ded)}` }
  }},
  { label: 'DRE: ROL = ROB − Deduções + Incentivos', description: 'Receita Líquida deve ser ROB menos deduções mais incentivos.', category: 'DRE', target_section: 'dre', check: (data) => {
      const rol = n(data, 'dre.receita_operacional_liquida')
      if (rol === 0) return { status: 'info', details: 'ROL zerada' }
      const rob = n(data, 'dre.receita_operacional_bruta'), ded = n(data, 'dre.total_deducoes'), inc = n(data, 'dre.incentivos_a_exportacoes')
      const calc = rob - ded + inc, pct = diffPct(calc, rol)
      if (pct > TOL) return { status: 'warning', details: `ROB-Ded+Inc: ${fmtN(calc)} | ROL: ${fmtN(rol)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)}` }
  }},
  { label: 'DRE: Lucro Bruto = ROL − CMV', description: 'Receita Líquida menos Custo = Lucro Bruto.', category: 'DRE', target_section: 'dre', check: (data) => {
      const rol = n(data, 'dre.receita_operacional_liquida'), cmv = n(data, 'dre.custo_servicos_produtos_mercadorias_vendidas'), lb = n(data, 'dre.lucro_bruto')
      if (rol === 0 && lb === 0) return { status: 'info', details: 'ROL e Lucro Bruto zerados' }
      const calc = rol - cmv, pct = diffPct(calc, lb)
      if (pct > TOL) return { status: 'warning', details: `ROL-CMV: ${fmtN(calc)} | LB: ${fmtN(lb)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)} | CMV: ${fmtN(cmv)} | LB: ${fmtN(lb)}` }
  }},
  { label: 'DRE: EBIT = Lucro Bruto − Despesas Operacionais', description: 'Lucro Operacional deve ser LB menos despesas operacionais.', category: 'DRE', target_section: 'dre', check: (data) => {
      const lb = n(data, 'dre.lucro_bruto'), desp = n(data, 'dre.total_despesas_operacionais'), lo = n(data, 'dre.lucro_operacional')
      if (lb === 0 && lo === 0) return { status: 'info', details: 'LB e EBIT zerados' }
      const calc = lb - desp, pct = diffPct(calc, lo)
      if (pct > TOL) return { status: 'warning', details: `LB-DespOp: ${fmtN(calc)} | EBIT: ${fmtN(lo)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `LB: ${fmtN(lb)} | DespOp: ${fmtN(desp)} | EBIT: ${fmtN(lo)}` }
  }},
  { label: 'Consistência interna de Despesas Operacionais', description: 'Total das despesas operacionais deve ser igual à soma dos sub-itens.', category: 'DRE', target_section: 'dre', check: (data) => {
      const total = n(data, 'dre.total_despesas_operacionais')
      if (total === 0) return { status: 'info', details: 'Despesas Operacionais zeradas' }
      const sum = n(data, 'dre.despesas_com_vendas') + n(data, 'dre.provisao_para_devedores_duvidosos')
        + n(data, 'dre.outras_receitas_despesas_operacionais') + n(data, 'dre.despesas_administrativas')
        + n(data, 'dre.despesas_tributarias') + n(data, 'dre.despesas_gerais')
        + n(data, 'dre.depreciacao') + n(data, 'dre.amortizacao')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total: ${fmtN(total)}` }
  }},
  { label: 'DRE: Despesas Financeiras = Encargos + Descontos + Variação Cambial', description: 'Total de despesas financeiras deve ser a soma dos sub-itens.', category: 'DRE', target_section: 'dre', check: (data) => {
      const total = n(data, 'dre.despesas_financeiras')
      if (total === 0) return { status: 'info', details: 'Despesas Financeiras zeradas' }
      const enc = n(data, 'dre.encargos_financeiros'), desc = n(data, 'dre.descontos_concedidos'), vc = n(data, 'dre.variacao_cambial_nao_paga')
      if (enc === 0 && desc === 0 && vc === 0) return { status: 'info', details: 'Sub-itens zerados (não detalhado)' }
      const sum = enc + desc + vc, pct = diffPct(total, sum)
      if (pct <= TOL) return { status: 'ok', details: `Despesas Financeiras: ${fmtN(total)}` }
      if (sum < total) return { status: 'info', details: `Total: ${fmtN(total)} | Decomposição parcial (${fmtN(sum)}) — itens sem sub-campo dedicado` }
      return { status: 'warning', details: `Soma: ${fmtN(sum)} | DespFin: ${fmtN(total)} | Dif: ${pct.toFixed(2)}% (possível double count)` }
  }},
  { label: 'DRE: Receitas Financeiras = Receitas financeiras + Variação cambial não recebida', description: 'Total de receitas financeiras deve ser a soma dos sub-itens.', category: 'DRE', target_section: 'dre', check: (data) => {
      const total = n(data, 'dre.total_receitas_financeiras')
      if (total === 0) return { status: 'info', details: 'Receitas Financeiras zeradas' }
      const rec = n(data, 'dre.receitas_financeiras'), vc = n(data, 'dre.variacao_cambial_nao_recebida')
      if (rec === 0 && vc === 0) return { status: 'info', details: 'Sub-itens zerados (não detalhado)' }
      const sum = rec + vc, pct = diffPct(total, sum)
      if (pct <= TOL) return { status: 'ok', details: `Receitas Financeiras: ${fmtN(total)}` }
      if (sum < total) return { status: 'info', details: `Total: ${fmtN(total)} | Decomposição parcial (${fmtN(sum)}) — itens sem sub-campo dedicado` }
      return { status: 'warning', details: `Soma: ${fmtN(sum)} | RecFin: ${fmtN(total)} | Dif: ${pct.toFixed(2)}% (possível double count)` }
  }},
  { label: 'DRE: LAIR = EBIT + Resultado Financeiro + Equivalência + Não-operacionais', description: 'LAIR = EBIT (±) Resultado Financeiro (±) Equivalência Patrimonial (+) Receita não operacional (−) Despesa não operacional (±) Saldo de correção monetária (±) Resultado de alienação de ativos.', category: 'DRE', target_section: 'dre', check: (data) => {
      const lo = n(data, 'dre.lucro_operacional'), lair = n(data, 'dre.lucro_antes_imposto_de_renda')
      if (lair === 0) return { status: 'info', details: 'LAIR zerado' }
      const lf = n(data, 'dre.lucro_financeiro')
      const ep = n(data, 'dre.resultado_de_equivalencia_patrimonial')
      const rno = n(data, 'dre.receita_nao_operacional'); const dno = n(data, 'dre.despesa_nao_operacional')
      const scm = n(data, 'dre.saldo_correcao_monetaria'); const raa = n(data, 'dre.resultado_alienacao_ativos')
      const rf = lf - lo
      const calc = lo + rf + ep + rno - dno + scm + raa
      const pct = diffPct(calc, lair)
      if (pct > TOL) return { status: 'warning', details: `Calculado: ${fmtN(calc)} | LAIR: ${fmtN(lair)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `EBIT: ${fmtN(lo)} | RF: ${fmtN(rf)} | EP: ${fmtN(ep)} | LAIR: ${fmtN(lair)}` }
  }},
  { label: 'DRE: Lucro antes das participações = LAIR − IRPJ/CSLL', description: 'LAIR menos impostos (IRPJ + CSLL) deve ser o Lucro antes das participações estatutárias.', category: 'DRE', target_section: 'dre', check: (data) => {
      const lair = n(data, 'dre.lucro_antes_imposto_de_renda')
      const ir = n(data, 'dre.provisao_imposto_de_renda') + n(data, 'dre.csll')
      const lap = n(data, 'dre.lucro_antes_participacoes')
      if (lair === 0 && lap === 0) return { status: 'info', details: 'LAIR e Lucro antes das participações zerados' }
      const calc = lair - ir, pct = diffPct(calc, lap)
      if (pct > TOL) return { status: 'warning', details: `LAIR−IR: ${fmtN(calc)} | Lucro antes das participações: ${fmtN(lap)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `LAIR: ${fmtN(lair)} | IR: ${fmtN(ir)} | Lucro antes das participações: ${fmtN(lap)}` }
  }},
  { label: 'DRE: Lucro Líquido = Lucro antes das participações − Participações − Minoritários', description: 'Para chegar ao LL, subtrair participações/gratificações estatutárias e participação de minoritários do Lucro antes das participações.', category: 'DRE', target_section: 'dre', check: (data) => {
      const lap = n(data, 'dre.lucro_antes_participacoes')
      const partGrat = n(data, 'dre.participacoes_gratificacoes_estatutarias')
      const partMin = n(data, 'dre.participacao_minoritarios')
      const ll = n(data, 'dre.lucro_liquido')
      if (lap === 0 && ll === 0) return { status: 'info', details: 'Lucro antes das participações e LL zerados' }
      const calc = lap - partGrat - partMin, pct = diffPct(calc, ll)
      if (pct > TOL) return { status: 'warning', details: `Calc: ${fmtN(calc)} | LL: ${fmtN(ll)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Lucro antes participações: ${fmtN(lap)} | Part/Grat: ${fmtN(partGrat)} | Minoritários: ${fmtN(partMin)} | LL: ${fmtN(ll)}` }
  }},
  { label: 'Disponibilidades não negativas', description: 'Caixa e Bancos não pode ser negativo.', category: 'Alertas', target_section: 'ativo', check: (data) => {
      const v = n(data, 'ativo_circulante.disponibilidades')
      if (v < -1) return { status: 'error', details: `Disponibilidades: ${fmtN(v)} — negativo` }
      return { status: 'ok', details: `Disponibilidades: ${fmtN(v)}` }
  }},
  { label: 'Receita Operacional Líquida ≥ 0', description: 'Após deduções, a ROL deve ser positiva.', category: 'Alertas', target_section: 'dre', check: (data) => {
      const rob = n(data, 'dre.receita_operacional_bruta')
      if (rob === 0) return { status: 'info', details: 'Receita Bruta zerada' }
      const rol = n(data, 'dre.receita_operacional_liquida')
      if (rol < -1) return { status: 'info', details: `ROB: ${fmtN(rob)} | ROL: ${fmtN(rol)} — ROL negativa` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)}` }
  }},
  { label: 'Patrimônio Líquido positivo', description: 'PL negativo indica insolvência técnica.', category: 'Alertas', target_section: 'passivo', check: (data) => {
      const pl = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (pl === 0) return { status: 'info', details: 'PL zerado' }
      if (pl < 0) return { status: 'info', details: `PL: ${fmtN(pl)} — negativo (insolvência técnica)` }
      return { status: 'ok', details: `PL: ${fmtN(pl)}` }
  }},
]

// ─── Props ─────────────────────────────────────────────────────────────────
interface Props {
  records: DataRecord[]
  onNavigate?: (section: TargetSection) => void
}

// ─── Helpers de render ─────────────────────────────────────────────────────
const SEVERITY_ORDER: Record<Status, number> = { error: 0, warning: 1, info: 3, ok: 2 }

function SeverityIcon({ status }: { status: Status }) {
  if (status === 'error')   return <IconX            size={14} className="text-red-600" />
  if (status === 'warning') return <IconAlertTriangle size={14} className="text-amber-600" />
  if (status === 'ok')      return <IconCheck         size={14} className="text-emerald-600" />
  return <span className="w-3.5 h-3.5 flex items-center justify-center text-gray-400 font-bold">–</span>
}

// ─── Componente ────────────────────────────────────────────────────────────
export default function PontosDeAtencao({ records, onNavigate }: Props) {
  const [filter, setFilter] = useState<Filter>('attention')

  const byRecord = useMemo(
    () => records.map(r => {
      const staticChecks = VALIDATIONS.map(v => ({ ...v, result: v.check(r.data) }))
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const pp: any[] = Array.isArray(r.data?._postprocessed) ? r.data._postprocessed : []
      const fonteWarnings: (Validation & { result: CheckResult })[] = pp
        .filter(w => w && w.tipo === 'aviso_fonte_inconsistente')
        .map(w => ({
          label: `Fonte inconsistente · ${w.campo}`,
          description: 'Soma dos itens registrados em fontes diverge do valor extraído do campo.',
          category: 'Fontes',
          target_section: (String(w.campo || '').startsWith('ativo') ? 'ativo'
                           : String(w.campo || '').startsWith('passivo') || String(w.campo || '').startsWith('patrimonio') ? 'passivo'
                           : String(w.campo || '').startsWith('dre') ? 'dre'
                           : 'identificacao') as TargetSection,
          check: () => ({ status: 'warning' as Status, details: '' }),
          result: {
            status: 'warning' as Status,
            details: `Valor do campo: ${fmtN(w.valor_campo)} | Soma fontes: ${fmtN(w.soma_fontes)} | Diferença: ${fmtN(w.diferenca)}`,
          },
        }))
      return { record: r, checks: [...staticChecks, ...fonteWarnings] }
    }),
    [records]
  )

  const allChecks = byRecord.flatMap(r => r.checks)
  const errorCount   = allChecks.filter(c => c.result.status === 'error').length
  const warningCount = allChecks.filter(c => c.result.status === 'warning').length
  const okCount      = allChecks.filter(c => c.result.status === 'ok').length
  const attentionCount = errorCount + warningCount
  const totalCount = allChecks.length  // inclui 'info'

  function passFilter(s: Status): boolean {
    if (filter === 'attention') return s === 'error' || s === 'warning'
    if (filter === 'all')       return true
    if (filter === 'error')     return s === 'error'
    if (filter === 'warning')   return s === 'warning'
    if (filter === 'ok')        return s === 'ok'
    return true
  }

  function recordLabel(r: DataRecord) {
    return `${r.tipo_entidade ?? 'INDIVIDUAL'} · ${r.periodo ? r.periodo.substring(0, 10) : '—'}`
  }

  const FILTERS: { key: Filter; label: string; count: number }[] = [
    { key: 'attention', label: 'Erros + Avisos', count: attentionCount },
    { key: 'all',       label: 'Todos',          count: totalCount },
    { key: 'error',     label: 'Erros',          count: errorCount },
    { key: 'warning',   label: 'Avisos',         count: warningCount },
    { key: 'ok',        label: 'OK',             count: okCount },
  ]

  return (
    <div className="space-y-4 max-w-3xl">
      {/* Header */}
      <div className="flex items-start justify-between pb-4 border-b border-gray-100">
        <div>
          <h2 className="text-base font-medium text-gray-900 leading-tight">Validações contábeis</h2>
          <p className="text-xs text-gray-500 mt-1">{VALIDATIONS.length} verificações sobre a estrutura do documento</p>
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="inline-flex items-center gap-1 text-red-600">
            <span className="w-1.5 h-1.5 rounded-full bg-red-500" />
            {errorCount} erro{errorCount !== 1 ? 's' : ''}
          </span>
          <span className="inline-flex items-center gap-1 text-amber-600">
            <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
            {warningCount} aviso{warningCount !== 1 ? 's' : ''}
          </span>
          <span className="inline-flex items-center gap-1 text-gray-400">
            <span className="w-1.5 h-1.5 rounded-full bg-gray-400" />
            {okCount} ok
          </span>
        </div>
      </div>

      {/* Filters */}
      <div className="flex gap-1.5 flex-wrap">
        {FILTERS.map(f => {
          const active = filter === f.key
          return (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              className={`text-[11px] px-2.5 py-1 rounded-md border font-medium transition-colors ${
                active
                  ? 'bg-white text-gray-900 border-gray-300 shadow-sm'
                  : 'bg-gray-50 text-gray-500 border-transparent hover:bg-gray-100 hover:text-gray-700'
              }`}
            >
              {f.label}
              {f.count > 0 && (
                <span className={`ml-1.5 tabular-nums ${active ? 'text-gray-500' : 'text-gray-400'}`}>{f.count}</span>
              )}
            </button>
          )
        })}
      </div>

      {/* Validations per record */}
      {byRecord.map(({ record, checks }, ri) => {
        const filtered = checks
          .filter(c => passFilter(c.result.status))
          .sort((a, b) => SEVERITY_ORDER[a.result.status] - SEVERITY_ORDER[b.result.status])

        if (filtered.length === 0) return null

        const recErrors   = checks.filter(c => c.result.status === 'error').length
        const recWarnings = checks.filter(c => c.result.status === 'warning').length

        return (
          <div key={ri}>
            {records.length > 1 && (
              <div className="flex items-center gap-2 px-3 py-1.5 mb-2 text-[11px] font-semibold text-gray-600 bg-gray-50 rounded-md">
                {recordLabel(record)}
                <span className="ml-auto font-normal text-gray-400">
                  {recErrors > 0 && <span className="text-red-600">{recErrors} erro{recErrors !== 1 ? 's' : ''}</span>}
                  {recErrors > 0 && recWarnings > 0 && ' · '}
                  {recWarnings > 0 && <span className="text-amber-600">{recWarnings} aviso{recWarnings !== 1 ? 's' : ''}</span>}
                </span>
              </div>
            )}

            <div className="space-y-1.5">
              {filtered.map((c, ci) => {
                const severity = c.result.status
                const borderColor = severity === 'error'   ? 'border-l-red-500'
                                  : severity === 'warning' ? 'border-l-amber-500'
                                  : severity === 'ok'      ? 'border-l-emerald-500'
                                                            : 'border-l-gray-300'
                const isOk = severity === 'ok' || severity === 'info'
                const clickable = !!onNavigate
                return (
                  <button
                    key={ci}
                    onClick={() => clickable && onNavigate(c.target_section)}
                    disabled={!clickable}
                    className={`group w-full text-left flex items-center gap-3 px-3 py-2.5 rounded-md bg-white border border-gray-100 border-l-2 ${borderColor} ${
                      isOk ? 'opacity-70' : ''
                    } ${clickable ? 'hover:bg-gray-50 hover:opacity-100 cursor-pointer' : ''} transition-colors`}
                    title={clickable ? `Ir para ${c.target_section}` : ''}
                  >
                    <SeverityIcon status={severity} />
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-medium text-gray-800 truncate">{c.label}</p>
                      <p className="text-[11px] text-gray-500 mt-0.5 truncate font-mono">{c.result.details}</p>
                    </div>
                    <span className="text-[10px] font-medium text-gray-500 bg-gray-100 px-2 py-0.5 rounded shrink-0">
                      {c.category}
                    </span>
                    {clickable && (
                      <IconChevronRight size={14} className="text-gray-300 group-hover:text-gray-500 transition-colors shrink-0" />
                    )}
                  </button>
                )
              })}
            </div>
          </div>
        )
      })}

      {/* Empty state */}
      {byRecord.every(({ checks }) => checks.filter(c => passFilter(c.result.status)).length === 0) && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <IconCheck size={28} className="text-emerald-500 mb-2" />
          {filter === 'attention' && <p className="text-sm text-gray-600">Todas as validações contábeis passaram</p>}
          {filter === 'error'     && <p className="text-sm text-gray-600">Nenhum erro encontrado</p>}
          {filter === 'warning'   && <p className="text-sm text-gray-600">Nenhum aviso encontrado</p>}
          {filter === 'ok'        && <p className="text-sm text-gray-600">Nenhuma validação passou ainda</p>}
          {filter === 'all'       && <p className="text-sm text-gray-600">Nenhuma validação disponível</p>}
        </div>
      )}
    </div>
  )
}
