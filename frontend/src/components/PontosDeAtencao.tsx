import { useMemo, useState } from 'react'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DataRecord = { tipo_entidade: string | null; periodo: string | null; data: any }

type Status = 'ok' | 'warning' | 'error' | 'info'
type Filter = 'all' | 'error' | 'warning' | 'ok'

interface CheckResult {
  status: Status
  details: string
}

interface Validation {
  label: string
  description: string
  category: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  check: (data: any) => CheckResult
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function n(data: any, path: string): number {
  const parts = path.split('.')
  let cur = data
  for (const p of parts) {
    if (cur == null) return 0
    cur = cur[p]
  }
  if (cur == null) return 0
  const v = parseFloat(String(cur))
  return isNaN(v) ? 0 : v
}

function fmtN(v: number): string {
  return new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 2 }).format(v)
}

function diffPct(a: number, b: number): number {
  const base = Math.max(Math.abs(a), Math.abs(b), 1)
  return (Math.abs(a - b) / base) * 100
}

const TOL = 0.01

const VALIDATIONS: Validation[] = [
  {
    label: 'Equação Contábil: Ativo Total = Passivo Total',
    description: 'O Ativo Total deve ser exatamente igual ao Passivo Total.',
    category: 'Balanço',
    check: (data) => {
      const ativo = n(data, 'ativo_total'), passivo = n(data, 'passivo_total')
      if (ativo === 0 && passivo === 0) return { status: 'info', details: 'Ativo e Passivo zerados — dados não extraídos?' }
      const pct = diffPct(ativo, passivo)
      if (pct > TOL) return { status: 'error', details: `Ativo: ${fmtN(ativo)} | Passivo: ${fmtN(passivo)} | Dif: ${fmtN(Math.abs(ativo - passivo))} (${pct.toFixed(2)}%)` }
      return { status: 'ok', details: `Ativo: ${fmtN(ativo)} = Passivo: ${fmtN(passivo)} ✓` }
    },
  },
  {
    label: 'Lucro Líquido → Lucros/Prejuízos Acumulados no PL',
    description: 'O resultado da DRE deve ser transferido para o PL.',
    category: 'Balanço',
    check: (data) => {
      const ll = n(data, 'dre.lucro_liquido')
      const lpa = n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados')
      const rl = n(data, 'patrimonio_liquido.reservas_de_lucro')
      // S.A.s alocam lucro em Reservas de Lucro, não em Lucros Acumulados — não alertar nesse caso
      if (Math.abs(ll) > 1 && Math.abs(lpa) < 1 && Math.abs(rl) < 1)
        return { status: 'warning', details: `LL: ${fmtN(ll)} | L/P Acumulados: ${fmtN(lpa)} | Reservas de Lucro: ${fmtN(rl)} — ambos zerados, verificar` }
      if (Math.abs(ll) > 1 && Math.abs(lpa) < 1 && Math.abs(rl) > 1)
        return { status: 'ok', details: `LL: ${fmtN(ll)} | Lucro alocado em Reservas de Lucro: ${fmtN(rl)} ✓` }
      return { status: 'ok', details: `LL: ${fmtN(ll)} | L/P Acumulados: ${fmtN(lpa)}` }
    },
  },
  {
    label: 'Ativo Total: AC + ANC + AP',
    description: 'Ativo Total deve ser a soma dos três grupos.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'ativo_total')
      if (total === 0) return { status: 'info', details: 'Ativo Total zerado' }
      const sum = n(data, 'ativo_circulante.total_ativo_circulante') + n(data, 'ativo_nao_circulante.total_ativo_nao_circulante') + n(data, 'ativo_permanente.total_ativo_permanente')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'error', details: `AC+ANC+AP: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `AC+ANC+AP = ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Passivo Total: PC + PNC + PL',
    description: 'Passivo Total deve ser a soma dos três grupos.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'passivo_total')
      if (total === 0) return { status: 'info', details: 'Passivo Total zerado' }
      const sum = n(data, 'passivo_circulante.total_passivo_circulante') + n(data, 'passivo_nao_circulante.total_passivo_nao_circulante') + n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'error', details: `PC+PNC+PL: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `PC+PNC+PL = ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Ativo Circulante',
    description: 'Soma dos itens do AC deve ser igual ao total.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'ativo_circulante.total_ativo_circulante')
      if (total === 0) return { status: 'info', details: 'AC zerado' }
      const sum = n(data, 'ativo_circulante.disponibilidades') + n(data, 'ativo_circulante.titulos_a_receber') + n(data, 'ativo_circulante.estoques') + n(data, 'ativo_circulante.adiantamentos') + n(data, 'ativo_circulante.impostos_a_recuperar') + n(data, 'ativo_circulante.outros_ativos_circulantes') + n(data, 'ativo_circulante.conta_corrente_socios_control_colig') + n(data, 'ativo_circulante.outros_ativos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total AC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total AC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Passivo Circulante',
    description: 'Soma dos itens do PC deve ser igual ao total.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'passivo_circulante.total_passivo_circulante')
      if (total === 0) return { status: 'info', details: 'PC zerado' }
      const sum = n(data, 'passivo_circulante.fornecedores') + n(data, 'passivo_circulante.financiamentos_com_instituicoes_de_credito') + n(data, 'passivo_circulante.salarios_contribuicoes') + n(data, 'passivo_circulante.tributos') + n(data, 'passivo_circulante.adiantamentos') + n(data, 'passivo_circulante.conta_corrente_socios_coligadas_controladas') + n(data, 'passivo_circulante.outros_passivos_circulante') + n(data, 'passivo_circulante.provisoes') + n(data, 'passivo_circulante.outros_passivos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Patrimônio Líquido',
    description: 'Soma dos componentes do PL deve ser igual ao total.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (total === 0) return { status: 'info', details: 'PL zerado' }
      const sum = n(data, 'patrimonio_liquido.capital_social') + n(data, 'patrimonio_liquido.reserva_de_capital') + n(data, 'patrimonio_liquido.reservas_de_lucro') + n(data, 'patrimonio_liquido.reservas_de_reavaliacao') + n(data, 'patrimonio_liquido.outras_reservas') + n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados') + n(data, 'patrimonio_liquido.acoes_em_tesouraria')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PL: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PL: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Ativo Não Circulante',
    description: 'Soma dos itens do ANC deve ser igual ao total.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'ativo_nao_circulante.total_ativo_nao_circulante')
      if (total === 0) return { status: 'info', details: 'ANC zerado' }
      const sum = n(data, 'ativo_nao_circulante.titulos_a_receber') + n(data, 'ativo_nao_circulante.estoques') + n(data, 'ativo_nao_circulante.adiantamentos') + n(data, 'ativo_nao_circulante.impostos_a_recuperar') + n(data, 'ativo_nao_circulante.despesas_pagas_antecipadamente') + n(data, 'ativo_nao_circulante.conta_corrente_socios_control_colig') + n(data, 'ativo_nao_circulante.outros_realizavel_a_longo_prazo')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total ANC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total ANC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Ativo Permanente',
    description: 'Investimentos + Imobilizado + Intangível = Total AP.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'ativo_permanente.total_ativo_permanente')
      if (total === 0) return { status: 'info', details: 'AP zerado' }
      const sum = n(data, 'ativo_permanente.investimentos') + n(data, 'ativo_permanente.imobilizado') + n(data, 'ativo_permanente.intangivel_diferido')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total AP: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total AP: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Passivo Não Circulante',
    description: 'Soma dos itens do PNC deve ser igual ao total.',
    category: 'Balanço',
    check: (data) => {
      const total = n(data, 'passivo_nao_circulante.total_passivo_nao_circulante')
      if (total === 0) return { status: 'info', details: 'PNC zerado' }
      const sum = n(data, 'passivo_nao_circulante.fornecedores') + n(data, 'passivo_nao_circulante.financiamentos_com_instituicoes_de_credito') + n(data, 'passivo_nao_circulante.salarios_contribuicoes') + n(data, 'passivo_nao_circulante.tributos') + n(data, 'passivo_nao_circulante.adiantamentos') + n(data, 'passivo_nao_circulante.conta_corrente_socios_coligadas_controladas') + n(data, 'passivo_nao_circulante.outros_passivos_nao_circulantes') + n(data, 'passivo_nao_circulante.provisoes')
      const pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Total PNC: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Total PNC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'DRE: Deduções = Vendas Anuladas + Abatimentos + Impostos',
    description: 'Total de deduções deve ser a soma dos sub-itens.',
    category: 'DRE',
    check: (data) => {
      const ded = n(data, 'dre.total_deducoes')
      if (ded === 0) return { status: 'info', details: 'Deduções zeradas' }
      const va = n(data, 'dre.vendas_anuladas'), ab = n(data, 'dre.abatimentos'), imp = n(data, 'dre.impostos_incidentes_sobre_vendas')
      if (va === 0 && ab === 0 && imp === 0) return { status: 'info', details: 'Sub-itens zerados (não detalhado)' }
      const sum = va + ab + imp, pct = diffPct(ded, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | Deduções: ${fmtN(ded)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Deduções: ${fmtN(ded)} ✓` }
    },
  },
  {
    label: 'DRE: ROL = ROB − Deduções + Incentivos',
    description: 'Receita Líquida deve ser ROB menos deduções mais incentivos.',
    category: 'DRE',
    check: (data) => {
      const rol = n(data, 'dre.receita_operacional_liquida')
      if (rol === 0) return { status: 'info', details: 'ROL zerada' }
      const rob = n(data, 'dre.receita_operacional_bruta'), ded = n(data, 'dre.total_deducoes'), inc = n(data, 'dre.incentivos_a_exportacoes')
      const calc = rob - ded + inc, pct = diffPct(calc, rol)
      if (pct > TOL) return { status: 'warning', details: `ROB-Ded+Inc: ${fmtN(calc)} | ROL: ${fmtN(rol)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)} ✓` }
    },
  },
  {
    label: 'DRE: Lucro Bruto = ROL − CMV',
    description: 'Receita Líquida menos Custo = Lucro Bruto.',
    category: 'DRE',
    check: (data) => {
      const rol = n(data, 'dre.receita_operacional_liquida'), cmv = n(data, 'dre.custo_servicos_produtos_mercadorias_vendidas'), lb = n(data, 'dre.lucro_bruto')
      if (rol === 0 && lb === 0) return { status: 'info', details: 'ROL e Lucro Bruto zerados' }
      const calc = rol - cmv, pct = diffPct(calc, lb)
      if (pct > TOL) return { status: 'warning', details: `ROL-CMV: ${fmtN(calc)} | LB: ${fmtN(lb)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)} | CMV: ${fmtN(cmv)} | LB: ${fmtN(lb)} ✓` }
    },
  },
  {
    label: 'DRE: EBIT = Lucro Bruto − Despesas Operacionais',
    description: 'Lucro Operacional deve ser LB menos despesas operacionais.',
    category: 'DRE',
    check: (data) => {
      const lb = n(data, 'dre.lucro_bruto'), desp = n(data, 'dre.total_despesas_operacionais'), lo = n(data, 'dre.lucro_operacional')
      if (lb === 0 && lo === 0) return { status: 'info', details: 'LB e EBIT zerados' }
      const calc = lb - desp, pct = diffPct(calc, lo)
      if (pct > TOL) return { status: 'warning', details: `LB-DespOp: ${fmtN(calc)} | EBIT: ${fmtN(lo)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `LB: ${fmtN(lb)} | DespOp: ${fmtN(desp)} | EBIT: ${fmtN(lo)} ✓` }
    },
  },
  {
    label: 'DRE: Despesas Financeiras = Encargos + Descontos + Variação Cambial',
    description: 'Total de despesas financeiras deve ser a soma dos sub-itens.',
    category: 'DRE',
    check: (data) => {
      const total = n(data, 'dre.despesas_financeiras')
      if (total === 0) return { status: 'info', details: 'Despesas Financeiras zeradas' }
      const enc = n(data, 'dre.encargos_financeiros'), desc = n(data, 'dre.descontos_concedidos'), vc = n(data, 'dre.variacao_cambial_nao_paga')
      if (enc === 0 && desc === 0 && vc === 0) return { status: 'info', details: 'Sub-itens zerados (não detalhado)' }
      const sum = enc + desc + vc, pct = diffPct(total, sum)
      if (pct > TOL) return { status: 'warning', details: `Soma: ${fmtN(sum)} | DespFin: ${fmtN(total)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `Despesas Financeiras: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'DRE: LAIR = EBIT + Resultado Financeiro + Equivalência',
    description: 'LAIR deve ser EBIT mais/menos resultado financeiro e equivalência patrimonial.',
    category: 'DRE',
    check: (data) => {
      const lo = n(data, 'dre.lucro_operacional'), lair = n(data, 'dre.lucro_antes_imposto_de_renda')
      if (lair === 0) return { status: 'info', details: 'LAIR zerado' }
      const lf = n(data, 'dre.lucro_financeiro')
      const ep = n(data, 'dre.resultado_de_equivalencia_patrimonial')
      const rno = n(data, 'dre.receita_nao_operacional')
      const dno = n(data, 'dre.despesa_nao_operacional')
      const scm = n(data, 'dre.saldo_correcao_monetaria')
      const raa = n(data, 'dre.resultado_alienacao_ativos')
      const rf = lf - lo // resultado financeiro líquido
      const calc = lo + rf + ep + rno - dno + scm + raa
      const pct = diffPct(calc, lair)
      if (pct > TOL) return { status: 'warning', details: `Calculado: ${fmtN(calc)} | LAIR: ${fmtN(lair)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `EBIT: ${fmtN(lo)} | RF: ${fmtN(rf)} | EP: ${fmtN(ep)} | LAIR: ${fmtN(lair)} ✓` }
    },
  },
  {
    label: 'DRE: Lucro Líquido = LAIR − IRPJ/CSLL',
    description: 'LAIR menos impostos deve ser o Lucro Líquido.',
    category: 'DRE',
    check: (data) => {
      const lair = n(data, 'dre.lucro_antes_imposto_de_renda'), ir = n(data, 'dre.provisao_imposto_de_renda') + n(data, 'dre.csll'), ll = n(data, 'dre.lucro_liquido')
      if (lair === 0 && ll === 0) return { status: 'info', details: 'LAIR e LL zerados' }
      const calc = lair - ir, pct = diffPct(calc, ll)
      if (pct > TOL) return { status: 'warning', details: `LAIR-IR: ${fmtN(calc)} | LL: ${fmtN(ll)} | Dif: ${pct.toFixed(2)}%` }
      return { status: 'ok', details: `LAIR: ${fmtN(lair)} | IR: ${fmtN(ir)} | LL: ${fmtN(ll)} ✓` }
    },
  },
  {
    label: 'Disponibilidades não negativas',
    description: 'Caixa e Bancos não pode ser negativo.',
    category: 'Alertas',
    check: (data) => {
      const v = n(data, 'ativo_circulante.disponibilidades')
      if (v < -1) return { status: 'error', details: `Disponibilidades: ${fmtN(v)} — negativo` }
      return { status: 'ok', details: `Disponibilidades: ${fmtN(v)} ✓` }
    },
  },
  {
    label: 'Receita Operacional Líquida ≥ 0',
    description: 'Após deduções, a ROL deve ser positiva.',
    category: 'Alertas',
    check: (data) => {
      const rob = n(data, 'dre.receita_operacional_bruta')
      if (rob === 0) return { status: 'info', details: 'Receita Bruta zerada' }
      const rol = n(data, 'dre.receita_operacional_liquida')
      if (rol < -1) return { status: 'info', details: `ROB: ${fmtN(rob)} | ROL: ${fmtN(rol)} — ROL negativa` }
      return { status: 'ok', details: `ROL: ${fmtN(rol)} ✓` }
    },
  },
  {
    label: 'Patrimônio Líquido positivo',
    description: 'PL negativo indica insolvência técnica.',
    category: 'Alertas',
    check: (data) => {
      const pl = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (pl === 0) return { status: 'info', details: 'PL zerado' }
      if (pl < 0) return { status: 'info', details: `PL: ${fmtN(pl)} — negativo (insolvência técnica)` }
      return { status: 'ok', details: `PL: ${fmtN(pl)} ✓` }
    },
  },
]

// Score ring SVG
function ScoreRing({ score, size = 72 }: { score: number; size?: number }) {
  const r = size * 0.34
  const cx = size / 2, cy = size / 2
  const circumference = 2 * Math.PI * r
  const progress = Math.max(0, Math.min(1, score / 100)) * circumference
  const color = score >= 80 ? '#10b981' : score >= 60 ? '#f59e0b' : '#ef4444'
  const trackColor = score >= 80 ? '#d1fae5' : score >= 60 ? '#fef3c7' : '#fee2e2'

  return (
    <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
      <circle cx={cx} cy={cy} r={r} fill="none" stroke={trackColor} strokeWidth={size * 0.1} />
      <circle
        cx={cx} cy={cy} r={r}
        fill="none"
        stroke={color}
        strokeWidth={size * 0.1}
        strokeDasharray={`${progress} ${circumference}`}
        strokeLinecap="round"
      />
    </svg>
  )
}

const STATUS_ICON = {
  ok:      { icon: '✓', bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', dot: 'bg-emerald-400' },
  warning: { icon: '!', bg: 'bg-amber-50',   border: 'border-amber-200',   text: 'text-amber-700',   dot: 'bg-amber-400' },
  error:   { icon: '✕', bg: 'bg-red-50',     border: 'border-red-200',     text: 'text-red-700',     dot: 'bg-red-400' },
  info:    { icon: '–', bg: 'bg-gray-50',    border: 'border-gray-200',    text: 'text-gray-500',    dot: 'bg-gray-300' },
}

interface Props {
  records: DataRecord[]
}

export default function PontosDeAtencao({ records }: Props) {
  const [filter, setFilter] = useState<Filter>('all')
  const [openItems, setOpenItems] = useState<Set<number>>(new Set())

  const byRecord = useMemo(
    () => records.map(r => ({
      record: r,
      checks: VALIDATIONS.map(v => ({ ...v, result: v.check(r.data) })),
    })),
    [records]
  )

  const allChecks = byRecord.flatMap(r => r.checks)
  const totalChecks = allChecks.filter(c => c.result.status !== 'info').length
  const errorCount   = allChecks.filter(c => c.result.status === 'error').length
  const warningCount = allChecks.filter(c => c.result.status === 'warning').length
  const okCount      = allChecks.filter(c => c.result.status === 'ok').length
  const infoCount    = allChecks.filter(c => c.result.status === 'info').length
  const score = totalChecks > 0 ? Math.round((okCount / totalChecks) * 100) : 100

  function toggleItem(idx: number) {
    setOpenItems(prev => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }

  function recordLabel(r: DataRecord) {
    return `${r.tipo_entidade ?? 'INDIVIDUAL'} · ${r.periodo ? r.periodo.substring(0, 10) : '—'}`
  }

  const scoreColor = score >= 80 ? 'text-emerald-700' : score >= 60 ? 'text-amber-700' : 'text-red-700'
  const scoreLabel = score >= 80 ? 'Ótimo' : score >= 60 ? 'Atenção' : 'Crítico'

  return (
    <div className="space-y-5 max-w-3xl">
      {/* Score dashboard */}
      <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-5">
        <div className="flex items-center gap-6">
          {/* Ring */}
          <div className="relative shrink-0">
            <ScoreRing score={score} size={80} />
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <span className={`text-lg font-bold leading-tight ${scoreColor}`}>{score}%</span>
            </div>
          </div>

          {/* Score details */}
          <div className="flex-1 min-w-0">
            <div className="flex items-baseline gap-2 mb-3">
              <h3 className={`text-base font-bold ${scoreColor}`}>{scoreLabel}</h3>
              <span className="text-xs text-gray-400">{okCount} de {totalChecks} validações passaram</span>
            </div>
            <div className="flex gap-3 flex-wrap">
              <div className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-red-400" />
                <span className="text-xs text-gray-600"><span className="font-semibold text-red-700">{errorCount}</span> erro{errorCount !== 1 ? 's' : ''}</span>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-amber-400" />
                <span className="text-xs text-gray-600"><span className="font-semibold text-amber-700">{warningCount}</span> aviso{warningCount !== 1 ? 's' : ''}</span>
              </div>
              <div className="flex items-center gap-1.5">
                <span className="w-2 h-2 rounded-full bg-emerald-400" />
                <span className="text-xs text-gray-600"><span className="font-semibold text-emerald-700">{okCount}</span> ok</span>
              </div>
              {infoCount > 0 && (
                <div className="flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full bg-gray-300" />
                  <span className="text-xs text-gray-400">{infoCount} sem dados</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex gap-1.5">
        {(['all', 'error', 'warning', 'ok'] as const).map(f => {
          const labels = { all: 'Todos', error: 'Erros', warning: 'Avisos', ok: 'OK' }
          const counts = { all: allChecks.length, error: errorCount, warning: warningCount, ok: okCount }
          const active = filter === f
          return (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`text-xs px-3 py-1.5 rounded-lg border font-medium transition-all ${
                active
                  ? f === 'error'   ? 'bg-red-600 text-white border-red-600'
                  : f === 'warning' ? 'bg-amber-500 text-white border-amber-500'
                  : f === 'ok'      ? 'bg-emerald-600 text-white border-emerald-600'
                  : 'bg-[#0F2137] text-white border-[#0F2137]'
                  : 'bg-white text-gray-600 border-gray-200 hover:border-gray-300 hover:bg-gray-50'
              }`}
            >
              {labels[f]}
              {counts[f] > 0 && <span className={`ml-1.5 ${active ? 'text-white/70' : 'text-gray-400'}`}>{counts[f]}</span>}
            </button>
          )
        })}
      </div>

      {/* Validations per record */}
      {byRecord.map(({ record, checks }, ri) => {
        const filtered = checks.filter(c => {
          if (filter === 'all') return true
          if (filter === 'error') return c.result.status === 'error'
          if (filter === 'warning') return c.result.status === 'warning'
          if (filter === 'ok') return c.result.status === 'ok'
          return true
        })

        if (filtered.length === 0) return null

        const recErrors   = checks.filter(c => c.result.status === 'error').length
        const recWarnings = checks.filter(c => c.result.status === 'warning').length

        return (
          <div key={ri}>
            {records.length > 1 && (
              <div className={`flex items-center gap-2 px-3 py-2 rounded-lg mb-2 text-xs font-semibold ${
                recErrors > 0 ? 'bg-red-50 text-red-700' : recWarnings > 0 ? 'bg-amber-50 text-amber-700' : 'bg-emerald-50 text-emerald-700'
              }`}>
                {recordLabel(record)}
                <span className="ml-auto font-normal text-inherit/70">
                  {recErrors > 0 ? `${recErrors} erro${recErrors !== 1 ? 's' : ''}` : ''}
                  {recErrors > 0 && recWarnings > 0 ? ' · ' : ''}
                  {recWarnings > 0 ? `${recWarnings} aviso${recWarnings !== 1 ? 's' : ''}` : ''}
                </span>
              </div>
            )}

            <div className="space-y-1.5">
              {filtered.map((c, ci) => {
                const globalIdx = ri * VALIDATIONS.length + ci
                const isOpen = openItems.has(globalIdx)
                const cfg = STATUS_ICON[c.result.status]

                return (
                  <div key={ci} className={`border ${cfg.border} rounded-xl overflow-hidden`}>
                    <button
                      onClick={() => toggleItem(globalIdx)}
                      className={`w-full flex items-center gap-3 px-4 py-3 text-left ${cfg.bg} hover:brightness-95 transition-all`}
                    >
                      {/* Status dot */}
                      <span className={`w-5 h-5 rounded-full ${cfg.bg} border ${cfg.border} flex items-center justify-center shrink-0`}>
                        <span className={`text-[10px] font-bold ${cfg.text}`}>{cfg.icon}</span>
                      </span>

                      <div className="flex-1 min-w-0">
                        <p className={`text-xs font-semibold ${cfg.text} truncate`}>{c.label}</p>
                        <p className="text-[10px] text-gray-500 mt-0.5 truncate">{c.result.details}</p>
                      </div>

                      <div className="flex items-center gap-2 shrink-0">
                        <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full border ${cfg.border} ${cfg.text} ${cfg.bg}`}>
                          {c.category}
                        </span>
                        <svg
                          className={`w-3.5 h-3.5 ${cfg.text} transition-transform duration-150 ${isOpen ? 'rotate-180' : ''}`}
                          fill="none" stroke="currentColor" viewBox="0 0 24 24"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                        </svg>
                      </div>
                    </button>

                    {isOpen && (
                      <div className="px-4 py-3 bg-white border-t border-gray-100 animate-fade-in">
                        <p className="text-xs text-gray-600 leading-relaxed">{c.description}</p>
                        <p className={`text-xs font-mono mt-2 ${cfg.text}`}>{c.result.details}</p>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}
