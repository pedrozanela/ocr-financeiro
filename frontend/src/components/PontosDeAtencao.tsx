import { useMemo } from 'react'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DataRecord = { tipo_entidade: string | null; periodo: string | null; data: any }

type Status = 'ok' | 'warning' | 'error' | 'info'

interface CheckResult {
  status: Status
  details: string
}

interface Validation {
  label: string
  description: string
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

const TOL = 1.0 // 1% tolerance

const VALIDATIONS: Validation[] = [
  {
    label: 'Equação Contábil Fundamental: Ativo Total = Passivo Total',
    description:
      'O Ativo Total deve ser exatamente igual ao Passivo Total (que já inclui Passivo Circulante, Não Circulante e Patrimônio Líquido). Diferença indica erro de extração ou inconsistência na demonstração.',
    check: (data) => {
      const ativo = n(data, 'ativo_total')
      const passivo = n(data, 'passivo_total')
      if (ativo === 0 && passivo === 0)
        return { status: 'info', details: 'Ativo e Passivo zerados — dados não extraídos?' }
      const pct = diffPct(ativo, passivo)
      const diff = Math.abs(ativo - passivo)
      if (pct > TOL)
        return {
          status: 'error',
          details: `Ativo: ${fmtN(ativo)} | Passivo: ${fmtN(passivo)} | Diferença: ${fmtN(diff)} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `Ativo: ${fmtN(ativo)} | Passivo: ${fmtN(passivo)} ✓` }
    },
  },
  {
    label: 'Lucro Líquido → Lucros/Prejuízos Acumulados no Patrimônio Líquido',
    description:
      'O resultado final da DRE (Lucro ou Prejuízo Líquido) deve obrigatoriamente ser transferido para a linha de Lucros ou Prejuízos Acumulados no Patrimônio Líquido. Se o Lucro Líquido ≠ 0 e esta linha estiver zerada, provavelmente houve falha na extração.',
    check: (data) => {
      const ll = n(data, 'dre.lucro_liquido')
      const lpa = n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados')
      if (Math.abs(ll) > 1 && Math.abs(lpa) < 1)
        return {
          status: 'warning',
          details: `Lucro Líquido: ${fmtN(ll)} | Lucros/Prej Acumulados no PL: ${fmtN(lpa)} — linha zerada, verificar`,
        }
      return {
        status: 'ok',
        details: `Lucro Líquido: ${fmtN(ll)} | Lucros/Prej Acumulados: ${fmtN(lpa)}`,
      }
    },
  },
  {
    label: 'Consistência do Ativo Total: AC + ANC + AP = Ativo Total',
    description:
      'Ativo Total deve ser igual à soma do Ativo Circulante, Ativo Não Circulante e Ativo Permanente.',
    check: (data) => {
      const total = n(data, 'ativo_total')
      if (total === 0) return { status: 'info', details: 'Ativo Total zerado' }
      const ac = n(data, 'ativo_circulante.total_ativo_circulante')
      const anc = n(data, 'ativo_nao_circulante.total_ativo_nao_circulante')
      const ap = n(data, 'ativo_permanente.total_ativo_permanente')
      const sum = ac + anc + ap
      const pct = diffPct(total, sum)
      if (pct > TOL)
        return {
          status: 'error',
          details: `AC+ANC+AP: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${fmtN(Math.abs(total - sum))} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `AC: ${fmtN(ac)} + ANC: ${fmtN(anc)} + AP: ${fmtN(ap)} = ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência do Passivo Total: PC + PNC + PL = Passivo Total',
    description:
      'Passivo Total deve ser igual à soma do Passivo Circulante, Passivo Não Circulante e Patrimônio Líquido.',
    check: (data) => {
      const total = n(data, 'passivo_total')
      if (total === 0) return { status: 'info', details: 'Passivo Total zerado' }
      const pc = n(data, 'passivo_circulante.total_passivo_circulante')
      const pnc = n(data, 'passivo_nao_circulante.total_passivo_nao_circulante')
      const pl = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      const sum = pc + pnc + pl
      const pct = diffPct(total, sum)
      if (pct > TOL)
        return {
          status: 'error',
          details: `PC+PNC+PL: ${fmtN(sum)} | Total: ${fmtN(total)} | Dif: ${fmtN(Math.abs(total - sum))} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `PC: ${fmtN(pc)} + PNC: ${fmtN(pnc)} + PL: ${fmtN(pl)} = ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Ativo Circulante',
    description:
      'A soma dos itens do Ativo Circulante deve ser aproximadamente igual ao seu total.',
    check: (data) => {
      const total = n(data, 'ativo_circulante.total_ativo_circulante')
      if (total === 0) return { status: 'info', details: 'Ativo Circulante zerado' }
      const sum =
        n(data, 'ativo_circulante.disponibilidades') +
        n(data, 'ativo_circulante.titulos_a_receber') +
        n(data, 'ativo_circulante.estoques') +
        n(data, 'ativo_circulante.adiantamentos') +
        n(data, 'ativo_circulante.impostos_a_recuperar') +
        n(data, 'ativo_circulante.outros_ativos_circulantes') +
        n(data, 'ativo_circulante.conta_corrente_socios_control_colig') +
        n(data, 'ativo_circulante.outros_ativos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `Soma itens: ${fmtN(sum)} | Total AC: ${fmtN(total)} | Dif: ${fmtN(Math.abs(total - sum))} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `Total AC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Passivo Circulante',
    description:
      'A soma dos itens do Passivo Circulante deve ser aproximadamente igual ao seu total.',
    check: (data) => {
      const total = n(data, 'passivo_circulante.total_passivo_circulante')
      if (total === 0) return { status: 'info', details: 'Passivo Circulante zerado' }
      const sum =
        n(data, 'passivo_circulante.fornecedores') +
        n(data, 'passivo_circulante.financiamentos_com_instituicoes_de_credito') +
        n(data, 'passivo_circulante.salarios_contribuicoes') +
        n(data, 'passivo_circulante.tributos') +
        n(data, 'passivo_circulante.adiantamentos') +
        n(data, 'passivo_circulante.conta_corrente_socios_coligadas_controladas') +
        n(data, 'passivo_circulante.outros_passivos_circulante') +
        n(data, 'passivo_circulante.provisoes') +
        n(data, 'passivo_circulante.outros_passivos_financeiros')
      const pct = diffPct(total, sum)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `Soma itens: ${fmtN(sum)} | Total PC: ${fmtN(total)} | Dif: ${fmtN(Math.abs(total - sum))} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `Total PC: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'Consistência interna do Patrimônio Líquido',
    description:
      'A soma dos componentes do Patrimônio Líquido deve ser igual ao seu total.',
    check: (data) => {
      const total = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (total === 0) return { status: 'info', details: 'Patrimônio Líquido zerado' }
      const sum =
        n(data, 'patrimonio_liquido.capital_social') +
        n(data, 'patrimonio_liquido.reserva_de_capital') +
        n(data, 'patrimonio_liquido.reservas_de_lucro') +
        n(data, 'patrimonio_liquido.reservas_de_reavaliacao') +
        n(data, 'patrimonio_liquido.outras_reservas') +
        n(data, 'patrimonio_liquido.lucros_ou_prejuizos_acumulados') +
        n(data, 'patrimonio_liquido.acoes_em_tesouraria')
      const pct = diffPct(total, sum)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `Soma itens: ${fmtN(sum)} | Total PL: ${fmtN(total)} | Dif: ${fmtN(Math.abs(total - sum))} (${pct.toFixed(2)}%)`,
        }
      return { status: 'ok', details: `Total PL: ${fmtN(total)} ✓` }
    },
  },
  {
    label: 'DRE: Lucro Bruto = ROL − CMV',
    description:
      'A Receita Operacional Líquida menos o Custo dos Serviços/Produtos/Mercadorias Vendidos deve ser igual ao Lucro Bruto.',
    check: (data) => {
      const rol = n(data, 'dre.receita_operacional_liquida')
      const cmv = n(data, 'dre.custo_servicos_produtos_mercadorias_vendidas')
      const lb = n(data, 'dre.lucro_bruto')
      if (rol === 0 && lb === 0) return { status: 'info', details: 'ROL e Lucro Bruto zerados' }
      const calc = rol - cmv
      const pct = diffPct(calc, lb)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `ROL: ${fmtN(rol)} − CMV: ${fmtN(cmv)} = ${fmtN(calc)} | LB extraído: ${fmtN(lb)} | Dif: ${pct.toFixed(2)}%`,
        }
      return { status: 'ok', details: `ROL: ${fmtN(rol)} | CMV: ${fmtN(cmv)} | Lucro Bruto: ${fmtN(lb)} ✓` }
    },
  },
  {
    label: 'DRE: Lucro Operacional (EBIT) = Lucro Bruto − Despesas Operacionais',
    description:
      'O Lucro Bruto menos o Total de Despesas Operacionais deve ser igual ao Lucro Operacional (EBIT).',
    check: (data) => {
      const lb = n(data, 'dre.lucro_bruto')
      const desp = n(data, 'dre.total_despesas_operacionais')
      const lo = n(data, 'dre.lucro_operacional')
      if (lb === 0 && lo === 0) return { status: 'info', details: 'Lucro Bruto e EBIT zerados' }
      const calc = lb - desp
      const pct = diffPct(calc, lo)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `LB: ${fmtN(lb)} − Desp Op: ${fmtN(desp)} = ${fmtN(calc)} | EBIT extraído: ${fmtN(lo)} | Dif: ${pct.toFixed(2)}%`,
        }
      return { status: 'ok', details: `Lucro Bruto: ${fmtN(lb)} | EBIT: ${fmtN(lo)} ✓` }
    },
  },
  {
    label: 'DRE: LAIR = EBIT + Resultado Financeiro + Equivalência Patrimonial',
    description:
      'O Lucro Antes do Imposto de Renda deve ser igual à soma do EBIT, do Resultado Financeiro e da Equivalência Patrimonial.',
    check: (data) => {
      const lo = n(data, 'dre.lucro_operacional')
      const rf = n(data, 'dre.lucro_financeiro') - n(data, 'dre.lucro_operacional')
      const ep = n(data, 'dre.resultado_de_equivalencia_patrimonial')
      const lair = n(data, 'dre.lucro_antes_imposto_de_renda')
      if (lair === 0) return { status: 'info', details: 'LAIR zerado' }
      const calc = lo + rf + ep
      const pct = diffPct(calc, lair)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `EBIT+RF+EP: ${fmtN(calc)} | LAIR extraído: ${fmtN(lair)} | Dif: ${pct.toFixed(2)}%`,
        }
      return { status: 'ok', details: `EBIT: ${fmtN(lo)} | RF: ${fmtN(rf)} | EP: ${fmtN(ep)} | LAIR: ${fmtN(lair)} ✓` }
    },
  },
  {
    label: 'DRE: Lucro Líquido = LAIR − IRPJ/CSLL',
    description:
      'O Lucro Líquido deve ser igual ao LAIR menos a Provisão para IRPJ e CSLL.',
    check: (data) => {
      const lair = n(data, 'dre.lucro_antes_imposto_de_renda')
      const ir = n(data, 'dre.provisao_imposto_de_renda') + n(data, 'dre.csll')
      const ll = n(data, 'dre.lucro_liquido')
      if (lair === 0 && ll === 0) return { status: 'info', details: 'LAIR e Lucro Líquido zerados' }
      const calc = lair - ir
      const pct = diffPct(calc, ll)
      if (pct > TOL)
        return {
          status: 'warning',
          details: `LAIR: ${fmtN(lair)} − IRPJ/CSLL: ${fmtN(ir)} = ${fmtN(calc)} | LL extraído: ${fmtN(ll)} | Dif: ${pct.toFixed(2)}%`,
        }
      return { status: 'ok', details: `LAIR: ${fmtN(lair)} | IRPJ/CSLL: ${fmtN(ir)} | Lucro Líquido: ${fmtN(ll)} ✓` }
    },
  },
  {
    label: 'Disponibilidades não negativas',
    description:
      'O saldo de Disponibilidades (Caixa e Bancos) não pode ser negativo em um Balanço Patrimonial. Valor negativo indica erro de extração.',
    check: (data) => {
      const v = n(data, 'ativo_circulante.disponibilidades')
      if (v < -1)
        return { status: 'error', details: `Disponibilidades: ${fmtN(v)} — valor negativo` }
      return { status: 'ok', details: `Disponibilidades: ${fmtN(v)} ✓` }
    },
  },
  {
    label: 'Receita Operacional Líquida ≥ 0',
    description:
      'Após as deduções (impostos, devoluções), a Receita Operacional Líquida deve ser positiva. Valor negativo indica que as deduções extraídas estão maiores que a receita bruta.',
    check: (data) => {
      const rob = n(data, 'dre.receita_operacional_bruta')
      if (rob === 0) return { status: 'info', details: 'Receita Bruta zerada' }
      const rol = n(data, 'dre.receita_operacional_liquida')
      if (rol < -1)
        return {
          status: 'warning',
          details: `ROB: ${fmtN(rob)} | ROL: ${fmtN(rol)} — verifique as deduções`,
        }
      return { status: 'ok', details: `ROB: ${fmtN(rob)} | ROL: ${fmtN(rol)} ✓` }
    },
  },
  {
    label: 'Patrimônio Líquido positivo',
    description:
      'PL negativo indica situação de insolvência técnica ou prejuízos acumulados superiores ao capital. Deve ser reportado como ponto de atenção.',
    check: (data) => {
      const pl = n(data, 'patrimonio_liquido.total_patrimonio_liquido')
      if (pl === 0) return { status: 'info', details: 'PL zerado — dados não extraídos?' }
      if (pl < 0)
        return {
          status: 'warning',
          details: `PL: ${fmtN(pl)} — negativo (insolvência técnica)`,
        }
      return { status: 'ok', details: `PL: ${fmtN(pl)} ✓` }
    },
  },
]

const STATUS_CONFIG: Record<Status, { bg: string; border: string; text: string; icon: string; badge: string }> = {
  ok:      { bg: 'bg-green-50',  border: 'border-green-200', text: 'text-green-700', icon: '✓', badge: 'bg-green-100 text-green-700' },
  warning: { bg: 'bg-amber-50',  border: 'border-amber-200', text: 'text-amber-700', icon: '⚠', badge: 'bg-amber-100 text-amber-700' },
  error:   { bg: 'bg-red-50',    border: 'border-red-200',   text: 'text-red-700',   icon: '✗', badge: 'bg-red-100 text-red-700' },
  info:    { bg: 'bg-gray-50',   border: 'border-gray-200',  text: 'text-gray-500',  icon: '–', badge: 'bg-gray-100 text-gray-500' },
}

interface Props {
  records: DataRecord[]
}

export default function PontosDeAtencao({ records }: Props) {
  // For each record, compute all validation results
  const byRecord = useMemo(
    () => records.map(r => ({
      record: r,
      checks: VALIDATIONS.map(v => ({ ...v, result: v.check(r.data) })),
    })),
    [records]
  )

  const totalErrors   = byRecord.reduce((s, { checks }) => s + checks.filter(c => c.result.status === 'error').length,   0)
  const totalWarnings = byRecord.reduce((s, { checks }) => s + checks.filter(c => c.result.status === 'warning').length, 0)
  const allOk = totalErrors === 0 && totalWarnings === 0

  function recordLabel(r: DataRecord) {
    const te  = r.tipo_entidade ?? 'INDIVIDUAL'
    const per = r.periodo ? r.periodo.substring(0, 10) : '—'
    return `${te} · ${per}`
  }

  return (
    <div className="space-y-4">
      {/* Summary banner */}
      <div className={`flex items-center gap-3 px-4 py-3 rounded-lg border text-xs font-medium ${
        allOk
          ? 'bg-green-50 border-green-200 text-green-700'
          : totalErrors > 0
            ? 'bg-red-50 border-red-200 text-red-700'
            : 'bg-amber-50 border-amber-200 text-amber-700'
      }`}>
        <span className="text-base">{allOk ? '✓' : totalErrors > 0 ? '✗' : '⚠'}</span>
        {allOk
          ? 'Todas as validações passaram.'
          : `${totalErrors > 0 ? `${totalErrors} erro${totalErrors !== 1 ? 's' : ''}` : ''}${totalErrors > 0 && totalWarnings > 0 ? ' · ' : ''}${totalWarnings > 0 ? `${totalWarnings} aviso${totalWarnings !== 1 ? 's' : ''}` : ''} encontrado${totalErrors + totalWarnings !== 1 ? 's' : ''}.`
        }
      </div>

      {/* One section per record (tipo_entidade × periodo) */}
      {byRecord.map(({ record, checks }, ri) => {
        const recErrors   = checks.filter(c => c.result.status === 'error').length
        const recWarnings = checks.filter(c => c.result.status === 'warning').length
        const recOk = recErrors === 0 && recWarnings === 0

        return (
          <div key={ri} className="border border-gray-200 rounded-lg overflow-hidden">
            {/* Record header */}
            <div className={`px-4 py-2.5 flex items-center gap-2 border-b border-gray-200 ${
              recErrors > 0 ? 'bg-red-50' : recWarnings > 0 ? 'bg-amber-50' : 'bg-green-50'
            }`}>
              <span className={`text-sm font-bold ${
                recErrors > 0 ? 'text-red-700' : recWarnings > 0 ? 'text-amber-700' : 'text-green-700'
              }`}>
                {recErrors > 0 ? '✗' : recWarnings > 0 ? '⚠' : '✓'}
              </span>
              <span className={`text-xs font-semibold ${
                recErrors > 0 ? 'text-red-700' : recWarnings > 0 ? 'text-amber-700' : 'text-green-700'
              }`}>
                {recordLabel(record)}
              </span>
              {!recOk && (
                <span className="ml-auto text-xs text-gray-500">
                  {recErrors > 0 ? `${recErrors} erro${recErrors !== 1 ? 's' : ''}` : ''}
                  {recErrors > 0 && recWarnings > 0 ? ' · ' : ''}
                  {recWarnings > 0 ? `${recWarnings} aviso${recWarnings !== 1 ? 's' : ''}` : ''}
                </span>
              )}
            </div>

            {/* Validations for this record */}
            <div className="divide-y divide-gray-100 bg-white">
              {checks.map((c, ci) => {
                const cfg = STATUS_CONFIG[c.result.status]
                return (
                  <div key={ci} className={`flex items-start gap-2 px-4 py-2 ${cfg.bg}`}>
                    <span className={`text-xs font-bold w-4 shrink-0 mt-0.5 ${cfg.text}`}>{cfg.icon}</span>
                    <div className="min-w-0">
                      <p className={`text-xs font-semibold ${cfg.text}`}>{c.label}</p>
                      <p className="text-xs text-gray-500 mt-0.5">{c.result.details}</p>
                    </div>
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
