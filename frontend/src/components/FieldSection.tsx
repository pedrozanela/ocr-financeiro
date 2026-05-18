import { useState } from 'react'
import { SectionDef, FieldDef } from './fieldDefinitions'
import { AssessmentItem } from './FinancialReview'

const brl = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
})

function getScaleMultiplier(scale: string): number {
  const s = scale.toLowerCase().trim()
  if (s.includes('milhão') || s.includes('milhao') || s.includes('million')) return 1_000_000
  if (s.includes('mil') || s.includes('thousand')) return 1_000
  return 1
}

function formatValue(raw: string, type: 'number' | 'text' | 'date', scale: string): string {
  if (!raw || raw === '' || raw === 'null') return ''
  if (type !== 'number') return raw
  const n = parseFloat(raw)
  if (isNaN(n)) return raw
  const multiplier = getScaleMultiplier(scale)
  return brl.format(n * multiplier)
}

// Cascade totals: soma simples respeitando sinais do documento.
// Deduções, custos, despesas, IR, CSLL etc. já vêm negativos do modelo.
const CASCADE_FORMULAS: Record<string, string[]> = {
  'dre.receita_operacional_liquida': [
    'dre.receita_operacional_bruta',
    'dre.total_deducoes',
    'dre.incentivos_a_exportacoes',
  ],
  'dre.lucro_bruto': [
    'dre.receita_operacional_liquida',
    'dre.total_custo',
  ],
  'dre.lucro_operacional': [
    'dre.lucro_bruto',
    'dre.total_despesas_operacionais',
  ],
  'dre.lucro_financeiro': [
    'dre.lucro_operacional',
    'dre.despesas_financeiras',
    'dre.total_receitas_financeiras',
  ],
  'dre.lucro_antes_imposto_de_renda': [
    'dre.lucro_financeiro',
    'dre.resultado_de_equivalencia_patrimonial',
    'dre.receita_nao_operacional',
    'dre.despesa_nao_operacional',
    'dre.saldo_correcao_monetaria',
    'dre.resultado_alienacao_ativos',
  ],
  'dre.lucro_antes_participacoes': [
    'dre.lucro_antes_imposto_de_renda',
    'dre.provisao_imposto_de_renda',
    'dre.csll',
  ],
  'dre.lucro_antes_participacao_minoritaria': [
    'dre.lucro_antes_participacoes',
    'dre.participacoes_gratificacoes_estatutarias',
  ],
  'dre.lucro_liquido': [
    'dre.lucro_antes_participacao_minoritaria',
    'dre.participacao_minoritarios',
  ],
}

const GROUP_LABELS: Record<string, string> = {
  'identificacao':       'Identificação',
  'ativo_circulante':    'Ativo Circulante',
  'ativo_nao_circulante':'Ativo Não Circulante',
  'ativo_permanente':    'Ativo Permanente',
  'passivo_circulante':  'Passivo Circulante',
  'passivo_nao_circulante': 'Passivo Não Circulante',
  'patrimonio_liquido':  'Patrimônio Líquido',
  'dre':                 'DRE',
}

interface FieldGroup {
  key: string
  label: string
  fields: FieldDef[]
  isRoot?: boolean
}

function buildGroups(fields: FieldDef[]): { groups: FieldGroup[]; useGroups: boolean } {
  const groupMap = new Map<string, FieldGroup>()
  const rootFields: FieldDef[] = []

  for (const field of fields) {
    const dot = field.path.indexOf('.')
    if (dot === -1) {
      rootFields.push(field)
    } else {
      const key = field.path.substring(0, dot)
      if (!groupMap.has(key)) {
        groupMap.set(key, { key, label: GROUP_LABELS[key] ?? key, fields: [] })
      }
      groupMap.get(key)!.fields.push(field)
    }
  }

  const prefixGroups = Array.from(groupMap.values())
  // Use collapsible groups when there are multiple prefixes (Ativo, Passivo)
  const useGroups = prefixGroups.length > 1

  const groups: FieldGroup[] = [...prefixGroups]
  if (rootFields.length > 0) {
    groups.push({ key: '__root', label: 'Totais', fields: rootFields, isRoot: true })
  }

  return { groups, useGroups }
}

interface CorrectionData {
  campo: string
  valor_correto: string
  comentario: string
  status?: string
  confirmado_em?: string | null
  confirmado_por?: string | null
}

function getEffectiveNumericValue(
  field: FieldDef,
  data: unknown,
  corrections: Record<string, CorrectionData>,
  getValue: (obj: unknown, path: string) => string,
  scale: string,
): number {
  if (field.type !== 'number') return 0
  const correction = corrections[field.path]
  const raw = correction ? correction.valor_correto : getValue(data, field.path)
  if (!raw || raw === '' || raw === 'null') return 0
  const n = parseFloat(raw)
  return isNaN(n) ? 0 : n * getScaleMultiplier(scale)
}

interface Props {
  section: SectionDef
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
  scale: string
  corrections: Record<string, CorrectionData>
  assessment: Record<string, AssessmentItem>
  saving: string | null
  saved: string | null
  getValue: (obj: unknown, path: string) => string
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string) => Promise<void>
  onDelete: (campo: string) => Promise<void>
  onConfirm: (campo: string) => Promise<void>
}

export default function FieldSection({ section, data, scale, corrections, assessment, saving, saved, getValue, onSave, onDelete, onConfirm }: Props) {
  const { groups, useGroups } = buildGroups(section.fields)
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())

  // Fontes map: field_path → source text from PDF
  const fontes: Record<string, string> = data?.fontes ?? {}

  // Postprocessed map: field_path → original LLM value (before model recalculation)
  const postprocessedMap: Record<string, number> = {}
  if (data?._postprocessed && Array.isArray(data._postprocessed)) {
    for (const pp of data._postprocessed) {
      postprocessedMap[pp.campo] = pp.original
    }
  }

  function toggle(key: string) {
    setCollapsed(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  function renderFields(fields: FieldDef[]) {
    return fields.map(field => {
      const extracted = getValue(data, field.path)
      const correction = corrections[field.path]
      return (
        <FieldRow
          key={field.path}
          label={field.label}
          path={field.path}
          extracted={extracted}
          extractedFormatted={formatValue(extracted, field.type, scale)}
          correctionFormatted={correction ? formatValue(correction.valor_correto, field.type, scale) : undefined}
          correction={correction}
          assessmentItem={assessment[field.path]}
          fonte={fontes[field.path]}
          llmOriginal={postprocessedMap[field.path]}
          scale={scale}
          isTotal={field.isTotal ?? false}
          saving={saving === field.path}
          saved={saved === field.path}
          onSave={onSave}
          onDelete={onDelete}
          onConfirm={onConfirm}
        />
      )
    })
  }

  if (!useGroups) {
    // Flat layout — DRE and Identificação
    // Build blocks: each isTotal field has the non-total number fields before it as components
    const hasAnyTotal = section.fields.some(f => f.isTotal)

    if (!hasAnyTotal) {
      // Identificação — no totals, render flat
      return (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="divide-y divide-gray-50">
            {renderFields(section.fields)}
          </div>
        </div>
      )
    }

    // DRE — totals show computed sum as primary value
    // Phase 1: compute all totals upfront so cascade can propagate
    const computedMap: Record<string, number> = {}
    const computedLabelMap: Record<string, string> = {}
    let bf: FieldDef[] = []

    // Helper: get effective value for a field, preferring computed total for totals
    function getEffective(path: string): number {
      if (path in computedMap) return computedMap[path]
      const raw = corrections[path]?.valor_correto ?? getValue(data, path)
      if (!raw || raw === '' || raw === 'null') return 0
      const n = parseFloat(raw)
      return isNaN(n) ? 0 : n * getScaleMultiplier(scale)
    }

    for (const field of section.fields) {
      if (field.isTotal && field.type === 'number') {
        const cascadeFormula = CASCADE_FORMULAS[field.path]
        if (cascadeFormula) {
          // Cascade reads from computedMap (propagates previous totals)
          computedMap[field.path] = cascadeFormula.reduce((sum, p) => sum + getEffective(p), 0)
          computedLabelMap[field.path] = 'Fórmula'
          bf.length = 0
        } else if (bf.length > 0) {
          computedMap[field.path] = bf.reduce(
            (sum, f) => sum + getEffective(f.path), 0
          )
          computedLabelMap[field.path] = 'Soma'
          bf.length = 0
        } else {
          bf.length = 0
        }
      } else if (!field.isTotal && field.type === 'number') {
        bf.push(field)
      }
    }

    // Phase 2: render using pre-computed map
    const elements: React.ReactNode[] = []
    for (const field of section.fields) {
      const extracted = getValue(data, field.path)
      const correction = corrections[field.path]

      elements.push(
        <FieldRow
          key={field.path}
          label={field.label}
          path={field.path}
          extracted={extracted}
          extractedFormatted={formatValue(extracted, field.type, scale)}
          correctionFormatted={correction ? formatValue(correction.valor_correto, field.type, scale) : undefined}
          correction={correction}
          assessmentItem={assessment[field.path]}
          computedTotal={computedMap[field.path]}
          computedLabel={computedLabelMap[field.path]}
          fonte={fontes[field.path]}
          llmOriginal={postprocessedMap[field.path]}
          scale={scale}
          isTotal={field.isTotal ?? false}
          saving={saving === field.path}
          saved={saved === field.path}
          onSave={onSave}
          onDelete={onDelete}
          onConfirm={onConfirm}
        />
      )
    }

    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="divide-y divide-gray-50">
          {elements}
        </div>
      </div>
    )
  }

  // Grouped layout — Ativo and Passivo
  // Pre-compute sums for each sub-group so root can use them
  const groupSumMap: Record<string, number> = {}
  for (const group of groups) {
    if (group.isRoot) continue
    const nonTotalFields = group.fields.filter(f => !f.isTotal && f.type === 'number')
    groupSumMap[group.key] = nonTotalFields.reduce(
      (sum, f) => sum + getEffectiveNumericValue(f, data, corrections, getValue, scale), 0
    )
  }

  return (
    <div className="space-y-3">
      {groups.map(group => {
        if (group.isRoot) {
          // Root total = sum of sub-group computed sums
          const rootSum = Object.values(groupSumMap).reduce((a, b) => a + b, 0)

          return (
            <div key={group.key} className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <div className="divide-y divide-gray-50">
                {group.fields.map(field => {
                  const extracted = getValue(data, field.path)
                  const correction = corrections[field.path]

                  return (
                    <FieldRow
                      key={field.path}
                      label={field.label}
                      path={field.path}
                      extracted={extracted}
                      extractedFormatted={formatValue(extracted, field.type, scale)}
                      correctionFormatted={correction ? formatValue(correction.valor_correto, field.type, scale) : undefined}
                      correction={correction}
                      assessmentItem={assessment[field.path]}
                      computedTotal={field.isTotal ? rootSum : undefined}
                      computedLabel="Soma dos blocos"
                      fonte={fontes[field.path]}
                      scale={scale}
                      isTotal={field.isTotal ?? false}
                      saving={saving === field.path}
                      saved={saved === field.path}
                      onSave={onSave}
                      onDelete={onDelete}
                      onConfirm={onConfirm}
                    />
                  )
                })}
              </div>
            </div>
          )
        }

        const isOpen = !collapsed.has(group.key)
        const corrCount = group.fields.filter(f => corrections[f.path]).length
        const totalField = [...group.fields].reverse().find(f => f.isTotal)
        const calculatedSum = groupSumMap[group.key] ?? 0

        return (
          <div key={group.key} className="border border-gray-200 rounded-xl shadow-sm overflow-hidden">
            {/* Group header — no sum, just label */}
            <button
              onClick={() => toggle(group.key)}
              className={`w-full flex items-center gap-3 px-4 py-3 text-left transition-colors ${
                isOpen ? 'bg-gray-50 border-b border-gray-200' : 'bg-white hover:bg-gray-50'
              }`}
            >
              <svg
                className={`w-4 h-4 text-gray-400 transition-transform duration-150 shrink-0 ${isOpen ? 'rotate-90' : ''}`}
                fill="none" stroke="currentColor" viewBox="0 0 24 24"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>

              <span className="text-sm font-semibold text-gray-700">{group.label}</span>

              {corrCount > 0 && (
                <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-700">
                  {corrCount} correç{corrCount !== 1 ? 'ões' : 'ão'}
                </span>
              )}
            </button>

            {isOpen && (
              <div className="bg-white divide-y divide-gray-50 animate-fade-in">
                {group.fields.map(field => {
                  const extracted = getValue(data, field.path)
                  const correction = corrections[field.path]

                  return (
                    <FieldRow
                      key={field.path}
                      label={field.label}
                      path={field.path}
                      extracted={extracted}
                      extractedFormatted={formatValue(extracted, field.type, scale)}
                      correctionFormatted={correction ? formatValue(correction.valor_correto, field.type, scale) : undefined}
                      correction={correction}
                      assessmentItem={assessment[field.path]}
                      computedTotal={field.isTotal ? calculatedSum : undefined}
                      computedLabel="Soma"
                      fonte={fontes[field.path]}
                      scale={scale}
                      isTotal={field.isTotal ?? false}
                      saving={saving === field.path}
                      saved={saved === field.path}
                      onSave={onSave}
                      onDelete={onDelete}
                      onConfirm={onConfirm}
                    />
                  )
                })}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ─── FieldRow ────────────────────────────────────────────────────────────────

interface RowProps {
  label: string
  path: string
  extracted: string
  extractedFormatted: string
  correctionFormatted?: string
  correction?: CorrectionData
  assessmentItem?: AssessmentItem
  /** For total fields: the computed sum that becomes the displayed value */
  computedTotal?: number
  /** For total fields: label describing how it was computed */
  computedLabel?: string
  /** Source text from PDF used to extract this field */
  fonte?: string
  /** Original LLM value before post-processing (if recalculated) */
  llmOriginal?: number
  scale: string
  isTotal: boolean
  saving: boolean
  saved: boolean
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string) => Promise<void>
  onDelete: (campo: string) => Promise<void>
  onConfirm: (campo: string) => Promise<void>
}

const ERROR_TAGS = [
  'Valor incorreto (OCR)',
  'Dígito errado',
  'Escala incorreta (mil/milhão)',
  'Campo trocado',
  'Sinal invertido (+/-)',
  'Valor ausente no PDF',
  'Faltou somar subconta',
  'IR/CSLL diferido não incluído',
]

function FieldRow({ label, path, extracted, extractedFormatted, correctionFormatted, correction, assessmentItem, computedTotal, computedLabel, fonte, llmOriginal, scale, isTotal, saving, saved, onSave, onDelete, onConfirm }: RowProps) {
  const mult = getScaleMultiplier(scale)
  // Display value in UI scale (unit), store in JSON scale
  const extractedScaled = (() => {
    const n = parseFloat(extracted)
    return isNaN(n) ? extracted : String(n * mult)
  })()
  const corrScaled = correction?.valor_correto
    ? (() => { const n = parseFloat(correction.valor_correto); return isNaN(n) ? correction.valor_correto : String(n * mult) })()
    : undefined
  const [editing, setEditing]       = useState(false)
  const [corrValue, setCorrValue]   = useState(corrScaled ?? extractedScaled)
  const [comment, setComment]       = useState(correction?.comentario ?? '')
  const [freeText, setFreeText]     = useState(false)
  const [confirming, setConfirming] = useState(false)

  const isConfirmed  = correction?.status === 'confirmado'
  const hasCorrBadge = !!correction

  function startEdit() {
    setCorrValue(corrScaled ?? extractedScaled)
    setComment(correction?.comentario ?? '')
    setFreeText(!!correction?.comentario && !ERROR_TAGS.includes(correction.comentario))
    setEditing(true)
  }

  function selectTag(tag: string) { setComment(tag); setFreeText(false) }
  function activateFreeText()     { setComment(''); setFreeText(true) }

  async function handleSave() {
    // Convert back to JSON scale for storage
    const n = parseFloat(corrValue)
    const valueToStore = isNaN(n) ? corrValue : String(n / mult)
    await onSave(path, extracted, valueToStore, comment)
    setEditing(false)
  }

  function handleCancel() {
    setCorrValue(corrScaled ?? extractedScaled)
    setComment(correction?.comentario ?? '')
    setFreeText(false)
    setEditing(false)
  }

  async function handleConfirm() {
    setConfirming(true)
    await onConfirm(path)
    setConfirming(false)
  }

  const leftBorder = isConfirmed
    ? 'border-l-2 border-l-emerald-400'
    : hasCorrBadge
      ? 'border-l-2 border-l-amber-400'
      : 'border-l-2 border-l-transparent'

  const rowBg = isTotal ? 'bg-gray-50/80' : 'bg-white'

  return (
    <div className={`${rowBg} ${leftBorder} group`}>
      <div className="px-4 py-2.5 flex items-center gap-3">
        {/* Label */}
        <div className="w-52 shrink-0 flex items-center gap-1.5">
          <span className={`text-sm leading-tight ${isTotal ? 'font-semibold text-gray-800' : 'text-gray-600'}`}>
            {label}
          </span>
          {assessmentItem && !correction && (
            <span
              title={assessmentItem.motivo}
              className={`inline-flex items-center text-[10px] font-semibold px-1.5 py-0.5 rounded-full cursor-help shrink-0 ${
                assessmentItem.confianca === 'baixa'
                  ? 'bg-red-50 text-red-600 border border-red-200'
                  : 'bg-orange-50 text-orange-600 border border-orange-200'
              }`}
            >
              {assessmentItem.confianca === 'baixa' ? '⚠ baixa' : '~ média'}
            </span>
          )}
          {fonte && (
            <span
              title={fonte}
              className="inline-flex items-center justify-center w-4 h-4 rounded-full cursor-help shrink-0 bg-gray-100 text-gray-400 hover:bg-blue-50 hover:text-blue-500 transition-colors text-[10px] font-bold"
            >
              i
            </span>
          )}
        </div>

        {/* Value */}
        <div className="flex-1 min-w-0 flex items-center gap-2">
          {isTotal && computedTotal !== undefined ? (
            <>
              {/* Total: show computed sum as primary value */}
              <span className="text-sm font-mono font-bold tabular-nums text-gray-900">
                {brl.format(computedTotal)}
              </span>
              {/* Badge showing LLM-extracted value if different */}
              {(() => {
                // Use original LLM value (before post-processing) if available
                const llmVal = llmOriginal !== undefined
                  ? llmOriginal * mult
                  : parseFloat(extracted) * mult || 0
                const llmFormatted = llmOriginal !== undefined
                  ? brl.format(llmVal)
                  : (extractedFormatted || '—')
                if (Math.abs(computedTotal - llmVal) > 0.01 && llmFormatted !== '—') {
                  return (
                    <span
                      title={`${computedLabel ?? 'Soma'} dos componentes\nValor extraído pelo LLM: ${llmFormatted}`}
                      className="inline-flex items-center gap-1 text-[10px] font-semibold px-1.5 py-0.5 rounded-full cursor-help shrink-0 bg-amber-50 text-amber-600 border border-amber-200"
                    >
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M12 3l9.66 16.5H2.34L12 3z" />
                      </svg>
                      LLM: {llmFormatted}
                    </span>
                  )
                }
                return null
              })()}
            </>
          ) : (
            <>
              <span className={`text-sm font-mono tabular-nums ${
                hasCorrBadge ? 'line-through text-gray-300' : isTotal ? 'font-bold text-gray-900' : 'text-gray-700'
              }`}>
                {extractedFormatted || <span className="text-gray-200 font-sans text-xs not-italic">—</span>}
              </span>
              {hasCorrBadge && (
                <span className={`flex items-center gap-1 text-sm font-mono font-semibold tabular-nums ${
                  isConfirmed ? 'text-emerald-700' : 'text-amber-700'
                }`}>
                  <svg className="w-3 h-3 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                  </svg>
                  {correctionFormatted ?? correction!.valor_correto}
                </span>
              )}
            </>
          )}
        </div>

        {/* Actions — visible on hover or when has correction */}
        <div className={`flex items-center gap-1.5 shrink-0 transition-opacity ${hasCorrBadge || editing ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}>
          {saved && (
            <span className="inline-flex items-center gap-1 text-xs text-emerald-600 font-medium">
              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
              </svg>
              Salvo
            </span>
          )}

          {hasCorrBadge && !editing && !isConfirmed && (
            <button
              onClick={handleConfirm}
              disabled={confirming}
              className="inline-flex items-center gap-1 text-xs px-2.5 py-1 rounded-md bg-emerald-50 text-emerald-700 hover:bg-emerald-100 border border-emerald-200 font-medium transition-all disabled:opacity-40"
            >
              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
              </svg>
              {confirming ? '…' : 'Confirmar'}
            </button>
          )}

          {isConfirmed && !editing && (
            <span className="inline-flex items-center gap-1 text-xs text-emerald-600 font-medium">
              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              Confirmado
            </span>
          )}

          {hasCorrBadge && !editing && (
            <button
              onClick={() => onDelete(path)}
              title="Remover correção"
              className="w-6 h-6 flex items-center justify-center rounded text-gray-300 hover:text-red-500 hover:bg-red-50 transition-all"
            >
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>
            </button>
          )}

          {!editing && (
            <button
              onClick={startEdit}
              className={`text-xs px-2.5 py-1 rounded-md font-medium transition-all ${
                hasCorrBadge
                  ? isConfirmed
                    ? 'bg-emerald-50 text-emerald-700 hover:bg-emerald-100 border border-emerald-200'
                    : 'bg-amber-50 text-amber-700 hover:bg-amber-100 border border-amber-200'
                  : 'bg-gray-50 text-gray-500 hover:bg-gray-100 border border-gray-200 hover:text-gray-700'
              }`}
            >
              {hasCorrBadge ? 'Editar' : 'Corrigir'}
            </button>
          )}
        </div>
      </div>

      {/* Audit trail */}
      {isConfirmed && (correction?.confirmado_por || correction?.confirmado_em) && (
        <div className="px-4 pb-2 flex items-center gap-1.5" style={{ paddingLeft: 'calc(13rem + 1rem)' }}>
          <svg className="w-3 h-3 text-emerald-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
          </svg>
          <span className="text-[10px] text-emerald-600">
            Confirmado{correction.confirmado_por ? ` por ${correction.confirmado_por}` : ''}
            {correction.confirmado_em ? ` em ${correction.confirmado_em.substring(0, 19).replace('T', ' ')}` : ''}
            {correction.comentario ? ` · ${correction.comentario}` : ''}
          </span>
        </div>
      )}

      {/* Edit form */}
      {editing && (
        <div className="border-t border-gray-100 bg-gray-50/80 px-4 py-3 space-y-3 animate-fade-in">
          <div className="flex gap-3 items-center">
            <label className="text-xs font-medium text-gray-500 w-28 shrink-0">Valor correto</label>
            <input
              value={corrValue}
              onChange={e => setCorrValue(e.target.value)}
              className="flex-1 text-sm font-mono border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-[#0F2137]/15 focus:border-[#0F2137]/30 bg-white"
              placeholder="Valor correto…"
              autoFocus
            />
          </div>

          <div className="flex gap-3 items-start">
            <label className="text-xs font-medium text-gray-500 w-28 shrink-0 pt-1">Tipo de erro</label>
            <div className="flex-1 space-y-2">
              <div className="flex flex-wrap gap-1.5">
                {ERROR_TAGS.map(tag => (
                  <button
                    key={tag}
                    type="button"
                    onClick={() => selectTag(tag)}
                    className={`text-xs px-2.5 py-1 rounded-full border transition-all ${
                      comment === tag && !freeText
                        ? 'bg-[#0F2137] text-white border-[#0F2137]'
                        : 'bg-white text-gray-600 border-gray-200 hover:border-[#0F2137]/30 hover:text-[#0F2137]'
                    }`}
                  >
                    {tag}
                  </button>
                ))}
                <button
                  type="button"
                  onClick={activateFreeText}
                  className={`text-xs px-2.5 py-1 rounded-full border transition-all ${
                    freeText
                      ? 'bg-[#0F2137] text-white border-[#0F2137]'
                      : 'bg-white text-gray-600 border-gray-200 hover:border-[#0F2137]/30 hover:text-[#0F2137]'
                  }`}
                >
                  Outro…
                </button>
              </div>
              {freeText && (
                <input
                  value={comment}
                  onChange={e => setComment(e.target.value)}
                  className="w-full text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-[#0F2137]/15 focus:border-[#0F2137]/30 bg-white"
                  placeholder="Descreva o erro…"
                  autoFocus
                />
              )}
            </div>
          </div>

          <div className="flex gap-2 justify-end pt-0.5">
            <button
              onClick={handleCancel}
              className="text-xs px-3.5 py-1.5 rounded-lg bg-white border border-gray-200 text-gray-600 hover:bg-gray-50 transition-all"
            >
              Cancelar
            </button>
            <button
              onClick={handleSave}
              disabled={saving}
              className="text-xs px-3.5 py-1.5 rounded-lg bg-[#0F2137] text-white hover:bg-[#1a3050] disabled:opacity-40 transition-all font-medium"
            >
              {saving ? 'Salvando…' : 'Salvar correção'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
