import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { SectionDef, FieldDef } from './fieldDefinitions'
import { AssessmentItem } from './FinancialReview'
import {
  IconSparkles, IconUserCheck, IconPencil, IconArrowBackUp,
  IconAlertCircle, IconEye, IconEyeOff, IconDownload, IconCheck, IconX,
  IconChevronRight, IconInfoCircle, IconTag,
} from '@tabler/icons-react'

// ─── Types ───────────────────────────────────────────────────────────────────

export type FonteItem = { nome: string; valor: number; operacao: '+' | '-' }
export type FonteValue = string | FonteItem[]

// ─── Constants / helpers ─────────────────────────────────────────────────────

const brl = new Intl.NumberFormat('pt-BR', {
  style: 'currency', currency: 'BRL',
  minimumFractionDigits: 2, maximumFractionDigits: 2,
})

const brNumber = new Intl.NumberFormat('pt-BR', {
  minimumFractionDigits: 2, maximumFractionDigits: 2,
})

// Parse aceitando ambos formatos: "23.868.692,18" (BR) ou "23868692.18" (US/cru).
// Heurística: se contém vírgula E ponto, o último é separador decimal.
// Se só vírgula → BR. Se só ponto → US (a menos que apareça mais de uma vez = milhar BR).
function parseNumericBR(s: string): number | null {
  if (!s || !s.trim()) return null
  const cleaned = s.trim().replace(/[^\d.,\-]/g, '')
  if (!cleaned) return null
  const lastComma = cleaned.lastIndexOf(',')
  const lastDot = cleaned.lastIndexOf('.')
  let normalized: string
  if (lastComma === -1 && lastDot === -1) {
    normalized = cleaned
  } else if (lastComma > lastDot) {
    // vírgula é decimal (BR): remove pontos (milhar) e troca vírgula por ponto
    normalized = cleaned.replace(/\./g, '').replace(',', '.')
  } else if (lastDot > lastComma) {
    // ponto é decimal (US): remove vírgulas (milhar)
    normalized = cleaned.replace(/,/g, '')
  } else {
    normalized = cleaned
  }
  // Heurística para "1.234" (sem vírgula): se há múltiplos pontos, todos são milhar → remove tudo
  if (lastComma === -1 && (cleaned.match(/\./g) || []).length > 1) {
    normalized = cleaned.replace(/\./g, '')
  }
  const n = parseFloat(normalized)
  return isNaN(n) ? null : n
}

function formatBRNumber(n: number | null): string {
  if (n === null || isNaN(n)) return ''
  return brNumber.format(n)
}

function getScaleMultiplier(scale: string): number {
  const s = scale.toLowerCase().trim()
  if (s.includes('milhão') || s.includes('milhao') || s.includes('million')) return 1_000_000
  if (s.includes('mil') || s.includes('thousand')) return 1_000
  return 1
}

function formatValue(raw: string, type: 'number' | 'text' | 'date' | 'enum', scale: string, options?: { value: number; label: string }[]): string {
  if (!raw || raw === '' || raw === 'null') return ''
  if (type === 'enum' && options) {
    const n = parseInt(raw, 10)
    if (isNaN(n)) {
      const byLabel = options.find(o => o.label.toLowerCase() === raw.toLowerCase())
      return byLabel ? byLabel.label : raw
    }
    const match = options.find(o => o.value === n)
    return match ? match.label : raw
  }
  if (type !== 'number') return raw
  const n = parseFloat(raw)
  if (isNaN(n)) return raw
  return brl.format(n * getScaleMultiplier(scale))
}

// Soma simples com sinais do documento
const CASCADE_FORMULAS: Record<string, string[]> = {
  'dre.receita_operacional_liquida': [
    'dre.receita_operacional_bruta',
    'dre.total_deducoes',
    'dre.incentivos_a_exportacoes',
  ],
  'dre.lucro_bruto': ['dre.receita_operacional_liquida', 'dre.total_custo'],
  'dre.lucro_operacional': ['dre.lucro_bruto', 'dre.total_despesas_operacionais'],
  'dre.lucro_financeiro': [
    'dre.lucro_operacional', 'dre.despesas_financeiras', 'dre.total_receitas_financeiras',
  ],
  'dre.lucro_antes_imposto_de_renda': [
    'dre.lucro_financeiro', 'dre.resultado_de_equivalencia_patrimonial',
    'dre.receita_nao_operacional', 'dre.despesa_nao_operacional',
    'dre.saldo_correcao_monetaria', 'dre.resultado_alienacao_ativos',
  ],
  'dre.lucro_antes_participacoes': [
    'dre.lucro_antes_imposto_de_renda', 'dre.provisao_imposto_de_renda', 'dre.csll',
  ],
  'dre.lucro_antes_participacao_minoritaria': [
    'dre.lucro_antes_participacoes', 'dre.participacoes_gratificacoes_estatutarias',
  ],
  'dre.lucro_liquido': ['dre.lucro_antes_participacao_minoritaria', 'dre.participacao_minoritarios'],
}

const GROUP_LABELS: Record<string, string> = {
  'identificacao': 'Identificação',
  'ativo_circulante': 'Ativo Circulante',
  'ativo_nao_circulante': 'Ativo Não Circulante',
  'ativo_permanente': 'Ativo Permanente',
  'passivo_circulante': 'Passivo Circulante',
  'passivo_nao_circulante': 'Passivo Não Circulante',
  'patrimonio_liquido': 'Patrimônio Líquido',
  'dre': 'DRE',
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
      if (!groupMap.has(key)) groupMap.set(key, { key, label: GROUP_LABELS[key] ?? key, fields: [] })
      groupMap.get(key)!.fields.push(field)
    }
  }
  const prefixGroups = Array.from(groupMap.values())
  const useGroups = prefixGroups.length > 1
  const groups: FieldGroup[] = [...prefixGroups]
  if (rootFields.length > 0) groups.push({ key: '__root', label: 'Totais', fields: rootFields, isRoot: true })
  return { groups, useGroups }
}

interface CorrectionData {
  campo: string
  valor_correto: string
  comentario: string
  status?: string
  confirmado_em?: string | null
  confirmado_por?: string | null
  tipo_erro?: string | null
}

function getEffectiveNumericValue(
  field: FieldDef, data: unknown, corrections: Record<string, CorrectionData>,
  getValue: (obj: unknown, path: string) => string, scale: string,
): number {
  if (field.type !== 'number') return 0
  const correction = corrections[field.path]
  const raw = correction ? correction.valor_correto : getValue(data, field.path)
  if (!raw || raw === '' || raw === 'null') return 0
  const n = parseFloat(raw)
  return isNaN(n) ? 0 : n * getScaleMultiplier(scale)
}

// ─── Props ───────────────────────────────────────────────────────────────────

interface Props {
  section: SectionDef
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
  scale: string
  corrections: Record<string, CorrectionData>
  assessment: Record<string, AssessmentItem>
  saving: string | null
  saved: string | null
  documentName?: string
  getValue: (obj: unknown, path: string) => string
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string, tipoErro?: string | null) => Promise<void>
  onDelete: (campo: string) => Promise<void>
  onConfirm: (campo: string) => Promise<void>
  onBulkConfirm?: (items: { campo: string; valor_extraido: string; valor_correto: string }[]) => Promise<void>
  readOnly?: boolean
}

// ─── Main component ──────────────────────────────────────────────────────────

export default function FieldSection({
  section, data, scale, corrections, assessment, saving, saved, documentName,
  getValue, onSave, onDelete, onConfirm, onBulkConfirm, readOnly = false,
}: Props) {
  const { groups, useGroups } = buildGroups(section.fields)
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const [editingPath, setEditingPath] = useState<string | null>(null)
  const [auditMode, setAuditMode] = useState(false)

  const fontes: Record<string, FonteValue> = data?.fontes ?? {}
  const postprocessedMap: Record<string, number> = {}
  if (data?._postprocessed && Array.isArray(data._postprocessed)) {
    for (const pp of data._postprocessed) postprocessedMap[pp.campo] = pp.original
  }

  function toggle(key: string) {
    setCollapsed(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  // Compute totals (cascade DRE + group sums)
  const computedMap: Record<string, number> = {}
  const computedLabelMap: Record<string, string> = {}
  if (!useGroups) {
    // Flat (DRE / Identificação) — cascade or running-sum
    function getEffective(path: string): number {
      if (path in computedMap) return computedMap[path]
      const raw = corrections[path]?.valor_correto ?? getValue(data, path)
      if (!raw || raw === '' || raw === 'null') return 0
      const n = parseFloat(raw)
      return isNaN(n) ? 0 : n * getScaleMultiplier(scale)
    }
    let bf: FieldDef[] = []
    for (const field of section.fields) {
      if (field.isTotal && field.type === 'number') {
        const cascadeFormula = CASCADE_FORMULAS[field.path]
        if (cascadeFormula) {
          computedMap[field.path] = cascadeFormula.reduce((s, p) => s + getEffective(p), 0)
          computedLabelMap[field.path] = 'Fórmula'
          bf = []
        } else if (bf.length > 0) {
          computedMap[field.path] = bf.reduce((s, f) => s + getEffective(f.path), 0)
          computedLabelMap[field.path] = 'Soma'
          bf = []
        } else {
          bf = []
        }
      } else if (!field.isTotal && field.type === 'number') {
        bf.push(field)
      }
    }
  }

  // Group sums (Ativo/Passivo)
  const groupSumMap: Record<string, number> = {}
  for (const group of groups) {
    if (group.isRoot) continue
    const nonTotalFields = group.fields.filter(f => !f.isTotal && f.type === 'number')
    groupSumMap[group.key] = nonTotalFields.reduce(
      (s, f) => s + getEffectiveNumericValue(f, data, corrections, getValue, scale), 0,
    )
  }
  const rootSum = Object.values(groupSumMap).reduce((a, b) => a + b, 0)

  // Counters and bulk handlers
  function fieldCounts(fields: FieldDef[]) {
    let total = 0, corrigidos = 0, confirmados = 0
    for (const f of fields) {
      total++
      const c = corrections[f.path]
      if (!c) continue
      const valorLLM = getValue(data, f.path)
      if (c.valor_correto !== valorLLM) corrigidos++
      else confirmados++
    }
    return { total, corrigidos, confirmados }
  }

  async function bulkConfirmGroup(fields: FieldDef[]) {
    if (!onBulkConfirm) return
    const toConfirm = fields
      .filter(f => !corrections[f.path] && f.type !== 'text') // só LLM puro, ignora text livre
      .map(f => ({
        campo: f.path,
        valor_extraido: getValue(data, f.path),
        valor_correto: getValue(data, f.path),
      }))
    if (toConfirm.length === 0) return
    await onBulkConfirm(toConfirm)
  }

  function exportCorrections(fields: FieldDef[]) {
    const items = fields.map(f => {
      const c = corrections[f.path]
      const valor_llm = parseFloat(getValue(data, f.path) || '0') * getScaleMultiplier(scale)
      const valor_final = c
        ? parseFloat(c.valor_correto || '0') * getScaleMultiplier(scale)
        : valor_llm
      return {
        campo: f.path,
        valor_llm: isNaN(valor_llm) ? null : valor_llm,
        valor_final: isNaN(valor_final) ? null : valor_final,
        acao: c ? (c.valor_correto !== getValue(data, f.path) ? 'corrigido' : 'confirmado') : 'llm',
        usuario: c?.confirmado_por ?? '',
        timestamp: c?.confirmado_em ?? '',
      }
    })
    const payload = {
      documento_id: documentName ?? '',
      cnpj: data?.cnpj ?? data?.identificacao?.cnpj ?? '',
      periodo: data?.identificacao?.periodo ?? '',
      correcoes: items,
    }
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${documentName ?? 'correcoes'}__${section.label}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  // ─── Render ──────────────────────────────────────────────────────────────────

  function renderRowsForGroup(group: FieldGroup) {
    return group.fields.map(field => {
      const extracted = getValue(data, field.path)
      const correction = corrections[field.path]
      // Compute total for this row
      let computedTotal: number | undefined = undefined
      let computedLabel: string | undefined = undefined
      if (field.isTotal && field.type === 'number') {
        if (useGroups) {
          computedTotal = group.isRoot ? rootSum : (groupSumMap[group.key] ?? 0)
          computedLabel = group.isRoot ? 'Soma dos blocos' : 'Soma'
        } else if (field.path in computedMap) {
          computedTotal = computedMap[field.path]
          computedLabel = computedLabelMap[field.path]
        }
      }
      const llmOriginal = postprocessedMap[field.path]
      const isDimmed = editingPath !== null && editingPath !== field.path

      const common = {
        field, extracted, correction, scale,
        assessmentItem: assessment[field.path], fonte: fontes[field.path],
        llmOriginal, computedTotal, computedLabel,
        readOnly, dimmed: isDimmed,
        saving: saving === field.path, saved: saved === field.path,
        getValue, onSave, onDelete,
        editingPath, setEditingPath,
      }

      return auditMode
        ? <FieldRowAudit key={field.path} {...common} />
        : <FieldRowNormal key={field.path} {...common} />
    })
  }

  function GroupHeader({ group }: { group: FieldGroup }) {
    if (group.isRoot) return null
    const counts = fieldCounts(group.fields.filter(f => !f.isTotal))
    const llmCount = counts.total - counts.corrigidos - counts.confirmados
    return (
      <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200">
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <button onClick={() => toggle(group.key)} className="text-gray-400 hover:text-gray-700">
            <IconChevronRight
              size={16}
              className={`transition-transform ${!collapsed.has(group.key) ? 'rotate-90' : ''}`}
            />
          </button>
          <div className="min-w-0">
            <p className="text-[15px] font-medium text-gray-800 leading-tight">{group.label}</p>
            <p className="text-xs text-gray-500 mt-0.5">
              {counts.total} {counts.total === 1 ? 'campo' : 'campos'}
              {counts.corrigidos > 0 && <> · <span className="text-blue-600">{counts.corrigidos} corrigido{counts.corrigidos !== 1 ? 's' : ''}</span></>}
              {counts.confirmados > 0 && <> · <span className="text-blue-600">{counts.confirmados} confirmado{counts.confirmados !== 1 ? 's' : ''}</span></>}
            </p>
          </div>
        </div>
        {!readOnly && (
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => setAuditMode(v => !v)}
              className={`inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-full border transition-colors ${
                auditMode ? 'bg-blue-50 text-blue-700 border-blue-200' : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'
              }`}
            >
              {auditMode ? <IconEyeOff size={12} /> : <IconEye size={12} />}
              {auditMode ? 'Ocultar originais' : 'Mostrar originais'}
            </button>
            {auditMode && (
              <button
                onClick={() => exportCorrections(group.fields)}
                className="inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-full border border-gray-200 bg-white text-gray-500 hover:bg-gray-50"
              >
                <IconDownload size={12} /> Exportar
              </button>
            )}
            {llmCount > 0 && (
              <button
                onClick={() => bulkConfirmGroup(group.fields.filter(f => !f.isTotal))}
                className="inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-md bg-blue-600 text-white hover:bg-blue-700 font-medium"
              >
                <IconCheck size={12} /> Confirmar tudo
              </button>
            )}
          </div>
        )}
      </div>
    )
  }

  // Single flat section (Identificação / DRE) — render as one card with toolbar
  if (!useGroups) {
    const fields = section.fields
    const counts = fieldCounts(fields.filter(f => !f.isTotal))
    const llmCount = counts.total - counts.corrigidos - counts.confirmados
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200">
          <div className="min-w-0">
            <p className="text-[15px] font-medium text-gray-800 leading-tight">{section.label}</p>
            <p className="text-xs text-gray-500 mt-0.5">
              {counts.total} {counts.total === 1 ? 'campo' : 'campos'}
              {counts.corrigidos > 0 && <> · <span className="text-blue-600">{counts.corrigidos} corrigido{counts.corrigidos !== 1 ? 's' : ''}</span></>}
              {counts.confirmados > 0 && <> · <span className="text-blue-600">{counts.confirmados} confirmado{counts.confirmados !== 1 ? 's' : ''}</span></>}
            </p>
          </div>
          {!readOnly && (
            <div className="flex items-center gap-2 shrink-0">
              <button
                onClick={() => setAuditMode(v => !v)}
                className={`inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-full border ${
                  auditMode ? 'bg-blue-50 text-blue-700 border-blue-200' : 'bg-white text-gray-500 border-gray-200 hover:bg-gray-50'
                }`}
              >
                {auditMode ? <IconEyeOff size={12} /> : <IconEye size={12} />}
                {auditMode ? 'Ocultar originais' : 'Mostrar originais'}
              </button>
              {auditMode && (
                <button
                  onClick={() => exportCorrections(fields)}
                  className="inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-full border border-gray-200 bg-white text-gray-500 hover:bg-gray-50"
                >
                  <IconDownload size={12} /> Exportar
                </button>
              )}
              {llmCount > 0 && (
                <button
                  onClick={() => bulkConfirmGroup(fields.filter(f => !f.isTotal))}
                  className="inline-flex items-center gap-1 text-[11px] px-2 py-1 rounded-md bg-blue-600 text-white hover:bg-blue-700 font-medium"
                >
                  <IconCheck size={12} /> Confirmar tudo
                </button>
              )}
            </div>
          )}
        </div>
        <div>{renderRowsForGroup({ key: section.label, label: section.label, fields })}</div>
      </div>
    )
  }

  // Grouped layout (Ativo / Passivo)
  return (
    <div className="space-y-3">
      {groups.map(group => {
        if (group.isRoot) {
          return (
            <div key={group.key} className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <div>{renderRowsForGroup(group)}</div>
            </div>
          )
        }
        const isOpen = !collapsed.has(group.key)
        return (
          <div key={group.key} className="border border-gray-200 rounded-xl shadow-sm overflow-hidden bg-white">
            <GroupHeader group={group} />
            {isOpen && <div>{renderRowsForGroup(group)}</div>}
          </div>
        )
      })}
    </div>
  )
}

// ─── FieldRowNormal ──────────────────────────────────────────────────────────

interface RowCommon {
  field: FieldDef
  extracted: string
  correction?: CorrectionData
  assessmentItem?: AssessmentItem
  fonte?: FonteValue
  llmOriginal?: number
  computedTotal?: number
  computedLabel?: string
  scale: string
  readOnly: boolean
  dimmed: boolean
  saving: boolean
  saved: boolean
  getValue: (obj: unknown, path: string) => string
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string, tipoErro?: string | null) => Promise<void>
  onDelete: (campo: string) => Promise<void>
  editingPath: string | null
  setEditingPath: (p: string | null) => void
}

function FieldRowNormal(props: RowCommon) {
  const { field, extracted, correction, scale, readOnly, dimmed, saving, saved,
          getValue: _gv, onSave, onDelete, editingPath, setEditingPath,
          computedTotal, computedLabel, llmOriginal } = props
  void _gv
  const isEditing = editingPath === field.path
  const isReviewed = !!correction
  const valueChanged = isReviewed && correction!.valor_correto !== extracted
  const isTotal = field.isTotal ?? false
  const mult = getScaleMultiplier(scale)
  const skipScale = field.type === 'enum' || field.type === 'text' || field.type === 'date'

  // Effective displayed value (JSON-scale, before unit multiplication)
  const effectiveRaw = isReviewed ? correction!.valor_correto : extracted
  const effectiveFormatted = field.isTotal && field.type === 'number' && computedTotal !== undefined
    ? brl.format(computedTotal)
    : formatValue(effectiveRaw, field.type, scale, field.options)

  // Edit form state — valor exibido em formato BR (23.868.692,18)
  const inputRef = useRef<HTMLInputElement>(null)
  function initialEditValue(): string {
    if (!effectiveRaw) return ''
    if (skipScale) return effectiveRaw
    const n = parseFloat(effectiveRaw)
    if (isNaN(n)) return effectiveRaw
    return formatBRNumber(n * mult)
  }
  const [editValue, setEditValue] = useState(initialEditValue)
  useEffect(() => {
    if (isEditing) {
      setEditValue(initialEditValue())
      setTimeout(() => inputRef.current?.focus(), 10)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isEditing])

  function startEdit() {
    if (readOnly || isEditing) return
    setEditingPath(field.path)
  }
  function cancelEdit() { setEditingPath(null) }
  // commitEdit recebe tipoErro/comentario do EditForm (estado interno dele)
  async function commitEdit(tipoErro: string | null, comentario: string) {
    let toStore = editValue
    if (!skipScale) {
      // Aceita formato BR (23.868.692,18) ou US (23868692.18)
      const n = parseNumericBR(editValue)
      if (n !== null) toStore = String(n / mult)
    }
    // Se valor não mudou (status='confirmado'), zera tipo_erro: não classifica algo que não foi corrigido
    const changed = toStore !== extracted
    const finalTipo = changed ? tipoErro : null
    const finalComent = changed ? comentario : ''
    await onSave(field.path, extracted, toStore, finalComent, finalTipo)
    setEditingPath(null)
  }
  async function restoreOriginal() {
    if (!correction) return
    await onDelete(field.path)
    setEditingPath(null)
  }

  // Total divergence indicator
  const llmTotalScaled = llmOriginal !== undefined ? llmOriginal : (parseFloat(extracted || '0') * mult)
  const totalDivergence = isTotal && field.type === 'number' && computedTotal !== undefined &&
    Math.abs((computedTotal ?? 0) - llmTotalScaled) > 0.01 && !isNaN(llmTotalScaled)
  const totalDelta = totalDivergence ? (computedTotal ?? 0) - llmTotalScaled : 0

  const labelEl = (
    <div className={`flex items-center gap-2 min-w-0 ${isEditing ? 'pt-2' : ''} ${isTotal ? 'font-medium text-gray-900' : 'text-gray-700'}`}>
      {isTotal && <IconChevronRight size={14} className="text-gray-400 shrink-0" />}
      <span className={`text-sm truncate ${isTotal ? '' : ''}`} title={field.label}>
        {field.label}
      </span>
    </div>
  )

  return (
    <div
      className={`group transition-all ${dimmed ? 'opacity-40' : ''} ${
        isEditing ? 'bg-blue-50/60 border-l-2 border-blue-400' : 'border-l-2 border-transparent hover:bg-gray-50'
      }`}
    >
      <div
        role="button"
        tabIndex={readOnly || isEditing ? -1 : 0}
        onClick={() => !isEditing && startEdit()}
        onKeyDown={(e) => {
          if ((e.key === 'Enter' || e.key === ' ') && !isEditing) {
            e.preventDefault()
            startEdit()
          }
        }}
        className={`grid grid-cols-[220px_1fr_120px] gap-3 ${isEditing ? 'items-start' : 'items-center'} px-4 py-2.5 border-b border-gray-100 ${
          isEditing ? 'cursor-default' : (readOnly ? 'cursor-default' : 'cursor-pointer')
        } focus:outline-none focus:bg-gray-50`}
      >
        {labelEl}

        {/* Value column */}
        <div className="flex items-center gap-2 min-w-0">
          {isEditing ? (
            <EditForm
              field={field}
              value={editValue}
              setValue={setEditValue}
              skipScale={skipScale}
              inputRef={inputRef}
              onCommit={commitEdit}
              onCancel={cancelEdit}
              onRestore={isReviewed ? restoreOriginal : undefined}
              initialTipo={correction?.tipo_erro ?? null}
              initialDetalhe={correction?.comentario ?? ''}
              llmOriginal={
                llmOriginal !== undefined
                  ? brl.format(llmOriginal)
                  : (field.type === 'number'
                    ? brl.format((parseFloat(extracted || '0') || 0) * mult)
                    : formatValue(extracted, field.type, scale, field.options))
              }
              saving={saving}
              valueChanged={valueChanged}
            />
          ) : (
            <>
              <span className={`font-mono text-sm tabular-nums truncate ${isReviewed ? 'font-medium text-gray-900' : 'text-gray-800'} ${isTotal ? 'font-semibold' : ''}`}>
                {effectiveFormatted || <span className="text-gray-300">—</span>}
              </span>
              {props.fonte && <SourceTooltip fonte={props.fonte} />}
              {!readOnly && !dimmed && (
                <IconPencil
                  size={13}
                  className={`text-gray-300 opacity-0 group-hover:opacity-100 transition-opacity shrink-0 ${isReviewed ? 'group-hover:text-blue-600' : 'group-hover:text-gray-500'}`}
                />
              )}
              {saved && (
                <span className="inline-flex items-center gap-0.5 text-[10px] text-emerald-600 font-medium">
                  <IconCheck size={11} /> Salvo
                </span>
              )}
            </>
          )}
        </div>

        {/* Origin column */}
        {!isEditing && (
          <div className="flex items-center justify-end gap-1 text-[11px] shrink-0">
            {totalDivergence ? (
              <span
                className="inline-flex items-center gap-1 text-amber-600 cursor-help"
                title={`Total do LLM: ${brl.format(llmTotalScaled)} · diferença: ${totalDelta >= 0 ? '+' : ''}${brl.format(totalDelta)}`}
              >
                <IconAlertCircle size={12} /> Difere {totalDelta >= 0 ? '+' : ''}{brl.format(totalDelta)}
              </span>
            ) : isReviewed ? (
              <span
                className="inline-flex items-center gap-1 text-blue-600 font-medium"
                title={
                  valueChanged
                    ? `Corrigido${correction?.confirmado_em ? ` em ${(correction.confirmado_em || '').substring(0, 10).split('-').reverse().join('/')}` : ''} · Original do LLM: ${formatValue(extracted, field.type, scale, field.options)}`
                    : `Confirmado${correction?.confirmado_em ? ` em ${(correction.confirmado_em || '').substring(0, 10).split('-').reverse().join('/')}` : ''}`
                }
              >
                <IconUserCheck size={12} /> Revisado
              </span>
            ) : isTotal ? (
              <span /> /* totais sem divergência: vazio */
            ) : (
              <span className="inline-flex items-center gap-1 text-gray-400">
                <IconSparkles size={12} /> LLM
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Edit form (inline) ──────────────────────────────────────────────────────

// 8 tipos de erro (id interno + label UI)
const TIPO_ERRO_OPTS: { id: string; label: string }[] = [
  { id: 'conta_campo_errado',   label: 'Conta no campo errado' },
  { id: 'faltou_somar_contas',  label: 'Faltou somar contas' },
  { id: 'campo_vazio_incorreto', label: 'Faltou extrair valor' },
  { id: 'numero_errado',        label: 'Valor numérico incorreto' },
  { id: 'escala_errada',        label: 'Escala errada (milhar/unidade)' },
  { id: 'sinal_trocado',        label: 'Sinal trocado (+/−)' },
  { id: 'conta_inventada',      label: 'Valor não existe no PDF' },
  { id: 'outro',                label: 'Outro' },
]

const PLACEHOLDERS_BY_TIPO: Record<string, string> = {
  conta_campo_errado:    'Ex: deveria estar em "Conta corrente sócios", não em "Outros ativos"',
  faltou_somar_contas:   'Ex: deveria somar Empréstimos + Debêntures',
  campo_vazio_incorreto: 'Ex: PDF mostra R$ 850.000 em Encargos Financeiros, LLM perdeu',
  numero_errado:         'Ex: PDF diz 12.500.000, LLM extraiu 1.250.000',
  escala_errada:         'Ex: documento em mil reais, mas LLM extraiu como unidade',
  sinal_trocado:         'Ex: provisão deveria ser negativa, LLM extraiu positiva',
  conta_inventada:       'Ex: esse valor não aparece em nenhuma linha do PDF',
  outro:                 'Descreva o que está errado',
}

function EditForm({
  field, value, setValue, skipScale, inputRef, onCommit, onCancel, onRestore,
  llmOriginal, saving, valueChanged, initialTipo, initialDetalhe,
}: {
  field: FieldDef
  value: string
  setValue: (v: string) => void
  skipScale: boolean
  inputRef: React.RefObject<HTMLInputElement>
  onCommit: (tipoErro: string | null, comentario: string) => void
  onCancel: () => void
  onRestore?: () => void
  llmOriginal: string
  saving: boolean
  valueChanged: boolean
  initialTipo: string | null
  initialDetalhe: string
}) {
  void valueChanged
  // Se já existe uma classificação prévia (re-edit), abre expandido
  const [expanded, setExpanded] = useState<boolean>(!!initialTipo || !!initialDetalhe)
  const [tipoErro, setTipoErro] = useState<string | null>(initialTipo)
  const [detalhe, setDetalhe] = useState<string>(initialDetalhe || '')
  // Foco automático no input ao abrir o editor
  useEffect(() => {
    const t = setTimeout(() => inputRef.current?.focus(), 10)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  function handleKey(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'Enter') { e.preventDefault(); onCommit(tipoErro, detalhe.trim()) }
    else if (e.key === 'Escape') { e.preventDefault(); onCancel() }
  }
  function toggleExpand() {
    // Aviso antes de recolher se há estado preenchido
    if (expanded && (tipoErro || detalhe.trim())) {
      const ok = window.confirm('Recolher vai descartar o tipo de erro e detalhe. Continuar?')
      if (!ok) return
      setTipoErro(null)
      setDetalhe('')
    }
    setExpanded(v => !v)
  }
  function togglePill(id: string) {
    setTipoErro(prev => (prev === id ? null : id))
  }
  function restoreLocal() {
    if (onRestore) {
      setTipoErro(null)
      setDetalhe('')
      onRestore()
    }
  }
  const placeholder = tipoErro ? PLACEHOLDERS_BY_TIPO[tipoErro] : 'Selecione um tipo de erro para ver exemplos'

  return (
    <div className="w-full flex flex-col gap-2 py-1" onClick={e => e.stopPropagation()}>
      {/* Linha principal: input + ações */}
      <div className="flex items-center gap-2">
        {field.type === 'enum' && field.options ? (
          field.options.length <= 4 ? (
            <div className="flex flex-wrap gap-2">
              {field.options.map(opt => (
                <label
                  key={opt.value}
                  className={`inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md border cursor-pointer ${
                    String(opt.value) === value
                      ? 'bg-blue-50 border-blue-400 text-blue-700'
                      : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <input
                    type="radio"
                    className="sr-only"
                    name={`enum-${field.path}`}
                    checked={String(opt.value) === value}
                    onChange={() => setValue(String(opt.value))}
                  />
                  <span className="font-semibold">{opt.value}</span> · {opt.label}
                </label>
              ))}
            </div>
          ) : (
            <select
              value={value}
              onChange={e => setValue(e.target.value)}
              className="font-mono text-sm px-2 py-1.5 border border-gray-300 rounded-md w-48"
              autoFocus
            >
              <option value="">—</option>
              {field.options.map(o => <option key={o.value} value={o.value}>{o.value} · {o.label}</option>)}
            </select>
          )
        ) : (
          <>
            {field.type === 'number' && !skipScale && (
              <span className="text-xs text-gray-400 font-medium">R$</span>
            )}
            <input
              ref={inputRef}
              type={field.type === 'date' ? 'date' : 'text'}
              value={value}
              onChange={e => setValue(e.target.value)}
              onKeyDown={handleKey}
              onBlur={() => {
                // Re-formata para BR ao perder foco (apenas number, sem skipScale)
                if (field.type === 'number' && !skipScale) {
                  const n = parseNumericBR(value)
                  if (n !== null) setValue(formatBRNumber(n))
                }
              }}
              className="font-mono text-sm tabular-nums px-2 py-1.5 border border-gray-300 rounded-md w-[180px] focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
              autoFocus
            />
          </>
        )}
        <div className="flex items-center gap-1.5 ml-auto">
          <button
            onClick={onCancel}
            disabled={saving}
            className="inline-flex items-center gap-1 text-xs px-2.5 py-1 rounded-md border border-gray-200 text-gray-600 hover:bg-gray-50 disabled:opacity-40"
          >
            <IconX size={12} /> Cancelar
          </button>
          <button
            onClick={() => onCommit(tipoErro, detalhe.trim())}
            disabled={saving}
            className="inline-flex items-center gap-1 text-xs px-2.5 py-1 rounded-md bg-blue-600 text-white hover:bg-blue-700 font-medium disabled:opacity-40"
          >
            <IconCheck size={12} /> {saving ? 'Salvando…' : 'Salvar'}
          </button>
        </div>
      </div>

      {/* Linha meta: original + restaurar + adicionar contexto */}
      <div className="flex items-center gap-3 text-[11px]">
        <span className="text-gray-500">Original do LLM: <span className="font-mono">{llmOriginal || '—'}</span></span>
        {onRestore && (
          <button
            onClick={(e) => { e.stopPropagation(); restoreLocal() }}
            className="inline-flex items-center gap-1 text-blue-600 hover:underline"
          >
            <IconArrowBackUp size={11} /> Restaurar original
          </button>
        )}
        <button
          onClick={toggleExpand}
          className={`inline-flex items-center gap-1 hover:underline ${expanded ? 'text-gray-500' : 'text-blue-600'}`}
        >
          <IconTag size={11} /> {expanded ? 'Recolher contexto' : 'Adicionar contexto'}
          {tipoErro && !expanded && (
            <span className="ml-1 text-[10px] bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">
              {TIPO_ERRO_OPTS.find(t => t.id === tipoErro)?.label ?? tipoErro}
            </span>
          )}
        </button>
        <span className="text-gray-400 ml-auto">Enter • Esc</span>
      </div>

      {/* Linha contexto (expandido) */}
      {expanded && (
        <div className="pt-2 mt-1 border-t border-gray-200 flex flex-col gap-2 animate-fade-in">
          <div>
            <p className="text-[11px] font-medium text-gray-600 mb-1">Tipo de erro</p>
            <div className="flex flex-wrap gap-1.5" role="radiogroup">
              {TIPO_ERRO_OPTS.map(opt => {
                const checked = tipoErro === opt.id
                return (
                  <button
                    key={opt.id}
                    role="radio"
                    aria-checked={checked}
                    onClick={() => togglePill(opt.id)}
                    className={`text-[11px] px-2.5 py-1 rounded-full border-2 transition-all ${
                      checked
                        ? 'bg-blue-600 border-blue-700 text-white font-semibold shadow-sm'
                        : 'bg-white border-gray-200 text-gray-600 hover:bg-gray-50 hover:border-gray-300'
                    }`}
                  >
                    {checked && <span className="mr-1">✓</span>}
                    {opt.label}
                  </button>
                )
              })}
            </div>
          </div>
          <div>
            <p className="text-[11px] font-medium text-gray-600 mb-1">
              Detalhe <span className="text-gray-400 font-normal">(opcional)</span>
            </p>
            <input
              type="text"
              value={detalhe}
              onChange={e => setDetalhe(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') { e.preventDefault(); onCommit(tipoErro, detalhe.trim()) }
                else if (e.key === 'Escape') { e.preventDefault(); onCancel() }
              }}
              placeholder={placeholder}
              className="w-full max-w-[600px] text-[12px] px-2 py-1.5 border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
        </div>
      )}
    </div>
  )
}

// ─── FieldRowAudit (4-column audit mode) ─────────────────────────────────────

function FieldRowAudit(props: RowCommon) {
  const { field, extracted, correction, scale, computedTotal, llmOriginal } = props
  const isReviewed = !!correction
  const mult = getScaleMultiplier(scale)
  const skipScale = field.type === 'enum' || field.type === 'text' || field.type === 'date'

  // LLM value (raw): prefer postprocessed original if available
  const llmFormatted = llmOriginal !== undefined
    ? brl.format(llmOriginal)
    : formatValue(extracted, field.type, scale, field.options)
  const finalRaw = isReviewed ? correction!.valor_correto : extracted
  const finalFormatted = field.isTotal && field.type === 'number' && computedTotal !== undefined
    ? brl.format(computedTotal)
    : formatValue(finalRaw, field.type, scale, field.options)

  // Delta
  const llmNum = skipScale ? NaN : (llmOriginal !== undefined ? llmOriginal : (parseFloat(extracted || '0') * mult))
  const finalNum = skipScale ? NaN : (field.isTotal && computedTotal !== undefined ? computedTotal : (parseFloat(finalRaw || '0') * mult))
  const diverges = !isNaN(llmNum) && !isNaN(finalNum) && Math.abs(llmNum - finalNum) > 0.01
  const delta = diverges ? finalNum - llmNum : 0

  return (
    <div className={`grid grid-cols-[200px_1fr_1fr_auto] gap-3 items-center px-4 py-2.5 border-b border-gray-100 ${diverges ? 'bg-blue-50/40' : ''}`}>
      <span className="text-sm text-gray-700 truncate" title={field.label}>{field.label}</span>
      <span className={`font-mono text-sm tabular-nums text-gray-400 ${diverges ? 'line-through' : ''}`}>
        {llmFormatted || '—'}
      </span>
      <span className={`font-mono text-sm tabular-nums ${isReviewed ? 'font-medium text-gray-900' : 'text-gray-800'}`}>
        {finalFormatted || '—'}
      </span>
      <div className="text-[11px] shrink-0 min-w-[80px] text-right inline-flex items-center justify-end gap-1.5">
        {props.fonte && <SourceTooltip fonte={props.fonte} />}
        {diverges ? (
          <span className="inline-flex items-center gap-1 text-blue-600">
            <IconPencil size={11} /> {delta >= 0 ? '+' : ''}{brl.format(delta)}
          </span>
        ) : (
          <span className="text-gray-300">=</span>
        )}
      </div>
    </div>
  )
}

// ─── SourceTooltip ────────────────────────────────────────────────────────────
// "i" icon with hover tooltip showing the PDF source text used by the LLM.
// Renders via portal to escape parent overflow:hidden. Position fixed with flip.

function SourceTooltip({ fonte }: { fonte: FonteValue }) {
  const [pos, setPos] = useState<{ x: number; y: number; flip: boolean } | null>(null)
  const iconRef = useRef<HTMLSpanElement>(null)
  const timer = useRef<number | null>(null)

  const isRich = Array.isArray(fonte)
  const tooltipWidth = isRich ? 320 : 280

  function show() {
    timer.current = window.setTimeout(() => {
      if (!iconRef.current) return
      const r = iconRef.current.getBoundingClientRect()
      const flip = r.right + tooltipWidth + 16 > window.innerWidth
      setPos({
        x: flip ? r.left - 8 : r.right + 8,
        y: r.top + r.height / 2,
        flip,
      })
    }, 300)
  }
  function hide() {
    if (timer.current) {
      clearTimeout(timer.current)
      timer.current = null
    }
    setPos(null)
  }

  const brlPlain = new Intl.NumberFormat('pt-BR', {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  })
  const totalCalc = isRich
    ? (fonte as FonteItem[]).reduce((acc, it) => acc + (it.operacao === '-' ? -it.valor : it.valor), 0)
    : 0

  return (
    <>
      <span
        ref={iconRef}
        onMouseEnter={show}
        onMouseLeave={hide}
        onClick={(e) => e.stopPropagation()}
        className="inline-flex items-center text-gray-400 hover:text-gray-600 cursor-help shrink-0"
      >
        <IconInfoCircle size={13} />
      </span>
      {pos &&
        createPortal(
          <div
            style={{
              position: 'fixed',
              top: pos.y,
              left: pos.x,
              transform: `translate(${pos.flip ? '-100%' : '0'}, -50%)`,
              zIndex: 100,
              maxWidth: tooltipWidth,
            }}
            className="bg-gray-50 border border-gray-200 rounded-md px-3 py-2.5 shadow-md pointer-events-none"
          >
            <div
              className="text-[11px] font-medium text-gray-500 uppercase mb-2"
              style={{ letterSpacing: '0.04em' }}
            >
              Fonte do PDF
            </div>

            {isRich ? (
              <>
                <div
                  className="text-[13px] text-gray-700"
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '14px 1fr auto',
                    columnGap: 8,
                    rowGap: 6,
                    alignItems: 'baseline',
                  }}
                >
                  {(fonte as FonteItem[]).map((it, i) => (
                    <span key={i} style={{ display: 'contents' }}>
                      <span
                        className={`font-mono ${it.operacao === '-' ? 'text-red-600 font-semibold' : 'text-gray-400'}`}
                      >
                        {it.operacao === '-' ? '−' : '+'}
                      </span>
                      <span className="break-words">{it.nome}</span>
                      <span className="font-mono text-gray-600 text-right tabular-nums">
                        {brlPlain.format(it.valor)}
                      </span>
                    </span>
                  ))}
                </div>
                <div
                  className="text-[13px] mt-2 pt-1.5 border-t border-gray-200"
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '14px 1fr auto',
                    columnGap: 8,
                    rowGap: 6,
                    alignItems: 'baseline',
                  }}
                >
                  <span className="font-mono text-gray-400">=</span>
                  <span className="text-gray-500">Total</span>
                  <span className="font-mono font-medium text-gray-900 text-right tabular-nums">
                    {brlPlain.format(totalCalc)}
                  </span>
                </div>
              </>
            ) : (
              <div className="text-[13px] text-gray-700 break-words">{fonte as string}</div>
            )}
          </div>,
          document.body
        )}
    </>
  )
}

