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
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="divide-y divide-gray-50">
          {renderFields(section.fields)}
        </div>
      </div>
    )
  }

  // Grouped layout — Ativo and Passivo
  return (
    <div className="space-y-3">
      {groups.map(group => {
        if (group.isRoot) {
          // Root-level totals rendered as standalone cards
          return (
            <div key={group.key} className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
              <div className="divide-y divide-gray-50">
                {renderFields(group.fields)}
              </div>
            </div>
          )
        }

        const isOpen = !collapsed.has(group.key)
        const corrCount = group.fields.filter(f => corrections[f.path]).length
        // Last isTotal field = group total
        const totalField = [...group.fields].reverse().find(f => f.isTotal)
        const totalValue = totalField ? formatValue(getValue(data, totalField.path), totalField.type, scale) : null

        return (
          <div key={group.key} className="border border-gray-200 rounded-xl shadow-sm overflow-hidden">
            {/* Group header */}
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

              {totalValue && (
                <span className="ml-auto text-sm font-mono font-bold text-gray-800 tabular-nums">{totalValue}</span>
              )}
            </button>

            {isOpen && (
              <div className="bg-white divide-y divide-gray-50 animate-fade-in">
                {renderFields(group.fields)}
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

function FieldRow({ label, path, extracted, extractedFormatted, correctionFormatted, correction, assessmentItem, isTotal, saving, saved, onSave, onDelete, onConfirm }: RowProps) {
  const [editing, setEditing]       = useState(false)
  const [corrValue, setCorrValue]   = useState(correction?.valor_correto ?? extracted)
  const [comment, setComment]       = useState(correction?.comentario ?? '')
  const [freeText, setFreeText]     = useState(false)
  const [confirming, setConfirming] = useState(false)

  const isConfirmed  = correction?.status === 'confirmado'
  const hasCorrBadge = !!correction

  function startEdit() {
    setCorrValue(correction?.valor_correto ?? extracted)
    setComment(correction?.comentario ?? '')
    setFreeText(!!correction?.comentario && !ERROR_TAGS.includes(correction.comentario))
    setEditing(true)
  }

  function selectTag(tag: string) { setComment(tag); setFreeText(false) }
  function activateFreeText()     { setComment(''); setFreeText(true) }

  async function handleSave() {
    await onSave(path, extracted, corrValue, comment)
    setEditing(false)
  }

  function handleCancel() {
    setCorrValue(correction?.valor_correto ?? extracted)
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
        </div>

        {/* Value */}
        <div className="flex-1 min-w-0 flex items-center gap-2">
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
