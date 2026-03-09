import { useState } from 'react'
import { SectionDef } from './fieldDefinitions'
import { CorrectionsMap } from './FinancialReview'

const brl = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
})

function formatValue(raw: string, type: 'number' | 'text' | 'date'): string {
  if (!raw || raw === '' || raw === 'null') return ''
  if (type !== 'number') return raw
  const n = parseFloat(raw)
  if (isNaN(n)) return raw
  return brl.format(n)
}

interface Props {
  section: SectionDef
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
  corrections: CorrectionsMap
  saving: string | null
  saved: string | null
  getValue: (obj: unknown, path: string) => string
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string) => Promise<void>
  onDelete: (campo: string) => Promise<void>
}

export default function FieldSection({ section, data, corrections, saving, saved, getValue, onSave, onDelete }: Props) {
  return (
    <div className="space-y-2">
      {section.fields.map(field => {
        const extracted = getValue(data, field.path)
        const correction = corrections[field.path]
        return (
          <FieldRow
            key={field.path}
            label={field.label}
            path={field.path}
            extracted={extracted}
            extractedFormatted={formatValue(extracted, field.type)}
            correctionFormatted={correction ? formatValue(correction.valor_correto, field.type) : undefined}
            correction={correction}
            isTotal={field.isTotal ?? false}
            saving={saving === field.path}
            saved={saved === field.path}
            onSave={onSave}
            onDelete={onDelete}
          />
        )
      })}
    </div>
  )
}

interface RowProps {
  label: string
  path: string
  extracted: string
  extractedFormatted: string
  correctionFormatted?: string
  correction?: { campo: string; valor_correto: string; comentario: string }
  isTotal: boolean
  saving: boolean
  saved: boolean
  onSave: (campo: string, valorExtraido: string, valorCorreto: string, comentario: string) => Promise<void>
  onDelete: (campo: string) => Promise<void>
}

const ERROR_TAGS = [
  'Dígito errado',
  'Escala incorreta (mil/milhão)',
  'Campo trocado',
  'Sinal invertido (+/-)',
  'Valor ausente no PDF',
]

function FieldRow({ label, path, extracted, extractedFormatted, correctionFormatted, correction, isTotal, saving, saved, onSave, onDelete }: RowProps) {
  const [editing, setEditing] = useState(false)
  const [corrValue, setCorrValue] = useState(correction?.valor_correto ?? extracted)
  const [comment, setComment] = useState(correction?.comentario ?? '')
  const [freeText, setFreeText] = useState(false)

  const hasCorrBadge = !!correction

  function startEdit() {
    const existingComment = correction?.comentario ?? ''
    setCorrValue(correction?.valor_correto ?? extracted)
    setComment(existingComment)
    setFreeText(!!existingComment && !ERROR_TAGS.includes(existingComment))
    setEditing(true)
  }

  function selectTag(tag: string) {
    setComment(tag)
    setFreeText(false)
  }

  function activateFreeText() {
    setComment('')
    setFreeText(true)
  }

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

  const baseRow = isTotal
    ? 'bg-gray-100 rounded-lg'
    : 'bg-white rounded-lg border border-gray-100'

  return (
    <div className={`px-4 py-3 ${baseRow} ${hasCorrBadge ? 'ring-1 ring-amber-300' : ''}`}>
      <div className="flex items-center gap-3">
        {/* Label */}
        <div className="w-56 shrink-0">
          <span className={`text-sm ${isTotal ? 'font-semibold text-gray-800' : 'text-gray-600'}`}>
            {label}
          </span>
        </div>

        {/* Extracted value */}
        <div className="flex-1 min-w-0">
          <span className={`text-sm font-mono ${
            hasCorrBadge ? 'line-through text-gray-400' : isTotal ? 'font-bold text-gray-900' : 'text-gray-800'
          }`}>
            {extractedFormatted || <span className="text-gray-300 italic not-italic font-sans">—</span>}
          </span>
          {hasCorrBadge && (
            <span className="ml-2 text-sm font-mono font-semibold text-amber-700">
              → {correctionFormatted ?? correction!.valor_correto}
            </span>
          )}
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2 shrink-0">
          {saved && (
            <span className="text-xs text-green-600 font-medium">✓ Salvo</span>
          )}
          {hasCorrBadge && !editing && (
            <button
              onClick={() => onDelete(path)}
              className="text-xs text-red-400 hover:text-red-600 transition-colors"
            >
              remover
            </button>
          )}
          {!editing && (
            <button
              onClick={startEdit}
              className={`text-xs px-2.5 py-1 rounded transition-colors ${
                hasCorrBadge
                  ? 'bg-amber-100 text-amber-700 hover:bg-amber-200'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {hasCorrBadge ? 'editar' : 'corrigir'}
            </button>
          )}
        </div>
      </div>

      {/* Edit form */}
      {editing && (
        <div className="mt-3 pl-56 space-y-2">
          <div className="flex gap-2 items-center">
            <label className="text-xs text-gray-500 w-24 shrink-0">Valor correto</label>
            <input
              value={corrValue}
              onChange={e => setCorrValue(e.target.value)}
              className="flex-1 text-sm font-mono border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-blue-400"
              placeholder="Valor correto…"
              autoFocus
            />
          </div>
          <div className="flex gap-2 items-start">
            <label className="text-xs text-gray-500 w-24 shrink-0 pt-1">Tipo de erro</label>
            <div className="flex-1 space-y-1.5">
              <div className="flex flex-wrap gap-1.5">
                {ERROR_TAGS.map(tag => (
                  <button
                    key={tag}
                    type="button"
                    onClick={() => selectTag(tag)}
                    className={`text-xs px-2 py-0.5 rounded-full border transition-colors ${
                      comment === tag && !freeText
                        ? 'bg-[#0F2137] text-white border-[#0F2137]'
                        : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
                    }`}
                  >
                    {tag}
                  </button>
                ))}
                <button
                  type="button"
                  onClick={activateFreeText}
                  className={`text-xs px-2 py-0.5 rounded-full border transition-colors ${
                    freeText
                      ? 'bg-[#0F2137] text-white border-[#0F2137]'
                      : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
                  }`}
                >
                  ✏ Outro…
                </button>
              </div>
              {freeText && (
                <input
                  value={comment}
                  onChange={e => setComment(e.target.value)}
                  className="w-full text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="Descreva o erro…"
                  autoFocus
                />
              )}
            </div>
          </div>
          <div className="flex gap-2 justify-end">
            <button
              onClick={handleCancel}
              className="text-xs px-3 py-1 rounded bg-gray-100 text-gray-600 hover:bg-gray-200 transition-colors"
            >
              Cancelar
            </button>
            <button
              onClick={handleSave}
              disabled={saving}
              className="text-xs px-3 py-1 rounded bg-[#0F2137] text-white hover:bg-[#1a3050] disabled:opacity-50 transition-colors"
            >
              {saving ? 'Salvando…' : 'Salvar correção'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
