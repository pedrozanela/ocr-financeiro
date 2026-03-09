import { DocSummary } from '../App'

function fmt(v: number | string | null) {
  if (v == null) return '—'
  const n = typeof v === 'string' ? parseFloat(v) : v
  if (isNaN(n)) return '—'
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(n)
}

interface Props {
  docs: DocSummary[]
  selected: string | null
  onSelect: (name: string) => void
}

export default function DocumentList({ docs, selected, onSelect }: Props) {
  return (
    <ul className="flex-1 overflow-y-auto divide-y divide-gray-100">
      {docs.map(doc => (
        <li key={doc.document_name}>
          <button
            onClick={() => onSelect(doc.document_name)}
            className={`w-full text-left px-4 py-3 hover:bg-gray-50 transition-colors ${
              selected === doc.document_name ? 'bg-slate-50 border-l-4 border-[#0F2137]' : 'border-l-4 border-transparent'
            }`}
          >
            <p className="text-xs font-semibold text-gray-800 truncate">
              {doc.razao_social ?? doc.document_name}
            </p>
            <p className="text-xs text-gray-400 mt-0.5 truncate">{doc.document_name}</p>
            <div className="flex gap-3 mt-1">
              {doc.periodo && (
                <span className="text-xs text-gray-500">{doc.periodo.substring(0, 10)}</span>
              )}
              {doc.ativo_total != null && (
                <span className="text-xs text-blue-600">{fmt(doc.ativo_total)}</span>
              )}
            </div>
          </button>
        </li>
      ))}
    </ul>
  )
}
