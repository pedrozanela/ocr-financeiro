import { DocSummary } from '../App'

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
            {doc.cnpj && (
              <p className="text-xs text-gray-500 mt-0.5">{doc.cnpj}</p>
            )}
          </button>
        </li>
      ))}
    </ul>
  )
}
