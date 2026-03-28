import { DocSummary } from '../App'

interface Props {
  docs: DocSummary[]
  selected: string | null
  onSelect: (name: string) => void
  search: string
}

function getInitials(name: string | null, fallback: string): string {
  if (!name) return fallback.slice(0, 2).toUpperCase()
  const words = name.trim().split(/\s+/)
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase()
  return (words[0][0] + words[1][0]).toUpperCase()
}

const AVATAR_COLORS = [
  'bg-blue-500', 'bg-violet-500', 'bg-emerald-500', 'bg-rose-500',
  'bg-amber-500', 'bg-cyan-500', 'bg-indigo-500', 'bg-teal-500',
]

function avatarColor(name: string): string {
  let hash = 0
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash)
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length]
}

function docStatus(doc: DocSummary): 'ok' | 'partial' | 'empty' {
  const hasAtivo = doc.ativo_total != null && Number(doc.ativo_total) !== 0
  const hasLucro = doc.lucro_liquido != null && doc.lucro_liquido !== '' && doc.lucro_liquido !== 0
  if (hasAtivo && hasLucro) return 'ok'
  if (hasAtivo || hasLucro) return 'partial'
  return 'empty'
}

const STATUS_CLASSES = {
  ok:      'bg-emerald-400',
  partial: 'bg-amber-400',
  empty:   'bg-white/20',
}

const STATUS_TITLES = {
  ok:      'Ativo Total e Lucro Líquido extraídos',
  partial: 'Extração parcial',
  empty:   'Dados não extraídos',
}

export default function DocumentList({ docs, selected, onSelect, search }: Props) {
  const filtered = docs.filter(doc => {
    if (!search.trim()) return true
    const q = search.toLowerCase()
    return (
      doc.razao_social?.toLowerCase().includes(q) ||
      doc.document_name.toLowerCase().includes(q) ||
      doc.cnpj?.toLowerCase().includes(q)
    )
  })

  if (docs.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center px-6 text-center pb-4">
        <div className="w-10 h-10 rounded-full bg-white/10 flex items-center justify-center mb-3">
          <svg className="w-5 h-5 text-white/30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M9 13h6m-3-3v6m5 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
        </div>
        <p className="text-xs text-white/30">Nenhum documento</p>
        <p className="text-[10px] text-white/20 mt-1">Envie um PDF para começar</p>
      </div>
    )
  }

  if (filtered.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center px-6 text-center pb-4">
        <p className="text-xs text-white/30">Nenhum resultado para</p>
        <p className="text-xs text-white/50 mt-1 font-medium">"{search}"</p>
      </div>
    )
  }

  return (
    <ul className="flex-1 overflow-y-auto px-3 py-1 space-y-0.5">
      {filtered.map(doc => {
        const isSelected = selected === doc.document_name
        const initials = getInitials(doc.razao_social, doc.document_name)
        const color = avatarColor(doc.document_name)
        const status = docStatus(doc)

        return (
          <li key={doc.document_name}>
            <button
              onClick={() => onSelect(doc.document_name)}
              className={`w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-2.5 transition-all group ${
                isSelected ? 'bg-white/15 ring-1 ring-white/20' : 'hover:bg-white/8'
              }`}
            >
              {/* Avatar */}
              <div className={`w-8 h-8 rounded-lg ${color} flex items-center justify-center shrink-0 relative`}>
                <span className="text-[10px] font-bold text-white">{initials}</span>
                {/* Status dot */}
                <span
                  className={`absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 rounded-full border-2 border-[#0F2137] ${STATUS_CLASSES[status]}`}
                  title={STATUS_TITLES[status]}
                />
              </div>

              {/* Info */}
              <div className="flex-1 min-w-0">
                <p className={`text-xs font-semibold truncate leading-tight ${isSelected ? 'text-white' : 'text-white/80'}`}>
                  {doc.razao_social ?? doc.document_name}
                </p>
                {doc.cnpj && (
                  <p className="text-[10px] text-white/35 mt-0.5 truncate tabular-nums">{doc.cnpj}</p>
                )}
              </div>

              {isSelected && (
                <div className="w-1.5 h-1.5 rounded-full bg-white shrink-0" />
              )}
            </button>
          </li>
        )
      })}
    </ul>
  )
}
