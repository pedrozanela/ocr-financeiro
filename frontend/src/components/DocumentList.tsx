import { useEffect, useMemo, useState } from 'react'
import { DocSummary } from '../App'
import {
  IconSearch, IconSend, IconCheck, IconArrowsSort, IconAlertCircle, IconChecks,
} from '@tabler/icons-react'

interface Props {
  docs: DocSummary[]
  selected: string | null
  onSelect: (name: string) => void
  onDelete: (name: string) => void
  search: string
  lastSyncAt?: number | null
  syncFailures?: number
  onRefresh?: () => void
}

type TabKey = 'pendentes' | 'concluidos'
type SortKey = 'recente' | 'antigo' | 'alfa' | 'status'

const TAB_LS_KEY = 'techfin.sidebar.tab'
const SORT_LS_KEYS: Record<TabKey, string> = {
  pendentes: 'techfin.sidebar.sort.pendentes',
  concluidos: 'techfin.sidebar.sort.concluidos',
}

const PALETTE = [
  '#a855f7', '#06b6d4', '#14b8a6', '#f97316',
  '#ec4899', '#8b5cf6', '#22c55e', '#3b82f6',
  '#ef4444', '#eab308', '#10b981', '#6366f1',
]

function avatarColor(cnpj: string | null, docName: string): string {
  const seed = (cnpj || docName).replace(/\D/g, '') || docName
  let hash = 0
  for (const ch of seed) hash = (hash * 31 + ch.charCodeAt(0)) | 0
  return PALETTE[Math.abs(hash) % PALETTE.length]
}

function getInitials(name: string | null, fallback: string): string {
  const src = name?.trim() || fallback
  const words = src.split(/\s+/).filter(Boolean)
  if (words.length === 0) return '??'
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase()
  return (words[0][0] + words[1][0]).toUpperCase()
}

function formatCnpj(cnpj: string | null): string {
  const d = (cnpj || '').replace(/\D/g, '')
  if (d.length !== 14) return cnpj || ''
  return `${d.slice(0,2)}.${d.slice(2,5)}.${d.slice(5,8)}/${d.slice(8,12)}-${d.slice(12,14)}`
}

// ─── Agrupamento temporal (America/Sao_Paulo) ──────────────────────────────
function getDocumentGroup(criadoEm: string | null): string {
  if (!criadoEm) return 'Sem data'
  const doc = new Date(criadoEm)
  const now = new Date()
  const tz = 'America/Sao_Paulo'
  const docDay = doc.toLocaleDateString('en-CA', { timeZone: tz })
  const nowDay = now.toLocaleDateString('en-CA', { timeZone: tz })
  if (docDay === nowDay) return 'Hoje'
  const yest = new Date(now); yest.setDate(yest.getDate() - 1)
  if (docDay === yest.toLocaleDateString('en-CA', { timeZone: tz })) return 'Ontem'
  // Início da semana (segunda 00:00 em SP)
  const wkday = now.toLocaleDateString('en-US', { timeZone: tz, weekday: 'short' })
  const offset: Record<string, number> = { Mon: 0, Tue: 1, Wed: 2, Thu: 3, Fri: 4, Sat: 5, Sun: 6 }
  const start = new Date(now); start.setDate(start.getDate() - (offset[wkday] ?? 0))
  const startStr = start.toLocaleDateString('en-CA', { timeZone: tz })
  return docDay >= startStr ? 'Esta semana' : 'Mais antigos'
}

const GROUP_ORDER = ['Hoje', 'Ontem', 'Esta semana', 'Mais antigos', 'Sem data']

// ─── Sub-texto por status ──────────────────────────────────────────────────
function metaText(doc: DocSummary): string {
  if (doc.status === 'submetido' && doc.submetido_em) {
    const d = new Date(doc.submetido_em)
    const now = new Date()
    const tz = 'America/Sao_Paulo'
    const docDay = d.toLocaleDateString('en-CA', { timeZone: tz })
    const nowDay = now.toLocaleDateString('en-CA', { timeZone: tz })
    const yest = new Date(now); yest.setDate(yest.getDate() - 1)
    const time = d.toLocaleTimeString('pt-BR', { timeZone: tz, hour: '2-digit', minute: '2-digit' })
    if (docDay === nowDay) return `submetido hoje ${time}`
    if (docDay === yest.toLocaleDateString('en-CA', { timeZone: tz })) return `submetido ontem ${time}`
    return `submetido em ${d.toLocaleDateString('pt-BR', { timeZone: tz })}`
  }
  if (doc.status === 'em_revisao') return `em revisão · ${doc.revisado_count} revisado${doc.revisado_count !== 1 ? 's' : ''}`
  if (doc.status === 'erro_submissao') return 'falha no envio'
  return 'não revisado'
}

const STATUS_DOT: Record<DocSummary['status'], string> = {
  nao_revisado:   'bg-gray-400',
  em_revisao:     'bg-blue-500',
  submetido:      'bg-emerald-500',
  erro_submissao: 'bg-red-500',
}

const STATUS_TEXT: Record<DocSummary['status'], string> = {
  nao_revisado:   'text-gray-400',
  em_revisao:     'text-blue-300',
  submetido:      'text-white/50',
  erro_submissao: 'text-red-300',
}

const STATUS_PRIORITY: Record<DocSummary['status'], number> = {
  nao_revisado: 0, em_revisao: 1, erro_submissao: 2, submetido: 3,
}

// ─── Componente principal ─────────────────────────────────────────────────
export default function DocumentList({
  docs, selected, onSelect, onDelete, search,
  syncFailures = 0, onRefresh,
}: Props) {
  void onDelete  // delete via menu de contexto/detalhe; sidebar não mostra trash agora
  const [tab, setTab] = useState<TabKey>(() => (localStorage.getItem(TAB_LS_KEY) as TabKey) || 'pendentes')
  const [sortMenuOpen, setSortMenuOpen] = useState(false)
  const [sortBy, setSortBy] = useState<SortKey>(() => (localStorage.getItem(SORT_LS_KEYS[tab]) as SortKey) || 'recente')
  const [resubmitting, setResubmitting] = useState<string | null>(null)
  const [resubmitConfirm, setResubmitConfirm] = useState<DocSummary | null>(null)

  useEffect(() => { localStorage.setItem(TAB_LS_KEY, tab) }, [tab])
  useEffect(() => { localStorage.setItem(SORT_LS_KEYS[tab], sortBy) }, [sortBy, tab])
  useEffect(() => {
    // Ao trocar de aba, restaura o sort dela
    const saved = localStorage.getItem(SORT_LS_KEYS[tab]) as SortKey
    if (saved) setSortBy(saved)
  }, [tab])

  // Particiona em pendentes vs concluídos
  const { pendentes, concluidos } = useMemo(() => {
    const p: DocSummary[] = []
    const c: DocSummary[] = []
    for (const d of docs) {
      if (d.status === 'submetido') c.push(d)
      else p.push(d)
    }
    return { pendentes: p, concluidos: c }
  }, [docs])

  // Filtro + ordenação
  const visible = useMemo(() => {
    const base = tab === 'pendentes' ? pendentes : concluidos
    const q = search.trim().toLowerCase()
    const filtered = !q ? base : base.filter(d => {
      const cnpjDigits = (d.cnpj || '').replace(/\D/g, '')
      const qDigits = q.replace(/\D/g, '')
      return (
        (d.razao_social || '').toLowerCase().includes(q) ||
        d.document_name.toLowerCase().includes(q) ||
        (qDigits && cnpjDigits.includes(qDigits))
      )
    })
    const sorted = [...filtered]
    sorted.sort((a, b) => {
      if (sortBy === 'alfa') return (a.razao_social || a.document_name).localeCompare(b.razao_social || b.document_name)
      if (sortBy === 'status') return STATUS_PRIORITY[a.status] - STATUS_PRIORITY[b.status]
      const tA = a.ingested_at ? new Date(a.ingested_at).getTime() : 0
      const tB = b.ingested_at ? new Date(b.ingested_at).getTime() : 0
      return sortBy === 'antigo' ? tA - tB : tB - tA
    })
    return sorted
  }, [tab, pendentes, concluidos, search, sortBy])

  // Agrupamento temporal (apenas quando ordenado por data)
  const groups = useMemo(() => {
    if (sortBy !== 'recente' && sortBy !== 'antigo') return [{ label: '', items: visible }]
    const byGroup = new Map<string, DocSummary[]>()
    for (const d of visible) {
      const g = getDocumentGroup(d.ingested_at)
      if (!byGroup.has(g)) byGroup.set(g, [])
      byGroup.get(g)!.push(d)
    }
    return GROUP_ORDER
      .filter(label => byGroup.has(label))
      .map(label => ({ label, items: byGroup.get(label)! }))
  }, [visible, sortBy])

  const stale = syncFailures >= 3

  async function handleResubmit(doc: DocSummary) {
    setResubmitting(doc.document_name)
    setResubmitConfirm(null)
    try {
      const r = await fetch(`/api/finalize/${encodeURIComponent(doc.document_name)}`, { method: 'POST' })
      if (!r.ok) {
        const body = await r.json().catch(() => ({}))
        alert(`Falha ao re-submeter: ${body.detail ?? r.status}`)
        return
      }
      if (onRefresh) await onRefresh()
    } finally {
      setResubmitting(null)
    }
  }

  // ─── Render ────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Tabs */}
      <div className="flex items-center gap-1 px-3 pt-2 pb-1 shrink-0">
        <TabButton active={tab === 'pendentes'} onClick={() => setTab('pendentes')}>
          Pendentes <span className="ml-1 opacity-60 tabular-nums">{pendentes.length}</span>
        </TabButton>
        <TabButton active={tab === 'concluidos'} onClick={() => setTab('concluidos')}>
          Concluídos <span className="ml-1 opacity-60 tabular-nums">{concluidos.length}</span>
        </TabButton>
      </div>

      {/* Sort + sync indicator */}
      <div className="flex items-center justify-between px-3 pb-1 shrink-0 text-[10px] text-white/40">
        <button
          onClick={() => setSortMenuOpen(v => !v)}
          className="relative inline-flex items-center gap-1 hover:text-white/70 transition-colors"
          title="Ordenar"
        >
          <IconArrowsSort size={11} />
          {sortBy === 'recente' && 'mais recentes'}
          {sortBy === 'antigo'  && 'mais antigos'}
          {sortBy === 'alfa'    && 'alfabético'}
          {sortBy === 'status'  && 'por status'}
          {sortMenuOpen && (
            <div className="absolute left-0 top-full mt-1 bg-[#0F2137] border border-white/15 rounded-md py-1 z-30 w-[140px] text-left">
              {(['recente','antigo','alfa','status'] as SortKey[]).map(k => (
                <div
                  key={k}
                  onClick={(e) => { e.stopPropagation(); setSortBy(k); setSortMenuOpen(false) }}
                  className={`px-2 py-1 hover:bg-white/10 cursor-pointer ${sortBy === k ? 'text-white' : 'text-white/70'}`}
                >
                  {k === 'recente' && 'Mais recentes'}
                  {k === 'antigo'  && 'Mais antigos'}
                  {k === 'alfa'    && 'Alfabético (A-Z)'}
                  {k === 'status'  && 'Por status'}
                </div>
              ))}
            </div>
          )}
        </button>
        {stale && (
          <span className="inline-flex items-center gap-1 text-amber-300" title="Sincronização atrasada">
            <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
            <span>sync atrasado</span>
          </span>
        )}
      </div>

      {/* Lista */}
      <div className="flex-1 overflow-y-auto px-2 pb-2 min-h-0">
        {visible.length === 0 ? (
          <EmptyState tab={tab} hasSearch={!!search.trim()} search={search} />
        ) : (
          groups.map(g => (
            <div key={g.label || 'all'}>
              {/* Header só aparece quando há mais de um grupo — caso contrário ele só polui */}
              {g.label && groups.length > 1 && (
                <div className="sticky top-0 z-10 px-2 pt-2 pb-1 bg-[#0F2137] text-[9px] uppercase tracking-wider text-white/30 font-semibold">
                  {g.label}
                </div>
              )}
              <ul className="space-y-0.5">
                {g.items.map(doc => (
                  <DocRow
                    key={doc.document_name}
                    doc={doc}
                    selected={selected === doc.document_name}
                    onClick={() => onSelect(doc.document_name)}
                    showResubmit={tab === 'concluidos'}
                    resubmitting={resubmitting === doc.document_name}
                    onResubmit={() => setResubmitConfirm(doc)}
                  />
                ))}
              </ul>
            </div>
          ))
        )}
      </div>

      {/* Confirm modal de re-submeter */}
      {resubmitConfirm && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/60" onClick={() => setResubmitConfirm(null)}>
          <div className="bg-white rounded-xl shadow-xl max-w-md w-full mx-4 p-5 text-gray-900" onClick={e => e.stopPropagation()}>
            <h3 className="text-sm font-semibold mb-2">Re-submeter à Techfin?</h3>
            <p className="text-xs text-gray-600 mb-3">
              <strong>{resubmitConfirm.razao_social || resubmitConfirm.document_name}</strong> será re-enviado com os mesmos valores.
              A Techfin pode aceitar como upsert (200) ou retornar 409 (tratado como sucesso idempotente).
            </p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setResubmitConfirm(null)} className="text-xs px-3 py-1.5 rounded border border-gray-200 hover:bg-gray-50">Cancelar</button>
              <button onClick={() => handleResubmit(resubmitConfirm)} className="text-xs px-3 py-1.5 rounded bg-blue-600 text-white hover:bg-blue-700 font-semibold inline-flex items-center gap-1">
                <IconSend size={12} /> Re-submeter
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Sub-componentes ──────────────────────────────────────────────────────

function TabButton({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className={`text-[11px] px-2.5 py-1 rounded-md font-medium transition-all ${
        active ? 'bg-white/15 text-white' : 'text-white/50 hover:text-white/70 hover:bg-white/5'
      }`}
    >
      {children}
    </button>
  )
}

function DocRow({
  doc, selected, onClick, showResubmit, resubmitting, onResubmit,
}: {
  doc: DocSummary
  selected: boolean
  onClick: () => void
  showResubmit: boolean
  resubmitting: boolean
  onResubmit: () => void
}) {
  const color = avatarColor(doc.cnpj, doc.document_name)
  const initials = getInitials(doc.razao_social, doc.document_name)
  return (
    <li>
      <div
        onClick={onClick}
        className={`group w-full text-left px-2 py-2 rounded-lg flex items-center gap-2 transition-colors cursor-pointer ${
          selected ? 'bg-white/15 ring-1 ring-white/20' : 'hover:bg-white/8'
        }`}
      >
        {/* Avatar */}
        <div
          className="w-8 h-8 rounded-md flex items-center justify-center shrink-0 relative text-[10px] font-bold text-white"
          style={{ backgroundColor: color }}
          title={doc.cnpj ? `CNPJ: ${formatCnpj(doc.cnpj)}` : doc.document_name}
        >
          {initials}
          <span className={`absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 rounded-full border-2 border-[#0F2137] ${STATUS_DOT[doc.status]}`} />
        </div>

        {/* Info */}
        <div className="flex-1 min-w-0">
          <p className={`text-xs font-semibold truncate leading-tight ${selected ? 'text-white' : 'text-white/80'}`}>
            {doc.razao_social ?? doc.document_name}
          </p>
          <p className={`text-[10px] mt-0.5 truncate ${STATUS_TEXT[doc.status]}`}>
            {metaText(doc)}
          </p>
        </div>

        {/* Action: re-submeter (só em Concluídos, no hover) */}
        {showResubmit && (
          <button
            onClick={(e) => { e.stopPropagation(); onResubmit() }}
            disabled={resubmitting}
            className="opacity-0 group-hover:opacity-100 p-1 rounded text-white/40 hover:text-white hover:bg-white/10 transition-all shrink-0 disabled:opacity-30"
            title="Re-submeter à Techfin"
          >
            {resubmitting ? (
              <IconCheck size={14} className="animate-pulse" />
            ) : (
              <IconSend size={14} />
            )}
          </button>
        )}
      </div>
    </li>
  )
}

function EmptyState({ tab, hasSearch, search }: { tab: TabKey; hasSearch: boolean; search: string }) {
  if (hasSearch) {
    return (
      <div className="flex flex-col items-center justify-center px-6 text-center py-12">
        <IconSearch size={20} className="text-white/20 mb-2" />
        <p className="text-xs text-white/40">Nenhum resultado para</p>
        <p className="text-xs text-white/60 mt-0.5 font-medium truncate max-w-full">"{search}"</p>
      </div>
    )
  }
  if (tab === 'pendentes') {
    return (
      <div className="flex flex-col items-center justify-center px-6 text-center py-12">
        <IconChecks size={24} className="text-emerald-400 mb-2" />
        <p className="text-xs text-white/80 font-medium">Tudo em dia!</p>
        <p className="text-[10px] text-white/40 mt-1">Nenhum documento pendente de revisão.</p>
      </div>
    )
  }
  return (
    <div className="flex flex-col items-center justify-center px-6 text-center py-12">
      <IconAlertCircle size={20} className="text-white/20 mb-2" />
      <p className="text-xs text-white/50">Nenhum documento submetido ainda.</p>
      <p className="text-[10px] text-white/30 mt-1">Documentos enviados pra Techfin aparecem aqui.</p>
    </div>
  )
}
