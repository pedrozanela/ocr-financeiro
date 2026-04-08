import { useEffect, useMemo, useRef, useState } from 'react'

interface Props {
  documentName: string
}

export default function OcrTextPanel({ documentName }: Props) {
  const [text, setText] = useState<string | null>(null)
  const [meta, setMeta] = useState<{ atualizado_em: string; atualizado_por: string } | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [matchCount, setMatchCount] = useState(0)
  const textRef = useRef<HTMLPreElement>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(`/api/documents/${encodeURIComponent(documentName)}/ocr-text`)
      .then(r => {
        if (!r.ok) throw new Error('Texto OCR não disponível para este documento')
        return r.json()
      })
      .then(d => {
        setText(d.document_text)
        setMeta({ atualizado_em: d.atualizado_em, atualizado_por: d.atualizado_por })
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [documentName])

  // Highlight search matches (pure, no side effects)
  const highlighted = useMemo(() => {
    if (!text) return ''
    if (!search.trim()) return escapeHtml(text)
    return escapeHtml(text).replace(
      new RegExp(`(${escapeHtml(search).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi'),
      '<mark class="bg-yellow-200 text-yellow-900 rounded px-0.5">$1</mark>'
    )
  }, [text, search])

  // Update match count as side effect
  useEffect(() => {
    if (!text || !search.trim()) { setMatchCount(0); return }
    const escaped = search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const matches = [...text.matchAll(new RegExp(escaped, 'gi'))]
    setMatchCount(matches.length)
  }, [text, search])

  function escapeHtml(s: string) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  }

  // Scroll to first match
  function scrollToMatch() {
    if (!textRef.current) return
    const mark = textRef.current.querySelector('mark')
    if (mark) mark.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }

  useEffect(() => {
    if (search) scrollToMatch()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [highlighted])

  if (loading) return (
    <div className="flex items-center justify-center h-40 text-sm text-gray-400">
      Carregando texto OCR...
    </div>
  )

  if (error) return (
    <div className="flex flex-col items-center justify-center h-40 gap-2 text-sm text-gray-400">
      <svg className="w-8 h-8 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
          d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <span>{error}</span>
      <span className="text-xs text-gray-300">Reprocesse com Vision OCR para gerar o texto</span>
    </div>
  )

  return (
    <div className="flex flex-col gap-3 h-full">
      {/* Meta + search bar */}
      <div className="flex items-center gap-3 shrink-0">
        <div className="relative flex-1">
          <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400 pointer-events-none"
            fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            placeholder="Buscar no texto OCR... (ex: 26.105.712)"
            value={search}
            onChange={e => { setSearch(e.target.value); setMatchCount(0) }}
            className="w-full pl-8 pr-3 py-1.5 text-xs border border-gray-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-blue-400"
          />
          {search && (
            <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-gray-400 tabular-nums">
              {matchCount} {matchCount === 1 ? 'resultado' : 'resultados'}
            </span>
          )}
        </div>
        {meta && (
          <span className="text-xs text-gray-400 shrink-0 tabular-nums">
            {meta.atualizado_por && `${meta.atualizado_por} · `}
            {meta.atualizado_em ? new Date(meta.atualizado_em).toLocaleString('pt-BR') : ''}
          </span>
        )}
      </div>

      {/* Text content */}
      <div className="flex-1 overflow-y-auto border border-gray-200 rounded-lg bg-gray-50">
        <pre
          ref={textRef}
          className="p-4 text-xs text-gray-700 whitespace-pre-wrap font-mono leading-relaxed"
          dangerouslySetInnerHTML={{ __html: highlighted }}
        />
      </div>
    </div>
  )
}
