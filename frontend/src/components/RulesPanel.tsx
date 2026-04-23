import { useEffect, useMemo, useState } from 'react'

interface Rule {
  id: number
  titulo: string
  regra: string
}

export default function RulesPanel() {
  const [rules, setRules] = useState<Rule[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Set<number>>(new Set())

  useEffect(() => {
    fetch('/api/rules')
      .then(r => r.json())
      .then(d => {
        if (d.rules) setRules(d.rules as Rule[])
        else setError(d.detail || 'Erro ao carregar regras')
      })
      .catch(e => setError(String(e)))
  }, [])

  const filtered = useMemo(() => {
    if (!rules) return []
    const q = search.trim().toLowerCase()
    if (!q) return rules
    return rules.filter(r =>
      r.titulo.toLowerCase().includes(q) || r.regra.toLowerCase().includes(q)
    )
  }, [rules, search])

  function toggle(id: number) {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function expandAll() {
    if (filtered.length) setExpanded(new Set(filtered.map(r => r.id)))
  }

  function collapseAll() {
    setExpanded(new Set())
  }

  if (error) {
    return (
      <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg p-4">
        Erro ao carregar regras: {error}
      </div>
    )
  }

  if (!rules) {
    return <div className="text-sm text-gray-500">Carregando regras...</div>
  }

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-semibold text-gray-900">Regras de Classificação Contábil</h3>
          <p className="text-xs text-gray-500 mt-0.5">
            {rules.length} regras que o modelo de extração segue. Essas regras têm prioridade sobre interpretação individual do LLM.
          </p>
        </div>
        <div className="flex gap-1 shrink-0">
          <button
            onClick={expandAll}
            className="text-xs px-2.5 py-1 rounded border border-gray-300 text-gray-600 hover:bg-gray-50"
          >
            Expandir todas
          </button>
          <button
            onClick={collapseAll}
            className="text-xs px-2.5 py-1 rounded border border-gray-300 text-gray-600 hover:bg-gray-50"
          >
            Recolher
          </button>
        </div>
      </div>

      <input
        type="text"
        value={search}
        onChange={e => setSearch(e.target.value)}
        placeholder="Buscar por título ou conteúdo..."
        className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
      />

      {filtered.length === 0 ? (
        <div className="text-sm text-gray-500 py-4 text-center">Nenhuma regra encontrada.</div>
      ) : (
        <div className="space-y-2">
          {filtered.map(rule => {
            const isExpanded = expanded.has(rule.id)
            return (
              <div key={rule.id} className="border border-gray-200 rounded-lg overflow-hidden bg-white">
                <button
                  onClick={() => toggle(rule.id)}
                  className="w-full flex items-center gap-3 px-4 py-3 hover:bg-gray-50 text-left"
                >
                  <span className="text-xs font-mono bg-blue-100 text-blue-700 rounded px-2 py-0.5 shrink-0">
                    #{rule.id}
                  </span>
                  <span className="text-sm font-medium text-gray-900 flex-1">{rule.titulo}</span>
                  <svg
                    className={`w-4 h-4 text-gray-400 shrink-0 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {isExpanded && (
                  <div className="px-4 py-3 border-t border-gray-100 bg-gray-50">
                    <p className="text-xs text-gray-700 whitespace-pre-wrap leading-relaxed">{rule.regra}</p>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
