import { useEffect, useState } from 'react'

interface Metrics {
  total_docs: number
  total_corrections: number
  docs_with_corrections: number
  accuracy_pct: number | null
  by_field: { campo: string; total: string }[]
  by_type: { tipo: string; total: string }[]
  by_doc: { document_name: string; corrections: string; razao_social: string | null }[]
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 px-5 py-4">
      <p className="text-xs text-gray-500">{label}</p>
      <p className="text-2xl font-bold text-gray-900 mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  )
}

function Bar({ label, value, max }: { label: string; value: number; max: number }) {
  const pct = max > 0 ? (value / max) * 100 : 0
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-gray-600 w-52 shrink-0 truncate" title={label}>{label}</span>
      <div className="flex-1 bg-gray-100 rounded-full h-2">
        <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-mono text-gray-700 w-6 text-right">{value}</span>
    </div>
  )
}

export default function MetricsDashboard() {
  const [data, setData] = useState<Metrics | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/metrics')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading) {
    return <div className="flex items-center justify-center h-full text-gray-400 text-sm">Carregando métricas…</div>
  }
  if (!data) {
    return <div className="flex items-center justify-center h-full text-red-500 text-sm">Erro ao carregar métricas.</div>
  }

  const maxField = Math.max(...data.by_field.map(r => parseInt(r.total) || 0), 1)
  const maxType = Math.max(...data.by_type.map(r => parseInt(r.total) || 0), 1)

  return (
    <div className="p-6 space-y-6 overflow-y-auto h-full">
      <div>
        <h2 className="text-base font-bold text-gray-900">Métricas de Acurácia</h2>
        <p className="text-xs text-gray-500 mt-0.5">Baseado nas correções feitas no app</p>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard
          label="Documentos revisados"
          value={String(data.total_docs)}
        />
        <StatCard
          label="Acurácia estimada"
          value={data.accuracy_pct != null ? `${data.accuracy_pct}%` : '—'}
          sub={`${data.total_corrections} erros em ~${data.total_docs * 70} campos`}
        />
        <StatCard
          label="Total de correções"
          value={String(data.total_corrections)}
          sub={`${data.docs_with_corrections} documento${data.docs_with_corrections !== 1 ? 's' : ''} afetado${data.docs_with_corrections !== 1 ? 's' : ''}`}
        />
        <StatCard
          label="Documentos sem erros"
          value={String(data.total_docs - data.docs_with_corrections)}
          sub={`de ${data.total_docs} total`}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Errors by field */}
        <div className="bg-white rounded-xl border border-gray-100 p-5">
          <h3 className="text-sm font-semibold text-gray-800 mb-4">Campos com mais erros</h3>
          {data.by_field.length === 0 ? (
            <p className="text-xs text-gray-400">Nenhuma correção registrada ainda.</p>
          ) : (
            <div className="space-y-2.5">
              {data.by_field.map(r => (
                <Bar key={r.campo} label={r.campo} value={parseInt(r.total) || 0} max={maxField} />
              ))}
            </div>
          )}
        </div>

        {/* Errors by type */}
        <div className="bg-white rounded-xl border border-gray-100 p-5">
          <h3 className="text-sm font-semibold text-gray-800 mb-4">Tipo de erro mais comum</h3>
          {data.by_type.length === 0 ? (
            <p className="text-xs text-gray-400">Nenhuma correção registrada ainda.</p>
          ) : (
            <div className="space-y-2.5">
              {data.by_type.map(r => (
                <Bar key={r.tipo} label={r.tipo} value={parseInt(r.total) || 0} max={maxType} />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Errors by document */}
      {data.by_doc.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-100 p-5">
          <h3 className="text-sm font-semibold text-gray-800 mb-4">Documentos com mais correções</h3>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-100">
                <th className="text-left pb-2 font-medium">Empresa</th>
                <th className="text-left pb-2 font-medium">Arquivo</th>
                <th className="text-right pb-2 font-medium">Correções</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {data.by_doc.map(r => (
                <tr key={r.document_name}>
                  <td className="py-2 text-gray-700 truncate max-w-[200px]">{r.razao_social ?? '—'}</td>
                  <td className="py-2 text-gray-400 truncate max-w-[200px]">{r.document_name}</td>
                  <td className="py-2 text-right font-mono font-semibold text-amber-700">{r.corrections}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <p className="text-xs text-gray-400">
        * Acurácia estimada considerando ~70 campos por documento. Para uma avaliação mais precisa,
        rode o notebook <code>evaluation.py</code> no workspace.
      </p>
    </div>
  )
}
