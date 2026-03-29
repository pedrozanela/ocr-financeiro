import { useEffect, useState } from 'react'

interface GlobalMetrics {
  total_docs: number
  total_corrections: number
  pending_corrections: number
  confirmed_corrections: number
  docs_with_corrections: number
  accuracy_pct: number | null
  by_field: { campo: string; pendente: string; confirmado: string; total: string }[]
  by_type: { tipo: string; total: string }[]
  by_doc: { document_name: string; pendente: string; confirmado: string; total: string; razao_social: string | null; accuracy_pct: string | null; total_records: string }[]
  by_user: { usuario: string; total_correcoes: string; confirmadas: string; ultima_correcao: string | null }[]
  recent: { document_name: string; campo: string; valor_extraido: string; valor_correto: string; comentario: string; criado_por: string; criado_em: string | null; confirmado_por: string; confirmado_em: string | null; status: string }[]
}

interface DocumentMetrics {
  document_name: string
  razao_social: string | null
  total_corrections: number
  pending_corrections: number
  confirmed_corrections: number
  records_with_corrections: number
  total_records: number
  accuracy_pct: number | null
  by_record: { tipo_entidade: string; periodo: string; pendente: string; confirmado: string; total: string; accuracy_pct: string | null }[]
  by_field: { campo: string; pendente: string; confirmado: string; total: string }[]
  by_type: { tipo: string; total: string }[]
}

function StatCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: 'amber' | 'green' | 'gray' }) {
  const colorClass = color === 'amber' ? 'text-amber-700' : color === 'green' ? 'text-green-700' : 'text-gray-900'
  return (
    <div className="bg-white rounded-xl border border-gray-100 px-5 py-4">
      <p className="text-xs text-gray-500">{label}</p>
      <p className={`text-2xl font-bold ${colorClass} mt-1`}>{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  )
}

function StackedBar({ label, pending, confirmed, max }: { label: string; pending: number; confirmed: number; max: number }) {
  const total = pending + confirmed
  const pctPending = max > 0 ? (pending / max) * 100 : 0
  const pctConfirmed = max > 0 ? (confirmed / max) * 100 : 0
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-gray-600 w-52 shrink-0 truncate" title={label}>{label}</span>
      <div className="flex-1 bg-gray-100 rounded-full h-2 flex">
        {pending > 0 && <div className="bg-amber-500 h-2 rounded-l-full" style={{ width: `${pctPending}%` }} />}
        {confirmed > 0 && <div className="bg-green-500 h-2 rounded-r-full" style={{ width: `${pctConfirmed}%` }} />}
      </div>
      <div className="flex gap-1 items-center text-xs font-mono">
        {pending > 0 && <span className="text-amber-700">{pending}</span>}
        {confirmed > 0 && <span className="text-green-700">+{confirmed}</span>}
        <span className="text-gray-400">=</span>
        <span className="text-gray-700 font-semibold">{total}</span>
      </div>
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
  const [view, setView] = useState<'global' | 'document'>('global')
  const [globalData, setGlobalData] = useState<GlobalMetrics | null>(null)
  const [docData, setDocData] = useState<DocumentMetrics | null>(null)
  const [selectedDoc, setSelectedDoc] = useState<string>('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetch('/api/metrics')
      .then(r => r.json())
      .then(d => {
        setGlobalData(d)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  useEffect(() => {
    if (view === 'document' && selectedDoc) {
      fetch(`/api/metrics/${encodeURIComponent(selectedDoc)}`)
        .then(r => r.json())
        .then(d => setDocData(d))
        .catch(() => setDocData(null))
    }
  }, [view, selectedDoc])

  if (loading) {
    return <div className="flex items-center justify-center h-full text-gray-400 text-sm">Carregando métricas…</div>
  }
  if (!globalData) {
    return <div className="flex items-center justify-center h-full text-red-500 text-sm">Erro ao carregar métricas.</div>
  }

  const data = view === 'global' ? globalData : docData
  const maxField = data ? Math.max(...data.by_field.map(r => parseInt(r.pendente || '0') + parseInt(r.confirmado || '0')), 1) : 1
  const maxType  = data ? Math.max(...data.by_type.map(r => parseInt(r.total) || 0), 1) : 1

  return (
    <div className="p-6 space-y-6 overflow-y-auto h-full">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-base font-bold text-gray-900">Métricas de Acurácia</h2>
          <p className="text-xs text-gray-500 mt-0.5">
            {view === 'global' ? 'Visão consolidada de todos os documentos' : `Detalhes do documento: ${docData?.razao_social ?? selectedDoc}`}
          </p>
        </div>

        <div className="flex items-center gap-2">
          {/* View selector */}
          <button
            onClick={() => setView('global')}
            className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
              view === 'global'
                ? 'bg-[#0F2137] text-white border-[#0F2137]'
                : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
            }`}
          >
            Global
          </button>
          <button
            onClick={() => setView('document')}
            className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
              view === 'document'
                ? 'bg-[#0F2137] text-white border-[#0F2137]'
                : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
            }`}
          >
            Por Documento
          </button>

          {/* Document selector (only when view === 'document') */}
          {view === 'document' && (
            <select
              value={selectedDoc}
              onChange={e => setSelectedDoc(e.target.value)}
              className="text-xs px-3 py-1.5 rounded border border-gray-300 bg-white text-gray-700 hover:border-[#0F2137] focus:outline-none focus:ring-2 focus:ring-blue-400"
            >
              <option value="">Selecione um documento…</option>
              {globalData.by_doc.map(d => (
                <option key={d.document_name} value={d.document_name}>
                  {d.razao_social ?? d.document_name}
                </option>
              ))}
            </select>
          )}
        </div>
      </div>

      {/* Global view */}
      {view === 'global' && (
        <>
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
            <StatCard label="Documentos revisados" value={String(globalData.total_docs)} />
            <StatCard
              label="Acurácia confirmada"
              value={globalData.accuracy_pct != null ? `${globalData.accuracy_pct}%` : '—'}
              sub={`${globalData.confirmed_corrections} confirmados em ~${globalData.total_docs * 70} campos`}
            />
            <StatCard label="Total de correções" value={String(globalData.total_corrections)} sub={`${globalData.docs_with_corrections} docs afetados`} />
            <StatCard label="Pendentes" value={String(globalData.pending_corrections)} color="amber" />
            <StatCard label="Confirmadas" value={String(globalData.confirmed_corrections)} color="green" />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Campos com mais erros</h3>
              {globalData.by_field.length === 0 ? (
                <p className="text-xs text-gray-400">Nenhuma correção registrada.</p>
              ) : (
                <div className="space-y-2.5">
                  {globalData.by_field.map(r => (
                    <StackedBar
                      key={r.campo}
                      label={r.campo}
                      pending={parseInt(r.pendente) || 0}
                      confirmed={parseInt(r.confirmado) || 0}
                      max={maxField}
                    />
                  ))}
                </div>
              )}
            </div>

            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Tipo de erro mais comum</h3>
              {globalData.by_type.length === 0 ? (
                <p className="text-xs text-gray-400">Nenhuma correção registrada.</p>
              ) : (
                <div className="space-y-2.5">
                  {globalData.by_type.map(r => (
                    <Bar key={r.tipo} label={r.tipo} value={parseInt(r.total) || 0} max={maxType} />
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* By user */}
          {globalData.by_user?.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Atividade por revisor</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b border-gray-100">
                    <th className="text-left pb-2 font-medium">Usuário</th>
                    <th className="text-right pb-2 font-medium">Correções</th>
                    <th className="text-right pb-2 font-medium">Confirmadas</th>
                    <th className="text-right pb-2 font-medium">Última atividade</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {globalData.by_user.map(r => (
                    <tr key={r.usuario}>
                      <td className="py-2 text-gray-700 font-medium">{r.usuario}</td>
                      <td className="py-2 text-right font-mono text-gray-700">{r.total_correcoes}</td>
                      <td className="py-2 text-right font-mono text-green-700">{r.confirmadas}</td>
                      <td className="py-2 text-right text-gray-400">
                        {r.ultima_correcao ? r.ultima_correcao.substring(0, 16).replace('T', ' ') : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Recent corrections */}
          {globalData.recent?.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Correções recentes</h3>
              <div className="space-y-2">
                {globalData.recent.map((r, i) => (
                  <div key={i} className="flex items-start gap-3 py-2 border-b border-gray-50 last:border-0">
                    <div className={`mt-0.5 w-1.5 h-1.5 rounded-full shrink-0 ${r.status === 'confirmado' ? 'bg-green-400' : 'bg-amber-400'}`} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-xs font-semibold text-gray-700 truncate">{r.campo}</span>
                        <span className="text-[10px] text-gray-400 truncate max-w-[180px]">{r.document_name}</span>
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-xs font-mono text-gray-400 line-through">{r.valor_extraido || '—'}</span>
                        <svg className="w-3 h-3 text-gray-300 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                        </svg>
                        <span className="text-xs font-mono font-semibold text-gray-700">{r.valor_correto || '—'}</span>
                        {r.comentario && <span className="text-[10px] text-gray-400 italic truncate">{r.comentario}</span>}
                      </div>
                    </div>
                    <div className="text-right shrink-0">
                      <p className="text-[10px] text-gray-500 font-medium">{r.criado_por}</p>
                      <p className="text-[10px] text-gray-300">
                        {r.criado_em ? r.criado_em.substring(0, 16).replace('T', ' ') : ''}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {globalData.by_doc.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Todos os documentos</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b border-gray-100">
                    <th className="text-left pb-2 font-medium">Empresa</th>
                    <th className="text-left pb-2 font-medium">Arquivo</th>
                    <th className="text-right pb-2 font-medium">Registros</th>
                    <th className="text-right pb-2 font-medium">Pendente</th>
                    <th className="text-right pb-2 font-medium">Confirmado</th>
                    <th className="text-right pb-2 font-medium">Total</th>
                    <th className="text-right pb-2 font-medium">Acurácia</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {globalData.by_doc.map(r => {
                    const acc = parseFloat(r.accuracy_pct || '0')
                    const accColor = acc >= 99 ? 'text-green-700' : acc >= 95 ? 'text-amber-700' : 'text-red-700'
                    return (
                      <tr key={r.document_name} className="hover:bg-gray-50 cursor-pointer" onClick={() => { setSelectedDoc(r.document_name); setView('document') }}>
                        <td className="py-2 text-gray-700 truncate max-w-[150px]">{r.razao_social ?? '—'}</td>
                        <td className="py-2 text-gray-400 truncate max-w-[150px]">{r.document_name}</td>
                        <td className="py-2 text-right font-mono text-gray-500">{r.total_records}</td>
                        <td className="py-2 text-right font-mono text-amber-700">{r.pendente}</td>
                        <td className="py-2 text-right font-mono text-green-700">{r.confirmado}</td>
                        <td className="py-2 text-right font-mono font-semibold text-gray-700">{r.total}</td>
                        <td className={`py-2 text-right font-mono font-bold ${accColor}`}>{r.accuracy_pct ? `${r.accuracy_pct}%` : '—'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}

      {/* Document view */}
      {view === 'document' && docData && (
        <>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <StatCard
              label="Acurácia do documento"
              value={docData.accuracy_pct != null ? `${docData.accuracy_pct}%` : '—'}
              sub={`${docData.total_records} registros · ~${docData.total_records * 70} campos`}
            />
            <StatCard label="Total de correções" value={String(docData.total_corrections)} sub={`${docData.records_with_corrections} registros afetados`} />
            <StatCard label="Pendentes" value={String(docData.pending_corrections)} color="amber" />
            <StatCard label="Confirmadas" value={String(docData.confirmed_corrections)} color="green" />
          </div>

          {/* By record (tipo_entidade + periodo) */}
          {docData.by_record.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Acurácia por registro (tipo entidade × período)</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-400 border-b border-gray-100">
                    <th className="text-left pb-2 font-medium">Tipo Entidade</th>
                    <th className="text-left pb-2 font-medium">Período</th>
                    <th className="text-right pb-2 font-medium">Pendente</th>
                    <th className="text-right pb-2 font-medium">Confirmado</th>
                    <th className="text-right pb-2 font-medium">Total</th>
                    <th className="text-right pb-2 font-medium">Acurácia</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {docData.by_record.map((r, i) => {
                    const acc = parseFloat(r.accuracy_pct || '0')
                    const accColor = acc >= 99 ? 'text-green-700' : acc >= 95 ? 'text-amber-700' : 'text-red-700'
                    return (
                      <tr key={i}>
                        <td className="py-2 text-gray-700">{r.tipo_entidade || 'INDIVIDUAL'}</td>
                        <td className="py-2 text-gray-700">{r.periodo ? r.periodo.substring(0, 10) : '—'}</td>
                        <td className="py-2 text-right font-mono text-amber-700">{r.pendente}</td>
                        <td className="py-2 text-right font-mono text-green-700">{r.confirmado}</td>
                        <td className="py-2 text-right font-mono font-semibold text-gray-700">{r.total}</td>
                        <td className={`py-2 text-right font-mono font-bold ${accColor}`}>{r.accuracy_pct ? `${r.accuracy_pct}%` : '—'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Campos com mais erros</h3>
              {docData.by_field.length === 0 ? (
                <p className="text-xs text-gray-400">Nenhuma correção neste documento.</p>
              ) : (
                <div className="space-y-2.5">
                  {docData.by_field.map(r => (
                    <StackedBar
                      key={r.campo}
                      label={r.campo}
                      pending={parseInt(r.pendente) || 0}
                      confirmed={parseInt(r.confirmado) || 0}
                      max={maxField}
                    />
                  ))}
                </div>
              )}
            </div>

            <div className="bg-white rounded-xl border border-gray-100 p-5">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Tipo de erro</h3>
              {docData.by_type.length === 0 ? (
                <p className="text-xs text-gray-400">Nenhuma correção neste documento.</p>
              ) : (
                <div className="space-y-2.5">
                  {docData.by_type.map(r => (
                    <Bar key={r.tipo} label={r.tipo} value={parseInt(r.total) || 0} max={maxType} />
                  ))}
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {view === 'document' && !docData && selectedDoc && (
        <div className="flex items-center justify-center h-64 text-gray-400 text-sm">
          Carregando métricas do documento…
        </div>
      )}
      {view === 'document' && !selectedDoc && (
        <div className="flex items-center justify-center h-64 text-gray-400 text-sm">
          Selecione um documento acima para ver métricas detalhadas
        </div>
      )}

      <p className="text-xs text-gray-400">
        * Acurácia baseada em correções <strong>confirmadas</strong>. Correções pendentes não afetam a métrica.
      </p>
    </div>
  )
}
