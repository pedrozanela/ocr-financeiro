import { useEffect, useState } from 'react'
import DocumentList from './components/DocumentList'
import FinancialReview from './components/FinancialReview'
import MetricsDashboard from './components/MetricsDashboard'

export interface DocSummary {
  document_name: string
  razao_social: string | null
  cnpj: string | null
  periodo: string | null
  ativo_total: number | string | null
  lucro_liquido: number | string | null
}

export default function App() {
  const [docs, setDocs] = useState<DocSummary[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [view, setView] = useState<'docs' | 'metrics'>('docs')
  const [loading, setLoading] = useState(true)
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [modelUpdating, setModelUpdating] = useState(false)
  const [modelUpdateMsg, setModelUpdateMsg] = useState<string | null>(null)

  function loadDocs() {
    return fetch('/api/documents')
      .then(r => r.json())
      .then(data => { setDocs(data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => { loadDocs() }, [])

  async function handleUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    setUploading(true)
    setUploadError(null)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await fetch('/api/documents/upload', { method: 'POST', body: form })
      const body = await res.json()
      if (!res.ok) throw new Error(body.detail ?? 'Erro desconhecido')

      const docName = body.document_name

      // Poll for OCR completion (runs in background on server)
      for (let i = 0; i < 180; i++) {
        await new Promise(r => setTimeout(r, 5000))
        const st = await fetch(`/api/documents/${encodeURIComponent(docName)}/status`).then(r => r.json())
        if (st.status === 'done') {
          await loadDocs()
          setSelected(docName)
          setView('docs')
          return
        }
        if (st.status === 'error') {
          throw new Error(st.detail ?? 'Erro ao processar OCR')
        }
        // still processing — keep polling
      }
      throw new Error('Timeout: processamento OCR demorou mais de 15 minutos')
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao processar PDF')
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-72 shrink-0 bg-white border-r border-gray-200 flex flex-col">
        {/* Logo header */}
        <div className="px-4 py-3 border-b border-gray-200 bg-[#0F2137]">
          <div className="flex items-center justify-between">
            <img src="/logo.webp" alt="Techfin ERP Finance" className="h-9 w-auto" />
            <label className={`cursor-pointer text-xs px-2.5 py-1.5 rounded border transition-colors shrink-0 ${
              uploading
                ? 'bg-[#1a3050] text-gray-400 border-[#2a4060] cursor-not-allowed'
                : 'bg-transparent text-white border-white/40 hover:bg-white/10'
            }`} title="Enviar novo PDF">
              {uploading ? '⏳' : '+ PDF'}
              <input type="file" accept=".pdf" className="hidden" onChange={handleUpload} disabled={uploading} />
            </label>
          </div>
          {uploading && (
            <p className="text-xs text-blue-300 mt-2 animate-pulse">Extraindo dados… pode levar 1-3 min ⏳</p>
          )}
          {uploadError && (
            <p className="text-xs text-red-300 mt-2">{uploadError}</p>
          )}
          {/* Export */}
          <a
            href="/api/export/excel"
            download="techfin_resultados.xlsx"
            className="mt-3 block text-center text-xs py-1 rounded bg-white/10 text-white/80 hover:bg-white/20 transition-colors"
          >
            ⬇ Exportar Excel
          </a>

          {/* Update Model */}
          <button
            disabled={modelUpdating}
            onClick={async () => {
              setModelUpdating(true)
              setModelUpdateMsg(null)
              try {
                const res = await fetch('/api/admin/update-model', { method: 'POST' })
                const data = await res.json()
                if (res.ok) {
                  setModelUpdateMsg(`Job disparado (run ${data.run_id}). Modelo será atualizado em ~10min.`)
                } else {
                  setModelUpdateMsg(`Erro: ${data.detail || 'falha ao disparar'}`)
                }
              } catch {
                setModelUpdateMsg('Erro de conexão')
              } finally {
                setModelUpdating(false)
              }
            }}
            className={`mt-2 w-full text-xs py-1.5 rounded transition-colors ${
              modelUpdating
                ? 'bg-amber-700/50 text-amber-200 cursor-wait'
                : 'bg-amber-600/30 text-amber-200 hover:bg-amber-600/50 border border-amber-500/30'
            }`}
          >
            {modelUpdating ? '⏳ Atualizando...' : '⟳ Atualizar Modelo'}
          </button>
          {modelUpdateMsg && (
            <p className="text-[10px] text-amber-200/70 mt-1">{modelUpdateMsg}</p>
          )}

          {/* View toggle */}
          <div className="flex gap-1 mt-2">
            <button
              onClick={() => setView('docs')}
              className={`flex-1 text-xs py-1 rounded transition-colors ${
                view === 'docs' ? 'bg-white text-[#0F2137] font-semibold' : 'bg-white/10 text-white/80 hover:bg-white/20'
              }`}
            >
              Documentos
            </button>
            <button
              onClick={() => setView('metrics')}
              className={`flex-1 text-xs py-1 rounded transition-colors ${
                view === 'metrics' ? 'bg-white text-[#0F2137] font-semibold' : 'bg-white/10 text-white/80 hover:bg-white/20'
              }`}
            >
              Métricas
            </button>
          </div>
        </div>
        {view === 'docs' && (
          loading ? (
            <div className="flex-1 flex items-center justify-center text-gray-400 text-sm">
              Carregando documentos…
            </div>
          ) : (
            <DocumentList docs={docs} selected={selected} onSelect={setSelected} />
          )
        )}
        {view === 'metrics' && (
          <div className="flex-1 flex items-center justify-center text-gray-400 text-xs px-4 text-center">
            Veja as métricas no painel →
          </div>
        )}
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-hidden bg-gray-50 flex flex-col">
        {view === 'metrics' ? (
          <div className="flex-1 overflow-hidden"><MetricsDashboard /></div>
        ) : selected ? (
          <FinancialReview documentName={selected} />
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-gray-400">
            <svg className="w-16 h-16 mb-4 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <p className="text-sm">Selecione um documento para revisar</p>
          </div>
        )}
      </main>
    </div>
  )
}
