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
  const [uploadingPerf, setUploadingPerf] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [currentUser, setCurrentUser] = useState<string | null>(null)

  function loadDocs() {
    return fetch('/api/documents')
      .then(r => r.json())
      .then(data => { setDocs(data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => {
    loadDocs()
    fetch('/api/me').then(r => r.json()).then(d => setCurrentUser(d.email)).catch(() => {})
  }, [])

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
      for (let i = 0; i < 360; i++) {
        await new Promise(r => setTimeout(r, 5000))
        const st = await fetch(`/api/documents/${encodeURIComponent(docName)}/status`).then(r => r.json())
        if (st.status === 'done') {
          await loadDocs()
          setSelected(docName)
          setView('docs')
          return
        }
        if (st.status === 'error') throw new Error(st.detail ?? 'Erro ao processar OCR')
      }
      await loadDocs()
      setView('docs')
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao processar PDF')
    } finally {
      setUploading(false)
    }
  }

  async function handleUploadPerformance(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    setUploadingPerf(true)
    setUploadError(null)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await fetch('/api/documents/upload-performance', { method: 'POST', body: form })
      const body = await res.json()
      if (!res.ok) throw new Error(body.detail ?? 'Erro desconhecido')
      const docName = body.document_name
      for (let i = 0; i < 360; i++) {
        await new Promise(r => setTimeout(r, 5000))
        const st = await fetch(`/api/documents/${encodeURIComponent(docName)}/status`).then(r => r.json())
        if (st.status === 'done') {
          await loadDocs()
          setSelected(docName)
          setView('docs')
          return
        }
        if (st.status === 'error') throw new Error(st.detail ?? 'Erro ao processar OCR')
      }
      await loadDocs()
      setView('docs')
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao processar PDF')
    } finally {
      setUploadingPerf(false)
    }
  }

  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* Sidebar */}
      <aside className="w-72 shrink-0 bg-[#0F2137] flex flex-col">
        {/* Logo */}
        <div className="px-5 pt-4 pb-3 border-b border-white/10">
          <img src="/logo.webp" alt="Techfin ERP Finance" className="h-8 w-auto" />
        </div>

        {/* View toggle */}
        <div className="px-4 pt-3 pb-2">
          <div className="flex bg-white/10 rounded-lg p-0.5">
            <button
              onClick={() => setView('docs')}
              className={`flex-1 text-xs font-medium py-1.5 rounded-md transition-all ${
                view === 'docs' ? 'bg-white text-[#0F2137] shadow-sm' : 'text-white/60 hover:text-white'
              }`}
            >
              Documentos
            </button>
            <button
              onClick={() => setView('metrics')}
              className={`flex-1 text-xs font-medium py-1.5 rounded-md transition-all ${
                view === 'metrics' ? 'bg-white text-[#0F2137] shadow-sm' : 'text-white/60 hover:text-white'
              }`}
            >
              Métricas
            </button>
          </div>
        </div>

        {/* Search */}
        {view === 'docs' && (
          <div className="px-3 pb-2">
            <div className="relative">
              <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30 pointer-events-none" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Buscar empresa ou CNPJ…"
                className="w-full bg-white/10 text-white/80 placeholder-white/25 text-xs rounded-lg px-3 py-2 pl-8 focus:outline-none focus:bg-white/15 focus:ring-1 focus:ring-white/20 transition-all"
              />
              {search && (
                <button
                  onClick={() => setSearch('')}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-white/30 hover:text-white/60 text-sm leading-none"
                >
                  ×
                </button>
              )}
            </div>
          </div>
        )}

        {/* Document list or placeholder */}
        <div className="flex-1 overflow-hidden flex flex-col min-h-0">
          {view === 'docs' ? (
            loading ? (
              <div className="flex-1 flex items-center justify-center">
                <div className="w-5 h-5 border-2 border-white/20 border-t-white/60 rounded-full animate-spin" />
              </div>
            ) : (
              <DocumentList docs={docs} selected={selected} onSelect={setSelected} search={search} />
            )
          ) : (
            <div className="flex-1 flex items-center justify-center text-white/25 text-xs px-6 text-center">
              Veja as métricas no painel →
            </div>
          )}
        </div>

        {/* Footer actions */}
        <div className="px-4 py-4 border-t border-white/10 space-y-2">
          {(uploading || uploadingPerf) && (
            <div className="flex items-center gap-2 bg-blue-400/20 rounded-lg px-3 py-2">
              <div className="w-3 h-3 border border-blue-300/50 border-t-blue-200 rounded-full animate-spin shrink-0" />
              <p className="text-xs text-blue-200">
                {uploadingPerf ? 'Modo Vision… pode levar 4-6 min' : 'Extraindo dados… pode levar 2-4 min'}
              </p>
            </div>
          )}
          {uploadError && (
            <div className="bg-red-500/20 rounded-lg px-3 py-2">
              <p className="text-xs text-red-300">{uploadError}</p>
            </div>
          )}
          <label className={`flex items-center justify-center gap-2 w-full py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
            (uploading || uploadingPerf) ? 'bg-white/5 text-white/20 cursor-not-allowed' : 'bg-white/10 text-white hover:bg-white/18'
          }`}>
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
            </svg>
            {uploading ? 'Processando…' : 'Enviar PDF'}
            <input type="file" accept=".pdf" className="hidden" onChange={handleUpload} disabled={uploading || uploadingPerf} />
          </label>

          <label className={`flex items-center justify-center gap-2 w-full py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
            (uploading || uploadingPerf) ? 'bg-white/5 text-white/20 cursor-not-allowed' : 'bg-amber-500/20 text-amber-200 hover:bg-amber-500/30'
          }`}>
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            {uploadingPerf ? 'Processando…' : 'Enviar PDF — Modo Vision'}
            <input type="file" accept=".pdf" className="hidden" onChange={handleUploadPerformance} disabled={uploading || uploadingPerf} />
          </label>

          <a
            href="/api/export/excel"
            download="techfin_resultados.xlsx"
            className="flex items-center justify-center gap-2 w-full py-2 rounded-lg text-xs font-medium bg-white/10 text-white hover:bg-white/18 transition-all"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
            Exportar Excel
          </a>

          {currentUser && (
            <div className="flex items-center gap-2 pt-1">
              <div className="w-6 h-6 rounded-full bg-white/15 flex items-center justify-center shrink-0">
                <svg className="w-3.5 h-3.5 text-white/50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                </svg>
              </div>
              <p className="text-[10px] text-white/40 truncate">{currentUser}</p>
            </div>
          )}
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-hidden flex flex-col">
        {view === 'metrics' ? (
          <div className="flex-1 overflow-hidden"><MetricsDashboard /></div>
        ) : selected ? (
          <FinancialReview documentName={selected} />
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-gray-300">
            <div className="w-16 h-16 mb-5 rounded-2xl bg-white border border-gray-100 shadow-sm flex items-center justify-center">
              <svg className="w-8 h-8 text-gray-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <p className="text-sm text-gray-400 font-medium">Selecione um documento para revisar</p>
            <p className="text-xs text-gray-300 mt-1">ou envie um novo PDF pelo painel lateral</p>
          </div>
        )}
      </main>
    </div>
  )
}
