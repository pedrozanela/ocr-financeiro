import { useEffect, useState } from 'react'
import DocumentList from './components/DocumentList'
import FinancialReview from './components/FinancialReview'
import MetricsDashboard from './components/MetricsDashboard'

export interface DocSummary {
  document_name: string
  razao_social: string | null
  cnpj: string | null
  ingested_at: string | null
  status: 'nao_revisado' | 'em_revisao' | 'submetido' | 'erro_submissao'
  revisado_count: number
  submetido_em: string | null
  finalizado_por: string | null
}

export default function App() {
  const [docs, setDocs] = useState<DocSummary[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [view, setView] = useState<'docs' | 'metrics'>('docs')
  const [loading, setLoading] = useState(true)
  const [uploading, setUploading] = useState(false)
  const [uploadingPerf, setUploadingPerf] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  type BatchItemStatus = 'queued' | 'uploading' | 'processing' | 'done' | 'error'
  interface BatchItem { name: string; status: BatchItemStatus }
  const [uploadProgress, setUploadProgress] = useState<{ items: BatchItem[]; startedAt: number | null }>({ items: [], startedAt: null })
  const [batchElapsed, setBatchElapsed] = useState(0)
  const [search, setSearch] = useState('')
  const [currentUser, setCurrentUser] = useState<string | null>(null)
  // Polling da sidebar
  const [sidebarVersion, setSidebarVersion] = useState<string | null>(null)
  const [lastSyncAt, setLastSyncAt] = useState<number | null>(null)
  const [syncFailures, setSyncFailures] = useState(0)

  async function loadDocs(): Promise<void> {
    try {
      const r = await fetch('/api/documentos/sidebar-state')
      if (!r.ok) { setSyncFailures(n => n + 1); return }
      const data = await r.json()
      setSyncFailures(0)
      setLastSyncAt(Date.now())
      // Atualiza só se a version mudou (evita re-render desnecessário)
      if (data.version !== sidebarVersion) {
        setSidebarVersion(data.version)
        setDocs(data.documentos || [])
      }
    } catch {
      setSyncFailures(n => n + 1)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadDocs()
    fetch('/api/me').then(r => r.json()).then(d => setCurrentUser(d.email)).catch(() => {})
    // Polling: 15s ativo, 60s background
    let interval = 15000
    let timer: ReturnType<typeof setInterval> = setInterval(loadDocs, interval)
    const onVisibility = () => {
      clearInterval(timer)
      interval = document.hidden ? 60000 : 15000
      timer = setInterval(loadDocs, interval)
    }
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisibility)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function handleDelete(documentName: string) {
    try {
      const res = await fetch(`/api/documents/${encodeURIComponent(documentName)}`, { method: 'DELETE' })
      if (!res.ok) {
        const body = await res.json()
        setUploadError(body.detail ?? 'Erro ao excluir')
        return
      }
      if (selected === documentName) setSelected(null)
      await loadDocs()
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao excluir documento')
    }
  }

  async function uploadSingleFile(file: File, endpoint: string): Promise<string> {
    const form = new FormData()
    form.append('file', file)
    const res = await fetch(endpoint, { method: 'POST', body: form })
    const body = await res.json()
    if (!res.ok) throw new Error(body.detail ?? `Erro ao enviar ${file.name}`)
    return body.document_name
  }

  async function pollUntilDone(docName: string): Promise<void> {
    for (let i = 0; i < 360; i++) {
      await new Promise(r => setTimeout(r, 5000))
      const st = await fetch(`/api/documents/${encodeURIComponent(docName)}/status`).then(r => r.json())
      if (st.status === 'done') return
      if (st.status === 'error') throw new Error(st.detail ?? `Erro ao processar ${docName}`)
    }
  }

  function updateItem(originalName: string, patch: Partial<BatchItem>) {
    setUploadProgress(p => ({
      ...p,
      items: p.items.map(it => it.name === originalName ? { ...it, ...patch } : it),
    }))
  }

  async function processFiles(files: File[], endpoint: string, setFlag: (v: boolean) => void) {
    if (files.length === 0) return
    setFlag(true)
    setUploadError(null)
    // Inicializa todos os items como 'queued' usando o filename
    const items: BatchItem[] = files.map(f => ({ name: f.name, status: 'queued' }))
    setUploadProgress({ items, startedAt: Date.now() })
    try {
      // Upload sequencial: cada arquivo passa de 'queued' -> 'uploading' -> (no fim) 'processing'
      const docNames: string[] = []
      for (const file of files) {
        updateItem(file.name, { status: 'uploading' })
        try {
          const docName = await uploadSingleFile(file, endpoint)
          docNames.push(docName)
          // Renomeia o item pro docName retornado (pra match com poll/status)
          setUploadProgress(p => ({
            ...p,
            items: p.items.map(it => it.name === file.name
              ? { ...it, name: docName, status: 'processing' }
              : it),
          }))
        } catch (e) {
          updateItem(file.name, { status: 'error' })
          throw e
        }
      }
      // Poll todos em paralelo, marca como done/error conforme cada termina
      await Promise.all(docNames.map(async (name) => {
        try {
          await pollUntilDone(name)
          updateItem(name, { status: 'done' })
        } catch {
          updateItem(name, { status: 'error' })
        }
        await loadDocs()
      }))
      if (docNames.length === 1) setSelected(docNames[0])
      setView('docs')
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao processar PDF')
      await loadDocs()
    } finally {
      setFlag(false)
      // Pequeno delay pra mostrar os checks verdes antes de sumir
      setTimeout(() => setUploadProgress({ items: [], startedAt: null }), 1500)
    }
  }

  // Tick pra atualizar ETA enquanto há batch ativo
  useEffect(() => {
    if (uploadProgress.startedAt === null) return
    const t = setInterval(() => setBatchElapsed(Date.now() - (uploadProgress.startedAt as number)), 1000)
    return () => clearInterval(t)
  }, [uploadProgress.startedAt])

  // Modal de aviso quando arquivos batem com docs já submetidos
  const [pendingUpload, setPendingUpload] = useState<{
    files: File[]
    endpoint: string
    setFlag: (v: boolean) => void
    submittedDocs: { name: string; submetido_em: string | null }[]
  } | null>(null)

  function checkSubmittedConflicts(files: File[]) {
    const submittedByName = new Map(
      docs.filter(d => d.status === 'submetido').map(d => [d.document_name, d.submetido_em])
    )
    return files
      .filter(f => submittedByName.has(f.name))
      .map(f => ({ name: f.name, submetido_em: submittedByName.get(f.name) ?? null }))
  }

  async function dispatchUpload(files: File[], endpoint: string, setFlag: (v: boolean) => void) {
    const conflicts = checkSubmittedConflicts(files)
    if (conflicts.length > 0) {
      setPendingUpload({ files, endpoint, setFlag, submittedDocs: conflicts })
      return
    }
    await processFiles(files, endpoint, setFlag)
  }

  async function confirmPendingUpload() {
    if (!pendingUpload) return
    const { files, endpoint, setFlag } = pendingUpload
    setPendingUpload(null)
    await processFiles(files, endpoint, setFlag)
  }

  async function handleUpload(e: React.ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? [])
    e.target.value = ''
    await dispatchUpload(files, '/api/documents/upload', setUploading)
  }

  async function handleUploadPerformance(e: React.ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? [])
    e.target.value = ''
    await dispatchUpload(files, '/api/documents/upload-performance', setUploadingPerf)
  }

  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      {/* Sidebar */}
      <aside className="w-72 shrink-0 bg-[#0F2137] flex flex-col">
        {/* Logo */}
        <div className="px-5 pt-4 pb-3 border-b border-white/10">
          <img
            src="https://d28bp6p67p77ho.cloudfront.net/Techfin_Finance_Light_57746e6511_80b733bd5a.svg"
            alt="Techfin Finance"
            className="h-14 w-auto"
          />
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
              <DocumentList
                docs={docs}
                selected={selected}
                onSelect={setSelected}
                onDelete={handleDelete}
                search={search}
                lastSyncAt={lastSyncAt}
                syncFailures={syncFailures}
                onRefresh={loadDocs}
              />
            )
          ) : (
            <div className="flex-1 flex items-center justify-center text-white/25 text-xs px-6 text-center">
              Veja as métricas no painel →
            </div>
          )}
        </div>

        {/* Footer actions */}
        <div className="px-4 py-4 border-t border-white/10 space-y-2">
          {uploadProgress.items.length > 0 && (() => {
            const items = uploadProgress.items
            const total = items.length
            const done  = items.filter(i => i.status === 'done').length
            const errs  = items.filter(i => i.status === 'error').length
            const completed = done + errs
            const pct = Math.round((completed / total) * 100)
            // ETA simples: (elapsed / completed) * (total - completed); só mostra após 1+ concluído
            const elapsedMs = batchElapsed
            const etaMs = completed > 0 ? (elapsedMs / completed) * (total - completed) : 0
            const etaText = etaMs > 0 && completed < total
              ? (etaMs < 60_000 ? `~${Math.round(etaMs / 1000)}s` : `~${Math.round(etaMs / 60_000)} min`)
              : null
            // Item em destaque (primeiro processing/uploading); fallback pro primeiro queued
            const headerLabel = total === 1
              ? (uploadingPerf ? 'Processando (Modo Vision)' : 'Processando')
              : 'Processando'
            const VISIBLE_QUEUED = 3
            const queued = items.filter(i => i.status === 'queued')
            const visibleQueued = queued.slice(0, VISIBLE_QUEUED)
            const hiddenQueued  = queued.length - visibleQueued.length
            return (
              <div className="bg-slate-900 border border-slate-800 rounded-lg p-3.5 text-white">
                {/* Header */}
                <div className="mb-3">
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-[13px] font-medium">{headerLabel}</span>
                    <span className="text-[11px] text-slate-400">
                      {completed} de {total}
                      {etaText && <> · {etaText} restantes</>}
                    </span>
                  </div>
                  {/* Progress bar */}
                  <div className="h-1 bg-slate-800 rounded overflow-hidden">
                    <div
                      className="h-full bg-blue-500 transition-all duration-300"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
                {/* Lista de itens */}
                <ul className="text-[11px] leading-relaxed space-y-0.5">
                  {/* Concluídos (cinza + check) */}
                  {items.filter(i => i.status === 'done').map(it => (
                    <li key={it.name} className="flex items-center gap-1.5 text-slate-500">
                      <svg className="w-3 h-3 text-emerald-500 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                      </svg>
                      <span className="flex-1 truncate">{it.name}</span>
                    </li>
                  ))}
                  {/* Erros */}
                  {items.filter(i => i.status === 'error').map(it => (
                    <li key={it.name} className="flex items-center gap-1.5 text-red-400">
                      <svg className="w-3 h-3 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                      <span className="flex-1 truncate">{it.name}</span>
                    </li>
                  ))}
                  {/* Em uploading/processing (branco + spinner) */}
                  {items.filter(i => i.status === 'uploading' || i.status === 'processing').map(it => (
                    <li key={it.name} className="flex items-center gap-1.5 text-white">
                      <span className="inline-block w-2.5 h-2.5 border-[1.5px] border-current border-t-transparent rounded-full animate-spin shrink-0" />
                      <span className="flex-1 truncate">{it.name}</span>
                      <span className="text-[10px] text-slate-400">{it.status === 'uploading' ? 'enviando' : 'extraindo'}</span>
                    </li>
                  ))}
                  {/* Queued visíveis (relógio, sem X — cancelamento não implementado) */}
                  {visibleQueued.map(it => (
                    <li key={it.name} className="flex items-center gap-1.5 text-slate-400">
                      <svg className="w-3 h-3 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                      <span className="flex-1 truncate">{it.name}</span>
                    </li>
                  ))}
                  {hiddenQueued > 0 && (
                    <li className="text-[11px] text-slate-500 pt-1">
                      + {hiddenQueued} documento{hiddenQueued !== 1 ? 's' : ''} na fila
                    </li>
                  )}
                </ul>
              </div>
            )
          })()}
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
            <input type="file" accept=".pdf" multiple className="hidden" onChange={handleUpload} disabled={uploading || uploadingPerf} />
          </label>

          <label className={`flex items-center justify-center gap-2 w-full py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
            (uploading || uploadingPerf) ? 'bg-white/5 text-white/20 cursor-not-allowed' : 'bg-amber-500/20 text-amber-200 hover:bg-amber-500/30'
          }`}>
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            {uploadingPerf ? 'Processando…' : 'Enviar PDF — Modo Vision'}
            <input type="file" accept=".pdf" multiple className="hidden" onChange={handleUploadPerformance} disabled={uploading || uploadingPerf} />
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

      {/* Modal: arquivos batem com docs ja submetidos */}
      {pendingUpload && (() => {
        const { submittedDocs, files } = pendingUpload
        const N = submittedDocs.length
        return (
          <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
            onClick={() => setPendingUpload(null)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') setPendingUpload(null)
              if (e.key === 'Enter') { e.preventDefault(); confirmPendingUpload() }
            }}
            tabIndex={-1}
          >
            <div
              className="bg-white rounded-xl shadow-xl max-w-lg w-full mx-4 p-6 text-gray-900"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex items-start gap-3 mb-3">
                <div className="w-10 h-10 rounded-full bg-amber-50 border border-amber-200 flex items-center justify-center shrink-0">
                  <svg className="w-5 h-5 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M12 3l9.66 16.5H2.34L12 3z" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="text-base font-semibold">
                    {N === 1 ? 'Este documento já foi submetido' : `${N} documentos já foram submetidos`}
                  </h3>
                  <p className="text-sm text-gray-600 mt-1">
                    Subir uma nova versão vai trazê-{N === 1 ? 'lo' : 'los'} de volta para <strong>Pendentes</strong> para nova revisão.
                    Suas correções e confirmações anteriores são <strong>preservadas</strong>.
                  </p>
                </div>
              </div>
              <ul className="border border-gray-100 rounded-lg divide-y divide-gray-100 max-h-56 overflow-y-auto text-xs mb-4">
                {submittedDocs.map(d => (
                  <li key={d.name} className="flex items-center justify-between px-3 py-2 gap-3">
                    <span className="flex-1 truncate" title={d.name}>{d.name}</span>
                    <span className="text-gray-400 tabular-nums shrink-0">
                      {d.submetido_em ? d.submetido_em.substring(0, 10).split('-').reverse().join('/') : '—'}
                    </span>
                  </li>
                ))}
              </ul>
              <div className="flex justify-end gap-2">
                <button
                  onClick={() => setPendingUpload(null)}
                  className="text-sm px-4 py-2 rounded-lg border border-gray-200 text-gray-700 hover:bg-gray-50"
                >
                  Cancelar
                </button>
                <button
                  onClick={confirmPendingUpload}
                  autoFocus
                  className="text-sm px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 font-semibold"
                >
                  Continuar com {files.length} arquivo{files.length !== 1 ? 's' : ''}
                </button>
              </div>
            </div>
          </div>
        )
      })()}
    </div>
  )
}
