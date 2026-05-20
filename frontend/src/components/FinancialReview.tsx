import { useEffect, useRef, useState } from 'react'
import FieldSection from './FieldSection'
// FontesPanel removed — fontes shown inline via FieldRow tooltip
import OcrTextPanel from './OcrTextPanel'
import PontosDeAtencao from './PontosDeAtencao'
import RulesPanel from './RulesPanel'
import { SECTIONS } from './fieldDefinitions'

interface Props {
  documentName: string
}

export interface Correction {
  campo: string
  tipo_entidade: string
  periodo: string
  valor_extraido: string
  valor_correto: string
  comentario: string
  status: string
  confirmado_em: string | null
  confirmado_por: string | null
}

export type CorrectionsMap = Record<string, Correction>

export interface AssessmentItem {
  campo: string
  confianca: 'media' | 'baixa'
  motivo: string
}

interface DocRecord {
  tipo_entidade: string | null
  periodo: string | null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any
  assessment?: AssessmentItem[]
  processado_em?: string | null
  modelo_versao?: string | null
  modo_extracao?: string | null
  status?: string
  finalizado_em?: string
  finalizado_por?: string
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getNestedValue(obj: any, path: string): string {
  const parts = path.split('.')
  let cur = obj
  for (const p of parts) {
    if (cur == null) return ''
    cur = cur[p]
  }
  if (cur == null) return ''
  if (typeof cur === 'number') return cur.toFixed(2)
  return String(cur)
}

function corrKey(campo: string, te: string, per: string) {
  return `${campo}__${te}__${per}`
}

// Tab icon SVG paths
const TAB_ICONS: Record<string, string> = {
  'Identificação': 'M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z',
  'Ativo': 'M7 11l5-5m0 0l5 5m-5-5v12',
  'Passivo': 'M17 13l-5 5m0 0l-5-5m5 5V6',
  'DRE': 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z',
  'Fontes': 'M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1',
  'Texto OCR': 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z',
  'Pontos de Atenção': 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
  'Regras': 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z',
}

// Toast component
function Toast({ message, onDone }: { message: string; onDone: () => void }) {
  useEffect(() => {
    const t = setTimeout(onDone, 2500)
    return () => clearTimeout(t)
  }, [onDone])

  return (
    <div className="fixed bottom-6 right-6 z-50 animate-slide-up">
      <div className="flex items-center gap-2.5 bg-gray-900 text-white text-xs px-4 py-2.5 rounded-xl shadow-lg">
        <svg className="w-3.5 h-3.5 text-emerald-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
        </svg>
        {message}
      </div>
    </div>
  )
}

// Loading skeleton
function LoadingSkeleton() {
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="bg-white border-b border-gray-100 px-6 py-5 shrink-0">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-100 rounded-lg w-72 mb-2.5" />
          <div className="flex gap-4">
            <div className="h-3 bg-gray-100 rounded w-32" />
            <div className="h-3 bg-gray-100 rounded w-20" />
            <div className="h-3 bg-gray-100 rounded w-28" />
          </div>
          <div className="flex gap-1 mt-5 pb-px">
            {['Identificação', 'Ativo', 'Passivo', 'DRE', 'Fontes', 'Atenção'].map(t => (
              <div key={t} className="h-8 bg-gray-100 rounded-t-lg w-20 first:w-24" />
            ))}
          </div>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-6 space-y-2">
        {[...Array(9)].map((_, i) => (
          <div key={i} className={`animate-pulse flex items-center gap-3 bg-white rounded-lg border border-gray-100 px-4 py-3 ${i % 3 === 2 ? 'opacity-60' : ''}`}>
            <div className="h-4 bg-gray-100 rounded w-44 shrink-0" />
            <div className="h-4 bg-gray-100 rounded flex-1" />
            <div className="h-6 bg-gray-100 rounded w-16 shrink-0" />
          </div>
        ))}
      </div>
    </div>
  )
}

export default function FinancialReview({ documentName }: Props) {
  const [records, setRecords] = useState<DocRecord[]>([])
  const [recordIdx, setRecordIdx] = useState(0)
  const [corrections, setCorrections] = useState<CorrectionsMap>({})
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState(0)
  const [saving, setSaving] = useState<string | null>(null)
  const [saved, setSaved] = useState<string | null>(null)
  const [showPdf, setShowPdf] = useState(false)
  const [reprocessing, setReprocessing] = useState(false)
  const [reprocessError, setReprocessError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [submitConfirm, setSubmitConfirm] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [docStatus, setDocStatus] = useState<string>('em_revisao')
  const [toast, setToast] = useState<string | null>(null)
  const toastRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  function showToast(msg: string) {
    if (toastRef.current) clearTimeout(toastRef.current)
    setToast(msg)
  }

  useEffect(() => {
    setLoading(true)
    setRecords([])
    setRecordIdx(0)
    setCorrections({})
    setActiveTab(0)
    Promise.all([
      fetch(`/api/documents/${encodeURIComponent(documentName)}`).then(r => r.json()),
      fetch(`/api/corrections/${encodeURIComponent(documentName)}`).then(r => r.json()),
    ]).then(([doc, corr]) => {
      const recs: DocRecord[] = doc.records ?? (doc.data ? [{ tipo_entidade: null, periodo: null, data: doc.data }] : [])
      setRecords(recs)
      setCorrections(corr)
      setDocStatus(doc.status ?? 'em_revisao')
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [documentName])

  async function handleSave(campo: string, valorExtraido: string, valorCorreto: string, comentario: string) {
    const te  = current.tipo_entidade ?? ''
    const per = current.periodo ?? ''
    setSaving(campo)
    await fetch('/api/corrections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ document_name: documentName, tipo_entidade: te, periodo: per, campo, valor_extraido: valorExtraido, valor_correto: valorCorreto, comentario, auto_confirm: true }),
    })
    const key = corrKey(campo, te, per)
    const nowIso = new Date().toISOString()
    setCorrections(prev => ({
      ...prev,
      [key]: { campo, tipo_entidade: te, periodo: per, valor_extraido: valorExtraido, valor_correto: valorCorreto, comentario, status: 'confirmado', confirmado_em: nowIso, confirmado_por: null },
    }))
    setSaving(null)
    setSaved(campo)
    setTimeout(() => setSaved(null), 2000)
    showToast(valorCorreto !== valorExtraido ? 'Valor revisado' : 'Valor confirmado')
  }

  async function handleBulkConfirm(items: { campo: string; valor_extraido: string; valor_correto: string }[]) {
    const te  = current.tipo_entidade ?? ''
    const per = current.periodo ?? ''
    await fetch('/api/corrections/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ document_name: documentName, tipo_entidade: te, periodo: per, items }),
    })
    const nowIso = new Date().toISOString()
    setCorrections(prev => {
      const next = { ...prev }
      for (const it of items) {
        const key = corrKey(it.campo, te, per)
        next[key] = {
          campo: it.campo, tipo_entidade: te, periodo: per,
          valor_extraido: it.valor_extraido, valor_correto: it.valor_correto, comentario: '',
          status: 'confirmado', confirmado_em: nowIso, confirmado_por: null,
        }
      }
      return next
    })
    showToast(`${items.length} ${items.length === 1 ? 'campo confirmado' : 'campos confirmados'}`)
  }

  async function handleDelete(campo: string) {
    const te  = current.tipo_entidade ?? ''
    const per = current.periodo ?? ''
    await fetch(
      `/api/corrections/${encodeURIComponent(documentName)}/${encodeURIComponent(campo)}?tipo_entidade=${encodeURIComponent(te)}&periodo=${encodeURIComponent(per)}`,
      { method: 'DELETE' }
    )
    const key = corrKey(campo, te, per)
    setCorrections(prev => { const c = { ...prev }; delete c[key]; return c })
    showToast('Correção removida')
  }

  async function handleConfirm(campo: string) {
    const te  = current.tipo_entidade ?? ''
    const per = current.periodo ?? ''
    const r = await fetch(
      `/api/corrections/${encodeURIComponent(documentName)}/${encodeURIComponent(campo)}/confirm?tipo_entidade=${encodeURIComponent(te)}&periodo=${encodeURIComponent(per)}`,
      { method: 'POST' }
    )
    const body = await r.json()
    const key = corrKey(campo, te, per)
    setCorrections(prev => ({
      ...prev,
      [key]: { ...prev[key], status: 'confirmado', confirmado_em: body.confirmado_em ?? null, confirmado_por: body.confirmado_por ?? null },
    }))
    showToast('Correção confirmada')
  }

  async function handleReprocess() {
    setReprocessing(true)
    setReprocessError(null)
    try {
      const r = await fetch(`/api/documents/${encodeURIComponent(documentName)}/reprocess`, { method: 'POST' })
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: 'Erro desconhecido' }))
        setReprocessError(err.detail ?? 'Erro ao reprocessar')
        return
      }
      for (let i = 0; i < 60; i++) {
        await new Promise(res => setTimeout(res, 5000))
        const st = await fetch(`/api/documents/${encodeURIComponent(documentName)}/status`).then(r => r.json())
        if (st.status === 'done') break
        if (st.status === 'error') throw new Error(st.detail ?? 'Erro ao reprocessar')
      }
      const [doc, corr] = await Promise.all([
        fetch(`/api/documents/${encodeURIComponent(documentName)}`).then(r => r.json()),
        fetch(`/api/corrections/${encodeURIComponent(documentName)}`).then(r => r.json()),
      ])
      const recs: DocRecord[] = doc.records ?? (doc.data ? [{ tipo_entidade: null, periodo: null, data: doc.data }] : [])
      setRecords(recs)
      setRecordIdx(0)
      setCorrections(corr)
      showToast('Documento reprocessado com sucesso')
    } catch (e: unknown) {
      setReprocessError(e instanceof Error ? e.message : 'Erro ao reprocessar')
    } finally {
      setReprocessing(false)
    }
  }

  async function handleSubmit() {
    setSubmitting(true)
    setSubmitError(null)
    try {
      const r = await fetch(`/api/finalize/${encodeURIComponent(documentName)}`, { method: 'POST' })
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: 'Erro desconhecido' }))
        setSubmitError(err.detail ?? 'Erro ao submeter')
        return
      }
      setDocStatus('finalizado')
      // Atualizar status em cada registro localmente
      setRecords(prev => prev.map(rec => ({ ...rec, status: 'finalizado' })))
      setSubmitConfirm(false)
      showToast('Documento finalizado com sucesso')
    } catch (e: unknown) {
      setSubmitError(e instanceof Error ? e.message : 'Erro ao submeter')
    } finally {
      setSubmitting(false)
    }
  }

  if (loading) return <LoadingSkeleton />
  if (!records.length) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-red-500">Erro ao carregar documento.</p>
      </div>
    )
  }

  const current = records[recordIdx] ?? records[0]
  const data = current.data
  const curTe  = current.tipo_entidade ?? ''
  const curPer = current.periodo ?? ''

  const recordCorrections: Record<string, Correction> = {}
  for (const [key, corr] of Object.entries(corrections)) {
    if (key.endsWith(`__${curTe}__${curPer}`)) {
      recordCorrections[(corr as Correction).campo] = corr as Correction
    }
  }

  const assessmentMap: Record<string, AssessmentItem> = {}
  for (const item of (current.assessment ?? [])) {
    assessmentMap[item.campo] = item
  }

  const pendingCount   = Object.values(recordCorrections).filter(c => c.status !== 'confirmado').length
  const isFinalized = docStatus === 'finalizado'

  // Count LLM-puro fields (sem correção) — usado para gate do botão Submeter
  const reviewableFields = SECTIONS.flatMap(s => s.fields).filter(f => !f.isTotal && f.type !== 'text')
  const llmPuroCount = reviewableFields.filter(f => !recordCorrections[f.path]).length
  const canSubmit = llmPuroCount === 0

  // Per-section correction counts for badges
  const sectionCounts = SECTIONS.map(s => {
    const paths = new Set(s.fields.map(f => f.path))
    return Object.keys(recordCorrections).filter(k => paths.has(k)).length
  })

  const OCR_TAB    = SECTIONS.length
  const PONTOS_TAB = SECTIONS.length + 1
  const REGRAS_TAB = SECTIONS.length + 2

  const tabs = [
    ...SECTIONS.map((s, i) => ({ label: s.label, count: sectionCounts[i] })),
    { label: 'Texto OCR', count: 0 },
    { label: 'Pontos de Atenção', count: 0 },
    { label: 'Regras', count: 0 },
  ]

  const pdfUrl   = `/api/documents/${encodeURIComponent(documentName)}/pdf`
  const excelUrl = `/api/export/excel?document=${encodeURIComponent(documentName)}`

  function recordLabel(r: DocRecord) {
    const te  = r.tipo_entidade ?? 'INDIVIDUAL'
    const per = r.periodo ? r.periodo.substring(0, 10) : '—'
    return `${te} · ${per}`
  }

  return (
    <div className="flex h-full overflow-hidden">
      {/* PDF panel */}
      {showPdf && (
        <div className="w-1/2 shrink-0 border-r border-gray-200 flex flex-col bg-gray-50">
          <div className="flex items-center justify-between px-4 py-2.5 bg-white border-b border-gray-200">
            <span className="text-xs font-medium text-gray-500 truncate">{documentName}</span>
            <button
              onClick={() => setShowPdf(false)}
              className="w-6 h-6 flex items-center justify-center rounded hover:bg-gray-100 text-gray-400 hover:text-gray-600 transition-colors ml-2 shrink-0"
            >
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <iframe src={pdfUrl} className="flex-1 w-full" title={documentName} />
        </div>
      )}

      {/* Fields panel */}
      <div className="flex flex-col flex-1 min-w-0 h-full overflow-hidden">
        {/* Header */}
        <div className="bg-white border-b border-gray-100 px-6 pt-5 pb-0 shrink-0">
          {/* Top row */}
          <div className="flex items-start justify-between pb-4">
            <div className="min-w-0">
              <h2 className="text-lg font-bold text-gray-900 truncate leading-tight">
                {data?.razao_social ?? documentName}
              </h2>
              <div className="flex flex-wrap gap-x-4 gap-y-1 mt-1.5">
                {data?.cnpj && (
                  <span className="text-xs text-gray-500 tabular-nums">
                    <span className="text-gray-400 font-medium mr-1">CNPJ</span>{data.cnpj}
                  </span>
                )}
                {data?.identificacao?.periodo && (
                  <span className="text-xs text-gray-500">
                    <span className="text-gray-400 font-medium mr-1">Período</span>{data.identificacao.periodo}
                  </span>
                )}
                {data?.identificacao?.tipo_demonstrativo && (
                  <span className="text-xs text-gray-500">{data.identificacao.tipo_demonstrativo}</span>
                )}
                {current.processado_em && (
                  <span className="text-xs text-gray-500">
                    <span className="text-gray-400 font-medium mr-1">Processado</span>{new Date(current.processado_em).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })}
                  </span>
                )}
                {current.modo_extracao && (
                  <span
                    className={
                      current.modo_extracao === 'vision'
                        ? 'inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full bg-purple-100 text-purple-700'
                        : 'inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full bg-blue-100 text-blue-700'
                    }
                    title={current.modo_extracao === 'vision'
                      ? 'Processado via Vision OCR (Claude Sonnet Vision)'
                      : 'Processado via ai_parse_document (modo padrão)'}
                  >
                    {current.modo_extracao === 'vision' ? '⚡ Vision OCR' : '📄 ai_parse'}
                  </span>
                )}
              </div>
            </div>

            {/* Actions */}
            <div className="flex items-center gap-2 shrink-0 ml-4">
              {/* Status badge */}
              {isFinalized ? (
                <span
                  className="inline-flex items-center gap-1.5 bg-emerald-100 text-emerald-800 text-xs font-bold px-2.5 py-1 rounded-full border border-emerald-300"
                  title={current.finalizado_por ? `Finalizado por ${current.finalizado_por}${current.finalizado_em ? ` em ${current.finalizado_em.substring(0, 19).replace('T', ' ')}` : ''}` : 'Documento finalizado'}
                >
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  Finalizado
                </span>
              ) : (
                <span className="inline-flex items-center gap-1.5 bg-blue-50 text-blue-700 text-xs font-semibold px-2.5 py-1 rounded-full border border-blue-200">
                  <span className="w-1.5 h-1.5 rounded-full bg-blue-500" />
                  Em revisão
                </span>
              )}

              {!isFinalized && llmPuroCount > 0 && (
                <span className="inline-flex items-center gap-1.5 bg-gray-50 text-gray-600 text-xs font-medium px-2.5 py-1 rounded-full border border-gray-200">
                  <span className="w-1.5 h-1.5 rounded-full bg-gray-400" />
                  {llmPuroCount} para revisar
                </span>
              )}

              {!isFinalized && (
                <button
                  onClick={() => canSubmit && setSubmitConfirm(true)}
                  disabled={submitting || !canSubmit}
                  title={canSubmit ? '' : `Revise os ${llmPuroCount} campo${llmPuroCount !== 1 ? 's' : ''} pendente${llmPuroCount !== 1 ? 's' : ''} antes de submeter`}
                  className={`inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg transition-all font-semibold ${
                    canSubmit
                      ? 'bg-emerald-600 text-white hover:bg-emerald-700 shadow-sm disabled:opacity-40'
                      : 'bg-white text-gray-400 border border-gray-200 cursor-not-allowed'
                  }`}
                >
                  <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                  </svg>
                  {submitting ? 'Submetendo…' : 'Submeter'}
                </button>
              )}

              <button
                onClick={handleReprocess}
                disabled={reprocessing || isFinalized}
                className="inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-gray-600 hover:bg-gray-50 hover:border-gray-300 transition-all disabled:opacity-40"
              >
                <svg className={`w-3.5 h-3.5 ${reprocessing ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                {reprocessing ? 'Reprocessando…' : 'Reprocessar'}
              </button>

              <a
                href={excelUrl}
                download
                className="inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-gray-200 bg-white text-gray-600 hover:bg-gray-50 hover:border-gray-300 transition-all"
              >
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                </svg>
                Excel
              </a>

              <button
                onClick={() => setShowPdf(v => !v)}
                className={`inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border transition-all ${
                  showPdf ? 'bg-[#0F2137] text-white border-[#0F2137]' : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50 hover:border-gray-300'
                }`}
              >
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                {showPdf ? 'Fechar PDF' : 'Ver PDF'}
              </button>
            </div>
          </div>

          {/* Error */}
          {reprocessError && (
            <div className="mb-3 flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
              <svg className="w-3.5 h-3.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              {reprocessError}
            </div>
          )}

          {/* Period selector */}
          {records.length > 1 && (
            <div className="flex gap-1.5 pb-3 flex-wrap">
              {records.map((r, i) => (
                <button
                  key={i}
                  onClick={() => setRecordIdx(i)}
                  className={`text-xs px-3 py-1 rounded-full border font-medium transition-all ${
                    recordIdx === i
                      ? 'bg-[#0F2137] text-white border-[#0F2137]'
                      : 'bg-white text-gray-600 border-gray-200 hover:border-[#0F2137]/30 hover:text-[#0F2137]'
                  }`}
                >
                  {recordLabel(r)}
                </button>
              ))}
            </div>
          )}

          {/* Tabs */}
          <div className="flex gap-0 overflow-x-auto">
            {tabs.map((tab, i) => {
              const isActive = activeTab === i
              const iconPath = TAB_ICONS[tab.label]
              return (
                <button
                  key={tab.label}
                  onClick={() => setActiveTab(i)}
                  className={`flex items-center gap-1.5 px-4 py-2.5 text-xs font-medium whitespace-nowrap border-b-2 transition-all ${
                    isActive
                      ? 'border-[#0F2137] text-[#0F2137]'
                      : 'border-transparent text-gray-400 hover:text-gray-600 hover:border-gray-200'
                  }`}
                >
                  {iconPath && (
                    <svg className="w-3.5 h-3.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.8} d={iconPath} />
                    </svg>
                  )}
                  {tab.label}
                  {tab.count > 0 && (
                    <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full min-w-[18px] text-center leading-tight ${
                      isActive ? 'bg-[#0F2137] text-white' : 'bg-amber-100 text-amber-700'
                    }`}>
                      {tab.count}
                    </span>
                  )}
                </button>
              )
            })}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {activeTab === REGRAS_TAB ? (
            <RulesPanel />
          ) : activeTab === PONTOS_TAB ? (
            <PontosDeAtencao records={[current]} />
          ) : activeTab === OCR_TAB ? (
            <OcrTextPanel documentName={documentName} />
          ) : (
            <FieldSection
              section={SECTIONS[activeTab]}
              data={data}
              scale={data?.identificacao?.escala_valores || ''}
              corrections={recordCorrections}
              assessment={assessmentMap}
              saving={saving}
              saved={saved}
              documentName={documentName}
              getValue={getNestedValue}
              onSave={handleSave}
              onDelete={handleDelete}
              onConfirm={handleConfirm}
              onBulkConfirm={handleBulkConfirm}
              readOnly={isFinalized}
            />
          )}
        </div>
      </div>

      {/* Submit confirmation modal */}
      {submitConfirm && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/50">
          <div className="bg-white rounded-xl shadow-xl max-w-md w-full mx-4 p-6">
            <div className="flex items-start gap-3 mb-4">
              <div className="w-10 h-10 rounded-full bg-amber-50 border border-amber-200 flex items-center justify-center shrink-0">
                <svg className="w-5 h-5 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M12 3l9.66 16.5H2.34L12 3z" />
                </svg>
              </div>
              <div>
                <h3 className="text-base font-semibold text-gray-900">Submeter documento?</h3>
                <p className="text-sm text-gray-600 mt-1">
                  Esta ação é <strong>irreversível</strong>. Após submeter, o documento será marcado como
                  <strong> Finalizado</strong> e nenhuma edição será mais permitida.
                </p>
                {pendingCount > 0 && (
                  <p className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1.5 mt-2">
                    Atenção: ainda há {pendingCount} correção{pendingCount !== 1 ? 'ões' : ''} legada{pendingCount !== 1 ? 's' : ''} pendente{pendingCount !== 1 ? 's' : ''} de confirmação.
                  </p>
                )}
              </div>
            </div>
            {submitError && (
              <div className="mb-3 flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                {submitError}
              </div>
            )}
            <div className="flex justify-end gap-2 mt-4">
              <button
                onClick={() => { setSubmitConfirm(false); setSubmitError(null) }}
                disabled={submitting}
                className="text-sm px-4 py-2 rounded-lg border border-gray-200 text-gray-700 hover:bg-gray-50 disabled:opacity-40"
              >
                Cancelar
              </button>
              <button
                onClick={handleSubmit}
                disabled={submitting}
                className="text-sm px-4 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 font-semibold disabled:opacity-40"
              >
                {submitting ? 'Submetendo…' : 'Confirmar e submeter'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Toast */}
      {toast && <Toast message={toast} onDone={() => setToast(null)} />}
    </div>
  )
}
