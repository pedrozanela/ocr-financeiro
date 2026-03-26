import { useEffect, useState } from 'react'
import FieldSection from './FieldSection'
import FontesPanel from './FontesPanel'
import PontosDeAtencao from './PontosDeAtencao'
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
  status: string          // 'pendente' | 'confirmado'
  confirmado_em: string | null
  confirmado_por: string | null
}

// Global map: key = "campo__tipo_entidade__periodo"
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

  useEffect(() => {
    setLoading(true)
    setRecords([])
    setRecordIdx(0)
    setCorrections({})
    Promise.all([
      fetch(`/api/documents/${encodeURIComponent(documentName)}`).then(r => r.json()),
      fetch(`/api/corrections/${encodeURIComponent(documentName)}`).then(r => r.json()),
    ]).then(([doc, corr]) => {
      const recs: DocRecord[] = doc.records ?? (doc.data ? [{ tipo_entidade: null, periodo: null, data: doc.data }] : [])
      setRecords(recs)
      setCorrections(corr)
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
      body: JSON.stringify({
        document_name: documentName,
        tipo_entidade: te,
        periodo: per,
        campo,
        valor_extraido: valorExtraido,
        valor_correto: valorCorreto,
        comentario,
      }),
    })
    const key = corrKey(campo, te, per)
    setCorrections(prev => ({
      ...prev,
      [key]: { campo, tipo_entidade: te, periodo: per, valor_extraido: valorExtraido, valor_correto: valorCorreto, comentario, status: 'pendente', confirmado_em: null, confirmado_por: null },
    }))
    setSaving(null)
    setSaved(campo)
    setTimeout(() => setSaved(null), 2000)
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
    } catch (e: unknown) {
      setReprocessError(e instanceof Error ? e.message : 'Erro ao reprocessar')
    } finally {
      setReprocessing(false)
    }
  }

  if (loading) {
    return <div className="flex items-center justify-center h-full text-gray-400 text-sm">Carregando…</div>
  }
  if (!records.length) {
    return <div className="flex items-center justify-center h-full text-red-500 text-sm">Erro ao carregar documento.</div>
  }

  const current = records[recordIdx] ?? records[0]
  const data = current.data
  const curTe  = current.tipo_entidade ?? ''
  const curPer = current.periodo ?? ''

  // Filter corrections to current record only (keyed by campo for FieldSection)
  const recordCorrections: Record<string, Correction> = {}
  for (const [key, corr] of Object.entries(corrections)) {
    if (key.endsWith(`__${curTe}__${curPer}`)) {
      recordCorrections[corr.campo] = corr
    }
  }

  // Build assessment map keyed by campo for the current record
  const assessmentMap: Record<string, AssessmentItem> = {}
  for (const item of (current.assessment ?? [])) {
    assessmentMap[item.campo] = item
  }

  const pendingCount   = Object.values(recordCorrections).filter(c => c.status !== 'confirmado').length
  const confirmedCount = Object.values(recordCorrections).filter(c => c.status === 'confirmado').length

  const FONTES_TAB = SECTIONS.length
  const PONTOS_TAB = SECTIONS.length + 1
  const tabs = [...SECTIONS.map(s => s.label), 'Fontes', 'Pontos de Atenção']
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
        <div className="w-1/2 shrink-0 border-r border-gray-200 flex flex-col bg-gray-100">
          <div className="flex items-center justify-between px-3 py-2 bg-white border-b border-gray-200">
            <span className="text-xs font-medium text-gray-600 truncate">{documentName}</span>
            <button
              onClick={() => setShowPdf(false)}
              className="text-gray-400 hover:text-gray-600 transition-colors ml-2 shrink-0"
              title="Fechar PDF"
            >
              ✕
            </button>
          </div>
          <iframe src={pdfUrl} className="flex-1 w-full" title={documentName} />
        </div>
      )}

      {/* Fields panel */}
      <div className="flex flex-col flex-1 min-w-0 h-full overflow-hidden">
        {/* Header */}
        <div className="bg-white border-b border-gray-200 px-6 py-4 shrink-0">
          <div className="flex items-start justify-between">
            <div className="min-w-0">
              <h2 className="text-base font-bold text-gray-900 truncate">{data?.razao_social ?? documentName}</h2>
              <div className="flex flex-wrap gap-4 mt-1 text-xs text-gray-500">
                {data?.cnpj && <span>CNPJ: {data.cnpj}</span>}
                {data?.identificacao?.periodo && <span>Período: {data.identificacao.periodo}</span>}
                {data?.identificacao?.tipo_demonstrativo && <span>{data.identificacao.tipo_demonstrativo}</span>}
                {data?.identificacao?.escala_valores && <span>Escala: {data.identificacao.escala_valores}</span>}
              </div>
            </div>
            <div className="flex items-center gap-2 shrink-0 ml-3">
              {pendingCount > 0 && (
                <span className="bg-amber-100 text-amber-700 text-xs font-medium px-2.5 py-1 rounded-full">
                  {pendingCount} pendente{pendingCount !== 1 ? 's' : ''}
                </span>
              )}
              {confirmedCount > 0 && (
                <span className="bg-green-100 text-green-700 text-xs font-medium px-2.5 py-1 rounded-full">
                  {confirmedCount} confirmada{confirmedCount !== 1 ? 's' : ''}
                </span>
              )}
              <button
                onClick={handleReprocess}
                disabled={reprocessing}
                className="text-xs px-3 py-1.5 rounded border border-gray-300 bg-white text-gray-600 hover:border-[#0F2137] hover:text-[#0F2137] transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                title="Reprocessar PDF com o modelo mais recente"
              >
                {reprocessing ? 'Reprocessando…' : '↺ Reprocessar'}
              </button>
              <a
                href={excelUrl}
                download
                className="text-xs px-3 py-1.5 rounded border border-gray-300 bg-white text-gray-600 hover:border-[#0F2137] hover:text-[#0F2137] transition-colors"
                title="Exportar este documento para Excel"
              >
                ⬇ Excel
              </a>
              <button
                onClick={() => setShowPdf(v => !v)}
                className={`text-xs px-3 py-1.5 rounded border transition-colors ${
                  showPdf
                    ? 'bg-[#0F2137] text-white border-[#0F2137]'
                    : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
                }`}
              >
                {showPdf ? '✕ Fechar PDF' : '📄 Ver PDF'}
              </button>
            </div>
          </div>
          {reprocessError && (
            <div className="mt-2 text-xs text-red-600 bg-red-50 border border-red-200 rounded px-3 py-1.5">
              {reprocessError}
            </div>
          )}

          {/* Entity / Period selector */}
          {records.length > 1 && (
            <div className="flex gap-1 mt-3 flex-wrap">
              {records.map((r, i) => (
                <button
                  key={i}
                  onClick={() => setRecordIdx(i)}
                  className={`text-xs px-2.5 py-1 rounded-full border transition-colors ${
                    recordIdx === i
                      ? 'bg-[#0F2137] text-white border-[#0F2137]'
                      : 'bg-white text-gray-600 border-gray-300 hover:border-[#0F2137] hover:text-[#0F2137]'
                  }`}
                >
                  {recordLabel(r)}
                </button>
              ))}
            </div>
          )}

          {/* Tabs */}
          <div className="flex gap-1 mt-4 -mb-px">
            {tabs.map((tab, i) => (
              <button
                key={tab}
                onClick={() => setActiveTab(i)}
                className={`px-3 py-1.5 text-xs font-medium rounded-t border-b-2 transition-colors ${
                  activeTab === i
                    ? 'border-[#0F2137] text-[#0F2137]'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                {tab}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {activeTab === PONTOS_TAB ? (
            <PontosDeAtencao records={[current]} />
          ) : activeTab === FONTES_TAB ? (
            <FontesPanel data={data} />
          ) : (
            <FieldSection
              section={SECTIONS[activeTab]}
              data={data}
              scale={data?.identificacao?.escala_valores || ''}
              corrections={recordCorrections}
              assessment={assessmentMap}
              saving={saving}
              saved={saved}
              getValue={getNestedValue}
              onSave={handleSave}
              onDelete={handleDelete}
              onConfirm={handleConfirm}
            />
          )}
        </div>
      </div>
    </div>
  )
}
