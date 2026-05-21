import { useEffect, useState } from 'react'
import { IconInfoCircle, IconChevronDown, IconChevronRight } from '@tabler/icons-react'

// ─── Types ───────────────────────────────────────────────────────────────────
interface ByDocVal {
  document_name: string; razao_social: string
  ok: number; warn: number; error: number; records: number
}
interface ByValidation {
  label: string; error_docs: number; warn_docs: number; ok_docs: number
}
interface Validacoes {
  total: number; ok: number; warn: number; error: number
  by_doc: ByDocVal[]; by_validation: ByValidation[]
}
interface ByVersao {
  modelo_versao: string; amostra: number
  confirmados: number; corrigidos: number; taxa_confirmacao: number | null
}
interface TopCampo {
  campo: string; revisoes: number; correcoes: number; taxa_correcao: number
}
interface TipoErro { tipo_erro: string; ocorrencias: number }
interface Acuracia {
  total_campos_extraidos: number; total_records: number
  revisado: number; confirmados: number; corrigidos: number
  cobertura_pct: number; confirmacao_pct: number | null
  by_versao: ByVersao[]
  top_campos: TopCampo[]
  tipos_erro: TipoErro[]; tipos_erro_nao_classif: number
}
interface ByUser {
  usuario: string; revisoes: number
  confirmacoes: number; correcoes: number
  ultima_atividade: string | null
}
interface RecentItem {
  document_name: string; campo: string
  valor_llm: string | null; valor_final: string | null
  acao: string; tipo_erro: string; comentario: string
  revisado_por: string; revisado_em: string
  modelo_versao: string
}
interface Atividade {
  docs_revisados: number
  tempo_medio_min: number | null; tempo_mediana_min: number | null
  by_user: ByUser[]; recent: RecentItem[]
}
interface MetricsData {
  view: 'global' | 'document'
  document_name?: string; razao_social?: string
  total_docs?: number
  validacoes: Validacoes
  acuracia: Acuracia
  atividade: Atividade
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
const brNum = new Intl.NumberFormat('pt-BR')
function fmt(n: number | null | undefined): string {
  if (n == null) return '—'
  return brNum.format(n)
}
function deltaPp(curr: number | null, prev: number | null): string {
  if (curr == null || prev == null) return ''
  const d = +(curr - prev).toFixed(1)
  return (d >= 0 ? '+' : '') + d + 'pp'
}
function deltaColor(curr: number | null, prev: number | null): string {
  if (curr == null || prev == null) return 'text-gray-400'
  const d = curr - prev
  if (d > 0.1) return 'text-emerald-600'
  if (d < -0.1) return 'text-red-600'
  return 'text-gray-500'
}

// ─── Block ─────────────────────────────────────────────────────────────────
function Block({
  title, subtitle, disclaimer, defaultOpen = true, children,
}: {
  title: string; subtitle: string; disclaimer: string
  defaultOpen?: boolean; children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <section className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
      <header
        className="px-5 py-4 border-b border-gray-100 cursor-pointer hover:bg-gray-50"
        onClick={() => setOpen(o => !o)}
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              {open ? <IconChevronDown size={16} className="text-gray-400" /> : <IconChevronRight size={16} className="text-gray-400" />}
              <h2 className="text-base font-semibold text-gray-900">{title}</h2>
            </div>
            <p className="text-xs text-gray-500 mt-1 ml-6">{subtitle}</p>
          </div>
        </div>
        <p className="mt-3 ml-6 flex items-start gap-1.5 text-[11px] text-gray-500 leading-relaxed">
          <IconInfoCircle size={12} className="shrink-0 mt-0.5 text-gray-400" />
          <span>{disclaimer}</span>
        </p>
      </header>
      {open && <div className="px-5 py-4 space-y-4">{children}</div>}
    </section>
  )
}

// ─── Bloco 1: Validações ───────────────────────────────────────────────────
function BlockValidacoes({ v, totalDocs }: { v: Validacoes; totalDocs: number }) {
  return (
    <Block
      title="Validações Contábeis"
      subtitle="Integridade estrutural das demonstrações financeiras"
      disclaimer="Mede consistência interna dos documentos extraídos. Não é acurácia do modelo — erros podem vir do PDF original."
    >
      <div className="bg-gray-50 border border-gray-100 rounded-lg p-4">
        <p className="text-sm text-gray-700">
          <span className="font-semibold tabular-nums">{fmt(v.total)}</span> validações executadas em{' '}
          <span className="font-semibold tabular-nums">{fmt(totalDocs)}</span> documento{totalDocs !== 1 ? 's' : ''}
        </p>
        <div className="flex gap-4 mt-2 text-xs">
          <span className="inline-flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-emerald-500" /><span className="tabular-nums font-medium">{fmt(v.ok)}</span> OK</span>
          <span className="inline-flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-amber-500" /><span className="tabular-nums font-medium">{fmt(v.warn)}</span> Avisos</span>
          <span className="inline-flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-red-500" /><span className="tabular-nums font-medium">{fmt(v.error)}</span> Erros</span>
        </div>
      </div>

      {v.by_doc.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Por documento (mais erros primeiro)</p>
          <div className="border border-gray-100 rounded-lg divide-y divide-gray-100 max-h-72 overflow-y-auto">
            {v.by_doc.slice(0, 20).map(d => (
              <div key={d.document_name} className="flex items-center gap-3 px-3 py-2 text-xs">
                <span className="flex-1 truncate" title={d.document_name}>{d.razao_social}</span>
                <span className="text-red-600 tabular-nums w-20 text-right">{d.error} erro{d.error !== 1 ? 's' : ''}</span>
                <span className="text-amber-600 tabular-nums w-20 text-right">{d.warn} aviso{d.warn !== 1 ? 's' : ''}</span>
                <span className="text-gray-400 tabular-nums w-14 text-right">{d.ok} ok</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {v.by_validation.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Por tipo de validação</p>
          <div className="border border-gray-100 rounded-lg divide-y divide-gray-100">
            {v.by_validation.slice(0, 15).map(b => (
              <div key={b.label} className="flex items-center gap-3 px-3 py-2 text-xs">
                <span className="flex-1 truncate" title={b.label}>{b.label}</span>
                <span className="text-red-600 tabular-nums w-24 text-right">{b.error_docs} doc{b.error_docs !== 1 ? 's' : ''} erro</span>
                <span className="text-amber-600 tabular-nums w-24 text-right">{b.warn_docs} aviso{b.warn_docs !== 1 ? 's' : ''}</span>
                <span className="text-gray-400 tabular-nums w-16 text-right">{b.ok_docs} ok</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </Block>
  )
}

// ─── Bloco 2: Acurácia ─────────────────────────────────────────────────────
function BlockAcuracia({ a }: { a: Acuracia }) {
  const tipoErroLabels: Record<string, string> = {
    conta_campo_errado: 'Conta no campo errado',
    faltou_somar_contas: 'Faltou somar contas',
    campo_vazio_incorreto: 'Faltou extrair valor',
    numero_errado: 'Valor numérico incorreto',
    escala_errada: 'Escala errada (milhar/unidade)',
    sinal_trocado: 'Sinal trocado (+/−)',
    conta_inventada: 'Valor não existe no PDF',
    outro: 'Outro',
  }
  const totalTipos = a.tipos_erro.reduce((s, t) => s + t.ocorrencias, 0)
  const totalCorrigidos = totalTipos + a.tipos_erro_nao_classif

  return (
    <Block
      title="Acurácia do Modelo"
      subtitle="Performance do LLM medida pela revisão humana"
      disclaimer="Toda métrica é relativa à amostra revisada. Campos não revisados não contam — não fingimos saber sobre eles."
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div className="bg-gray-50 border border-gray-100 rounded-lg p-4">
          <p className="text-[11px] text-gray-500 uppercase tracking-wider">Cobertura de revisão</p>
          <p className="text-2xl font-bold text-gray-900 mt-1 tabular-nums">{a.cobertura_pct}%</p>
          <p className="text-xs text-gray-500 mt-0.5">
            ({fmt(a.revisado)} de {fmt(a.total_campos_extraidos)} campos extraídos)
          </p>
        </div>
        <div className="bg-gray-50 border border-gray-100 rounded-lg p-4">
          <p className="text-[11px] text-gray-500 uppercase tracking-wider">Taxa de confirmação na amostra revisada</p>
          <p className="text-2xl font-bold text-gray-900 mt-1 tabular-nums">
            {a.confirmacao_pct != null ? `${a.confirmacao_pct}%` : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">
            ({fmt(a.confirmados)} confirmados · {fmt(a.corrigidos)} corrigidos · n={fmt(a.revisado)})
          </p>
        </div>
      </div>
      {a.total_campos_extraidos > a.revisado && (
        <p className="text-[11px] text-gray-500 flex items-start gap-1.5">
          <IconInfoCircle size={12} className="shrink-0 mt-0.5 text-gray-400" />
          Sobre os {fmt(a.total_campos_extraidos - a.revisado)} campos não revisados, não temos evidência empírica de acerto ou erro.
        </p>
      )}

      {a.by_versao.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Evolução por versão do modelo</p>
          <div className="border border-gray-100 rounded-lg divide-y divide-gray-100">
            {a.by_versao.map((v, i) => {
              const prev = i > 0 ? a.by_versao[i - 1] : null
              const d = prev ? deltaPp(v.taxa_confirmacao, prev.taxa_confirmacao) : ''
              const dCls = prev ? deltaColor(v.taxa_confirmacao, prev.taxa_confirmacao) : ''
              return (
                <div key={v.modelo_versao} className="flex items-center gap-3 px-3 py-2 text-xs">
                  <span className="flex-1 font-mono text-gray-700">{v.modelo_versao}</span>
                  <span className="tabular-nums font-medium text-gray-900">{v.taxa_confirmacao ?? '—'}%</span>
                  <span className="text-gray-500 tabular-nums">(n={fmt(v.amostra)})</span>
                  {d && <span className={`tabular-nums w-16 text-right ${dCls}`}>Δ {d}</span>}
                </div>
              )
            })}
          </div>
          <p className="text-[10px] text-gray-400 mt-1.5 flex items-start gap-1">
            <IconInfoCircle size={11} className="shrink-0 mt-0.5" />
            Δ relativo à versão anterior. Amostras diferentes — comparação não é exata.
          </p>
        </div>
      )}

      {a.top_campos.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Campos onde o LLM erra mais (n ≥ 5 revisões)</p>
          <div className="border border-gray-100 rounded-lg divide-y divide-gray-100">
            {a.top_campos.map(c => (
              <div key={c.campo} className="flex items-center gap-3 px-3 py-2 text-xs">
                <span className="flex-1 font-mono text-gray-700 truncate" title={c.campo}>{c.campo}</span>
                <span className="tabular-nums font-medium text-red-600 w-16 text-right">{c.taxa_correcao}%</span>
                <span className="text-gray-500 tabular-nums">({c.correcoes} de {c.revisoes})</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {(a.tipos_erro.length > 0 || a.tipos_erro_nao_classif > 0) && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Tipos de erro nas correções (com classificação)</p>
          <div className="space-y-1.5">
            {a.tipos_erro.map(t => {
              const pct = totalTipos > 0 ? Math.round(t.ocorrencias / totalTipos * 100) : 0
              return (
                <div key={t.tipo_erro} className="flex items-center gap-3 text-xs">
                  <span className="w-44 truncate text-gray-700">{tipoErroLabels[t.tipo_erro] || t.tipo_erro}</span>
                  <div className="flex-1 h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div className="h-full bg-blue-500" style={{ width: `${pct}%` }} />
                  </div>
                  <span className="tabular-nums text-gray-600 w-20 text-right">{t.ocorrencias} ({pct}%)</span>
                </div>
              )
            })}
          </div>
          <p className="text-[11px] text-gray-500 mt-2 flex items-start gap-1.5">
            <IconInfoCircle size={11} className="shrink-0 mt-0.5 text-gray-400" />
            {fmt(totalTipos)} correç{totalTipos !== 1 ? 'ões' : 'ão'} classificada{totalTipos !== 1 ? 's' : ''} de {fmt(totalCorrigidos)} totais
            {a.tipos_erro_nao_classif > 0 && <>. {a.tipos_erro_nao_classif} sem classificação (legado ou não preenchido).</>}
          </p>
        </div>
      )}
    </Block>
  )
}

// ─── Bloco 3: Atividade ────────────────────────────────────────────────────
function BlockAtividade({ at, totalDocs, totalRecords }: { at: Atividade; totalDocs: number; totalRecords: number }) {
  return (
    <Block
      title="Atividade de Revisão"
      subtitle="Produtividade da equipe e histórico de operações"
      disclaimer="Mede trabalho realizado pela equipe, não qualidade do modelo. Mais correções podem indicar modelo pior OU equipe mais atenta."
    >
      <div className="bg-gray-50 border border-gray-100 rounded-lg p-4 text-sm">
        <p className="text-gray-700">
          <span className="font-semibold tabular-nums">{fmt(at.docs_revisados)}</span> documento{at.docs_revisados !== 1 ? 's' : ''} com revisão (de {fmt(totalDocs)})
        </p>
        <p className="text-xs text-gray-500 mt-1 tabular-nums">{fmt(totalRecords * 70)} campos extraídos no total</p>
        {(at.tempo_medio_min != null || at.tempo_mediana_min != null) && (
          <p className="text-xs text-gray-600 mt-2">
            Tempo de revisão por documento (ingestão → submissão):
            {at.tempo_medio_min != null   && <> média <span className="font-semibold tabular-nums">{at.tempo_medio_min} min</span></>}
            {at.tempo_mediana_min != null && <> · mediana <span className="font-semibold tabular-nums">{at.tempo_mediana_min} min</span></>}
          </p>
        )}
      </div>

      {at.by_user.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Atividade por revisor</p>
          <div className="border border-gray-100 rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead className="bg-gray-50">
                <tr className="text-gray-500">
                  <th className="text-left px-3 py-2 font-medium">Revisor</th>
                  <th className="text-right px-3 py-2 font-medium">Revisões</th>
                  <th className="text-right px-3 py-2 font-medium">Confirmações</th>
                  <th className="text-right px-3 py-2 font-medium">Correções</th>
                  <th className="text-right px-3 py-2 font-medium">Última atividade</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {at.by_user.map(u => (
                  <tr key={u.usuario}>
                    <td className="px-3 py-2 truncate">{u.usuario}</td>
                    <td className="px-3 py-2 text-right tabular-nums">{u.revisoes}</td>
                    <td className="px-3 py-2 text-right tabular-nums text-emerald-700">{u.confirmacoes}</td>
                    <td className="px-3 py-2 text-right tabular-nums text-amber-700">{u.correcoes}</td>
                    <td className="px-3 py-2 text-right text-gray-500 tabular-nums">{u.ultima_atividade ? u.ultima_atividade.substring(0, 19).replace('T', ' ') : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {at.recent.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-700 mb-2">Atividade recente</p>
          <div className="border border-gray-100 rounded-lg overflow-hidden max-h-96 overflow-y-auto">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 sticky top-0">
                <tr className="text-gray-500">
                  <th className="text-left px-3 py-2 font-medium">Quando</th>
                  <th className="text-left px-3 py-2 font-medium">Documento</th>
                  <th className="text-left px-3 py-2 font-medium">Campo</th>
                  <th className="text-left px-3 py-2 font-medium">Ação</th>
                  <th className="text-left px-3 py-2 font-medium">LLM → Final</th>
                  <th className="text-left px-3 py-2 font-medium">Tipo</th>
                  <th className="text-left px-3 py-2 font-medium">Versão</th>
                  <th className="text-left px-3 py-2 font-medium">Por</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {at.recent.map((r, i) => (
                  <tr key={i}>
                    <td className="px-3 py-2 text-gray-500 tabular-nums whitespace-nowrap">{r.revisado_em?.substring(0, 19).replace('T', ' ') || '—'}</td>
                    <td className="px-3 py-2 truncate max-w-[160px]" title={r.document_name}>{r.document_name}</td>
                    <td className="px-3 py-2 font-mono text-gray-700">{r.campo}</td>
                    <td className="px-3 py-2">
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${r.acao === 'corrigido' ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800'}`}>
                        {r.acao}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono tabular-nums">
                      {r.acao === 'corrigido' ? (
                        <span><span className="text-gray-400 line-through">{r.valor_llm}</span> → <span className="text-gray-900">{r.valor_final}</span></span>
                      ) : (
                        <span className="text-gray-500">{r.valor_final}</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-gray-600">{r.tipo_erro || '—'}</td>
                    <td className="px-3 py-2 text-gray-500 font-mono text-[10px]">{r.modelo_versao || '—'}</td>
                    <td className="px-3 py-2 text-gray-500 truncate max-w-[120px]" title={r.revisado_por}>{r.revisado_por}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </Block>
  )
}

// ─── Documentos para o seletor ─────────────────────────────────────────────
interface DocOption { document_name: string; razao_social: string | null }

// ─── Main ──────────────────────────────────────────────────────────────────
export default function MetricsDashboard() {
  const [view, setView] = useState<'global' | 'document'>('global')
  const [selectedDoc, setSelectedDoc] = useState<string>('')
  const [data, setData] = useState<MetricsData | null>(null)
  const [docs, setDocs] = useState<DocOption[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/documentos/sidebar-state').then(r => r.json()).then(d => setDocs(d.documentos || [])).catch(() => setDocs([]))
  }, [])

  useEffect(() => {
    if (view === 'document' && !selectedDoc) { setLoading(false); return }
    setLoading(true)
    const url = view === 'global'
      ? '/api/metrics'
      : `/api/metrics/${encodeURIComponent(selectedDoc)}`
    fetch(url).then(r => r.json()).then(d => { setData(d); setLoading(false) }).catch(() => setLoading(false))
  }, [view, selectedDoc])

  return (
    <div className="h-full overflow-y-auto bg-gray-50 px-6 py-5">
      <div className="max-w-5xl mx-auto space-y-4">
        {/* Header */}
        <div className="flex items-start justify-between mb-2">
          <div>
            <h1 className="text-xl font-bold text-gray-900">Métricas</h1>
            <p className="text-xs text-gray-500 mt-1">
              {view === 'document' && data?.view === 'document'
                ? `Documento: ${data.razao_social ?? data.document_name}`
                : 'Visão consolidada de todos os documentos'}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <div className="flex bg-white border border-gray-200 rounded-lg overflow-hidden">
              <button
                onClick={() => setView('global')}
                className={`text-xs px-3 py-1.5 font-medium ${view === 'global' ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-50'}`}
              >
                Global
              </button>
              <button
                onClick={() => setView('document')}
                className={`text-xs px-3 py-1.5 font-medium ${view === 'document' ? 'bg-gray-900 text-white' : 'text-gray-600 hover:bg-gray-50'}`}
              >
                Por Documento
              </button>
            </div>
            {view === 'document' && (
              <select
                value={selectedDoc}
                onChange={e => setSelectedDoc(e.target.value)}
                className="text-xs px-2 py-1.5 border border-gray-200 rounded-lg bg-white max-w-[260px]"
              >
                <option value="">Selecione…</option>
                {docs.map(d => (
                  <option key={d.document_name} value={d.document_name}>
                    {d.razao_social || d.document_name}
                  </option>
                ))}
              </select>
            )}
          </div>
        </div>

        {loading && <div className="text-sm text-gray-500 py-12 text-center">Carregando métricas…</div>}

        {!loading && view === 'document' && !selectedDoc && (
          <div className="bg-white border border-gray-200 rounded-xl p-8 text-center text-sm text-gray-500">
            Selecione um documento para ver suas métricas.
          </div>
        )}

        {!loading && data && (view === 'global' || selectedDoc) && (
          <>
            <BlockValidacoes v={data.validacoes} totalDocs={data.total_docs ?? 1} />
            <BlockAcuracia   a={data.acuracia} />
            <BlockAtividade  at={data.atividade} totalDocs={data.total_docs ?? 1} totalRecords={data.acuracia.total_records} />
          </>
        )}
      </div>
    </div>
  )
}
