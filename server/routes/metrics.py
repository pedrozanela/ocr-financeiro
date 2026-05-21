"""Métricas — 3 blocos conceituais separados.

Bloco 1 — Validações Contábeis: integridade estrutural (Ativo=Passivo, soma de componentes, etc).
Bloco 2 — Acurácia do Modelo: cobertura + taxa de confirmação na amostra revisada + evolução por versão.
Bloco 3 — Atividade de Revisão: trabalho da equipe (revisões, correções, tempo, usuários).

Fontes:
  - feedback_llm (FEEDBACK_TABLE)    — histórico append-only (revisões submetidas)
  - revisoes_em_andamento (REVISOES) — estado transitório (workflow ativo)
  - resultados (RESULTS) + extracted_json — para rodar validações contábeis
  - documentos + resultados_final     — para tempo de revisão
"""
import json
from fastapi import APIRouter
from ..db import execute_sql
from ..config import RESULTS_TABLE, FEEDBACK_TABLE, REVISOES_TABLE, SOURCE_TABLE, RESULTS_FINAL_TABLE

router = APIRouter()

TOL = 0.01
FIELDS_PER_RECORD = 70

# ─── Validações contábeis (mesmas regras do PontosDeAtencao) ────────────────

def _n(data, path):
    parts = path.split('.')
    cur = data
    for p in parts:
        if not isinstance(cur, dict): return 0
        cur = cur.get(p)
    if cur is None: return 0
    try: return float(cur)
    except: return 0

def _diffPct(a, b):
    base = max(abs(a), abs(b), 1)
    return abs(a - b) / base * 100

def _run_validations(data):
    """Roda 23 validações e retorna {label: 'ok'|'warn'|'error'|'info'}."""
    results = {}
    def check(name, calc, expected, is_error=False):
        if expected == 0 and calc == 0:
            results[name] = 'ok'; return
        pct = _diffPct(calc, expected)
        if pct <= TOL: results[name] = 'ok'
        elif is_error: results[name] = 'error'
        else: results[name] = 'warn'

    at = _n(data,'ativo_total'); pt = _n(data,'passivo_total')
    check('Ativo=Passivo', at, pt, True)
    ac=_n(data,'ativo_circulante.total_ativo_circulante'); anc=_n(data,'ativo_nao_circulante.total_ativo_nao_circulante'); ap=_n(data,'ativo_permanente.total_ativo_permanente')
    check('AC+ANC+AP=AT', ac+anc+ap, at, True)
    pc=_n(data,'passivo_circulante.total_passivo_circulante'); pnc=_n(data,'passivo_nao_circulante.total_passivo_nao_circulante'); pl=_n(data,'patrimonio_liquido.total_patrimonio_liquido')
    check('PC+PNC+PL=PT', pc+pnc+pl, pt, True)
    sum_ac=sum(_n(data,f'ativo_circulante.{f}') for f in ['disponibilidades','titulos_a_receber','estoques','adiantamentos','impostos_a_recuperar','outros_ativos_circulantes','conta_corrente_socios_control_colig','outros_ativos_financeiros'])
    if ac > 0: check('AC interno', sum_ac, ac)
    else: results['AC interno'] = 'ok'
    sum_pc=sum(_n(data,f'passivo_circulante.{f}') for f in ['fornecedores','financiamentos_com_instituicoes_de_credito','salarios_contribuicoes','tributos','adiantamentos','conta_corrente_socios_coligadas_controladas','outros_passivos_circulante','provisoes','outros_passivos_financeiros'])
    if pc > 0: check('PC interno', sum_pc, pc)
    else: results['PC interno'] = 'ok'
    sum_pl=sum(_n(data,f'patrimonio_liquido.{f}') for f in ['capital_social','reserva_de_capital','reservas_de_lucro','reservas_de_reavaliacao','outras_reservas','lucros_ou_prejuizos_acumulados','acoes_em_tesouraria'])
    if pl > 0: check('PL interno', sum_pl, pl)
    else: results['PL interno'] = 'ok'
    sum_anc=sum(_n(data,f'ativo_nao_circulante.{f}') for f in ['titulos_a_receber','estoques','adiantamentos','impostos_a_recuperar','despesas_pagas_antecipadamente','conta_corrente_socios_control_colig','outros_realizavel_a_longo_prazo'])
    if anc > 0: check('ANC interno', sum_anc, anc)
    else: results['ANC interno'] = 'ok'
    sum_ap=sum(_n(data,f'ativo_permanente.{f}') for f in ['investimentos','imobilizado','intangivel_diferido'])
    if ap > 0: check('AP interno', sum_ap, ap)
    else: results['AP interno'] = 'ok'
    sum_pnc=sum(_n(data,f'passivo_nao_circulante.{f}') for f in ['fornecedores','financiamentos_com_instituicoes_de_credito','salarios_contribuicoes','tributos','adiantamentos','conta_corrente_socios_coligadas_controladas','outros_passivos_nao_circulantes','provisoes'])
    if pnc > 0: check('PNC interno', sum_pnc, pnc)
    else: results['PNC interno'] = 'ok'
    rol=_n(data,'dre.receita_operacional_liquida'); rob=_n(data,'dre.receita_operacional_bruta'); ded=_n(data,'dre.total_deducoes'); inc=_n(data,'dre.incentivos_a_exportacoes')
    if rol > 0: check('ROL=ROB-Ded', rob-ded+inc, rol)
    else: results['ROL=ROB-Ded'] = 'ok'
    lb=_n(data,'dre.lucro_bruto'); cmv=_n(data,'dre.custo_servicos_produtos_mercadorias_vendidas')
    if rol > 0 and lb != 0: check('LB=ROL-CMV', rol-cmv, lb)
    else: results['LB=ROL-CMV'] = 'ok'
    desp=_n(data,'dre.total_despesas_operacionais'); ebit=_n(data,'dre.lucro_operacional')
    if lb != 0: check('EBIT=LB-Desp', lb-desp, ebit)
    else: results['EBIT=LB-Desp'] = 'ok'
    lf=_n(data,'dre.lucro_financeiro'); ep=_n(data,'dre.resultado_de_equivalencia_patrimonial'); lair=_n(data,'dre.lucro_antes_imposto_de_renda')
    if lair != 0:
        rf = lf - ebit if lf != 0 else 0
        rno=_n(data,'dre.receita_nao_operacional'); dno=_n(data,'dre.despesa_nao_operacional')
        scm=_n(data,'dre.saldo_correcao_monetaria'); raa=_n(data,'dre.resultado_alienacao_ativos')
        check('LAIR=EBIT+RF+EP', ebit+rf+ep+rno-dno+scm+raa, lair)
    else: results['LAIR=EBIT+RF+EP'] = 'ok'
    ir=_n(data,'dre.provisao_imposto_de_renda')+_n(data,'dre.csll'); ll=_n(data,'dre.lucro_liquido')
    if lair != 0 and ll != 0: check('LL=LAIR-IR', lair-ir, ll)
    else: results['LL=LAIR-IR'] = 'ok'
    return results


def _run_validations_per_record(filter_doc: str | None = None):
    """Roda validações em todos os registros e retorna dados agregados."""
    where = "WHERE document_name = :name" if filter_doc else ""
    params = [{"name": "name", "value": filter_doc}] if filter_doc else None
    rows = execute_sql(
        f"""SELECT document_name, COALESCE(razao_social, document_name) AS razao_social,
                   tipo_entidade, periodo, extracted_json
            FROM {RESULTS_TABLE}
            {where}
            ORDER BY document_name, tipo_entidade, periodo""",
        params,
    )
    by_doc = {}     # doc → {ok, warn, error, records, issues}
    by_validation = {}  # label → {ok, warn, error}
    for row in rows:
        try:
            data = json.loads(row["extracted_json"]) if isinstance(row["extracted_json"], str) else row["extracted_json"]
        except (json.JSONDecodeError, TypeError):
            continue
        results = _run_validations(data or {})
        doc = row["document_name"]
        rs = row.get("razao_social") or doc
        if doc not in by_doc:
            by_doc[doc] = {"razao_social": rs, "ok": 0, "warn": 0, "error": 0, "records": 0}
        by_doc[doc]["records"] += 1
        for label, status in results.items():
            if status in ('ok', 'warn', 'error'):
                by_doc[doc][status] += 1
                if label not in by_validation:
                    by_validation[label] = {"ok_docs": set(), "warn_docs": set(), "error_docs": set()}
                if status == 'ok':    by_validation[label]["ok_docs"].add(doc)
                elif status == 'warn': by_validation[label]["warn_docs"].add(doc)
                else:                  by_validation[label]["error_docs"].add(doc)
    # Sets → counts
    by_validation_list = sorted(
        [
            {"label": label, "error_docs": len(v["error_docs"]), "warn_docs": len(v["warn_docs"]), "ok_docs": len(v["ok_docs"])}
            for label, v in by_validation.items()
        ],
        key=lambda x: (-x["error_docs"], -x["warn_docs"]),
    )
    by_doc_list = sorted(
        [{"document_name": doc, **info} for doc, info in by_doc.items()],
        key=lambda x: (-x["error"], -x["warn"], x["razao_social"]),
    )
    total_validations = sum(d["ok"] + d["warn"] + d["error"] for d in by_doc.values())
    total_ok          = sum(d["ok"]    for d in by_doc.values())
    total_warn        = sum(d["warn"]  for d in by_doc.values())
    total_error       = sum(d["error"] for d in by_doc.values())
    return {
        "total": total_validations,
        "ok": total_ok, "warn": total_warn, "error": total_error,
        "by_doc": by_doc_list,
        "by_validation": by_validation_list,
    }


# ─── Bloco 2 helpers ────────────────────────────────────────────────────────

def _bloco_acuracia(filter_doc: str | None = None):
    """Cobertura, taxa de confirmação, evolução por versão, top campos, tipos de erro."""
    where_doc = "AND document_name = :name" if filter_doc else ""
    params = [{"name": "name", "value": filter_doc}] if filter_doc else None

    # Total de campos extraídos (denominador da cobertura)
    if filter_doc:
        rec = execute_sql(
            f"SELECT COUNT(*) AS records FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": filter_doc}],
        )
    else:
        rec = execute_sql(f"SELECT COUNT(*) AS records FROM {RESULTS_TABLE}")
    total_records = int(rec[0]["records"]) if rec else 0
    total_campos = total_records * FIELDS_PER_RECORD

    # Cobertura: revisões submetidas (feedback_llm) — última versão por (doc, campo, te, per)
    counts = execute_sql(
        f"""SELECT
              COUNT(*) AS revisado,
              COUNT(*) FILTER (WHERE acao = 'confirmado') AS confirmados,
              COUNT(*) FILTER (WHERE acao = 'corrigido')  AS corrigidos
            FROM {FEEDBACK_TABLE}
            WHERE 1=1 {where_doc}""",
        params,
    )
    r = counts[0] if counts else {}
    revisado    = int(r.get("revisado") or 0)
    confirmados = int(r.get("confirmados") or 0)
    corrigidos  = int(r.get("corrigidos") or 0)
    cobertura_pct = round(revisado / total_campos * 100, 1) if total_campos > 0 else 0.0
    confirmacao_pct = round(confirmados / revisado * 100, 1) if revisado > 0 else None

    # Evolução por versão
    by_versao = execute_sql(
        f"""SELECT modelo_versao,
                   COUNT(*) AS amostra,
                   COUNT(*) FILTER (WHERE acao = 'confirmado') AS confirmados,
                   COUNT(*) FILTER (WHERE acao = 'corrigido')  AS corrigidos
            FROM {FEEDBACK_TABLE}
            WHERE modelo_versao IS NOT NULL {where_doc}
            GROUP BY modelo_versao
            ORDER BY MAX(submetido_em) ASC""",
        params,
    )
    # taxa
    for v in by_versao:
        n = int(v["amostra"] or 0)
        v["taxa_confirmacao"] = round(int(v["confirmados"] or 0) / n * 100, 1) if n > 0 else None

    # Top campos com maior taxa de correção (n >= 5)
    top_campos = execute_sql(
        f"""SELECT campo,
                   COUNT(*) AS revisoes,
                   COUNT(*) FILTER (WHERE acao = 'corrigido') AS correcoes
            FROM {FEEDBACK_TABLE}
            WHERE 1=1 {where_doc}
            GROUP BY campo
            HAVING COUNT(*) >= 5
            ORDER BY (COUNT(*) FILTER (WHERE acao = 'corrigido'))::numeric / NULLIF(COUNT(*),0) DESC,
                     COUNT(*) DESC
            LIMIT 10""",
        params,
    )
    for c in top_campos:
        c["taxa_correcao"] = round(int(c["correcoes"]) / int(c["revisoes"]) * 100, 1) if int(c["revisoes"]) > 0 else 0.0

    # Tipos de erro classificados
    tipos = execute_sql(
        f"""SELECT tipo_erro, COUNT(*) AS ocorrencias
            FROM {FEEDBACK_TABLE}
            WHERE acao = 'corrigido' AND tipo_erro IS NOT NULL {where_doc}
            GROUP BY tipo_erro
            ORDER BY ocorrencias DESC""",
        params,
    )
    nao_classif_rows = execute_sql(
        f"SELECT COUNT(*) AS cnt FROM {FEEDBACK_TABLE} WHERE acao = 'corrigido' AND tipo_erro IS NULL {where_doc}",
        params,
    )
    nao_classif = int(nao_classif_rows[0]["cnt"]) if nao_classif_rows else 0

    return {
        "total_campos_extraidos": total_campos,
        "total_records": total_records,
        "revisado": revisado,
        "confirmados": confirmados,
        "corrigidos": corrigidos,
        "cobertura_pct": cobertura_pct,
        "confirmacao_pct": confirmacao_pct,
        "by_versao": by_versao,
        "top_campos": top_campos,
        "tipos_erro": tipos,
        "tipos_erro_nao_classif": nao_classif,
    }


# ─── Bloco 3 helpers ────────────────────────────────────────────────────────

def _bloco_atividade(filter_doc: str | None = None):
    """Produtividade, tempo médio, atividade por revisor, correções recentes."""
    where_doc = "AND document_name = :name" if filter_doc else ""
    params = [{"name": "name", "value": filter_doc}] if filter_doc else None

    # Resumo
    if filter_doc:
        docs_revisados = 1 if execute_sql(
            f"SELECT 1 FROM {FEEDBACK_TABLE} WHERE document_name = :name LIMIT 1",
            [{"name": "name", "value": filter_doc}],
        ) else 0
    else:
        d_rows = execute_sql(f"SELECT COUNT(DISTINCT document_name) AS n FROM {FEEDBACK_TABLE}")
        docs_revisados = int(d_rows[0]["n"]) if d_rows else 0

    # Tempo médio: diferença entre finalizado_em e ingested_at
    tempo_rows = execute_sql(
        f"""SELECT
              AVG(EXTRACT(EPOCH FROM (rf.finalizado_em - d.ingested_at))/60) AS media_min,
              PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (rf.finalizado_em - d.ingested_at))/60
              ) AS mediana_min
            FROM {RESULTS_FINAL_TABLE} rf
            JOIN {SOURCE_TABLE} d ON d.document_name = rf.document_name
            WHERE rf.finalizado_em IS NOT NULL AND d.ingested_at IS NOT NULL
              {where_doc.replace('document_name', 'rf.document_name')}""",
        params,
    )
    tempo_media = tempo_rows[0].get("media_min") if tempo_rows else None
    tempo_mediana = tempo_rows[0].get("mediana_min") if tempo_rows else None

    # Por revisor
    by_user = execute_sql(
        f"""SELECT COALESCE(revisado_por, 'unknown') AS usuario,
                   COUNT(*) AS revisoes,
                   COUNT(*) FILTER (WHERE acao = 'confirmado') AS confirmacoes,
                   COUNT(*) FILTER (WHERE acao = 'corrigido')  AS correcoes,
                   (MAX(revisado_em))::text AS ultima_atividade
            FROM {FEEDBACK_TABLE}
            WHERE 1=1 {where_doc}
            GROUP BY 1
            ORDER BY revisoes DESC""",
        params,
    )

    # Recentes (com modelo_versao)
    recent = execute_sql(
        f"""SELECT document_name, campo,
                   (valor_llm)::text   AS valor_llm,
                   (valor_final)::text AS valor_final,
                   acao,
                   COALESCE(tipo_erro, '') AS tipo_erro,
                   COALESCE(tipo_erro_detalhe, '') AS comentario,
                   COALESCE(revisado_por, 'unknown') AS revisado_por,
                   (revisado_em)::text   AS revisado_em,
                   COALESCE(modelo_versao, '') AS modelo_versao
            FROM {FEEDBACK_TABLE}
            WHERE 1=1 {where_doc}
            ORDER BY submetido_em DESC
            LIMIT 30""",
        params,
    )
    return {
        "docs_revisados": docs_revisados,
        "tempo_medio_min":  round(float(tempo_media), 1)   if tempo_media   is not None else None,
        "tempo_mediana_min": round(float(tempo_mediana), 1) if tempo_mediana is not None else None,
        "by_user": by_user,
        "recent": recent,
    }


# ─── Endpoints ──────────────────────────────────────────────────────────────

@router.get("/metrics")
def get_metrics():
    """Métricas globais — 3 blocos."""
    total_docs_rows = execute_sql(f"SELECT COUNT(DISTINCT document_name) AS n FROM {RESULTS_TABLE}")
    total_docs = int(total_docs_rows[0]["n"]) if total_docs_rows else 0

    validacoes = _run_validations_per_record(None)
    acuracia   = _bloco_acuracia(None)
    atividade  = _bloco_atividade(None)

    return {
        "view": "global",
        "total_docs": total_docs,
        "validacoes": validacoes,
        "acuracia":   acuracia,
        "atividade":  atividade,
    }


@router.get("/metrics/{document_name}")
def get_document_metrics(document_name: str):
    """Métricas de um documento específico."""
    doc_info = execute_sql(
        f"SELECT MAX(razao_social) AS razao_social FROM {RESULTS_TABLE} WHERE document_name = :name",
        [{"name": "name", "value": document_name}],
    )
    razao_social = doc_info[0].get("razao_social") if doc_info else None

    validacoes = _run_validations_per_record(document_name)
    acuracia   = _bloco_acuracia(document_name)
    atividade  = _bloco_atividade(document_name)

    return {
        "view": "document",
        "document_name": document_name,
        "razao_social": razao_social,
        "validacoes": validacoes,
        "acuracia":   acuracia,
        "atividade":  atividade,
    }
