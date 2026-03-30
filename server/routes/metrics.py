import json
from fastapi import APIRouter
from ..db import execute_sql
from ..config import RESULTS_TABLE, CORRECTIONS_TABLE

router = APIRouter()

TOL = 0.01

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
    ok, warn, error = 0, 0, 0
    issues = []
    def check(name, calc, expected, is_error=False):
        nonlocal ok, warn, error
        if expected == 0 and calc == 0:
            ok += 1; return
        pct = _diffPct(calc, expected)
        if pct <= TOL:
            ok += 1
        elif is_error:
            error += 1; issues.append(name)
        else:
            warn += 1; issues.append(name)

    at = _n(data,'ativo_total'); pt = _n(data,'passivo_total')
    check('Ativo=Passivo', at, pt, True)
    ac=_n(data,'ativo_circulante.total_ativo_circulante'); anc=_n(data,'ativo_nao_circulante.total_ativo_nao_circulante'); ap=_n(data,'ativo_permanente.total_ativo_permanente')
    check('AC+ANC+AP=AT', ac+anc+ap, at, True)
    pc=_n(data,'passivo_circulante.total_passivo_circulante'); pnc=_n(data,'passivo_nao_circulante.total_passivo_nao_circulante'); pl=_n(data,'patrimonio_liquido.total_patrimonio_liquido')
    check('PC+PNC+PL=PT', pc+pnc+pl, pt, True)
    # Consistências internas
    sum_ac=sum(_n(data,f'ativo_circulante.{f}') for f in ['disponibilidades','titulos_a_receber','estoques','adiantamentos','impostos_a_recuperar','outros_ativos_circulantes','conta_corrente_socios_control_colig','outros_ativos_financeiros'])
    if ac > 0: check('AC interno', sum_ac, ac)
    else: ok += 1
    sum_pc=sum(_n(data,f'passivo_circulante.{f}') for f in ['fornecedores','financiamentos_com_instituicoes_de_credito','salarios_contribuicoes','tributos','adiantamentos','conta_corrente_socios_coligadas_controladas','outros_passivos_circulante','provisoes','outros_passivos_financeiros'])
    if pc > 0: check('PC interno', sum_pc, pc)
    else: ok += 1
    sum_pl=sum(_n(data,f'patrimonio_liquido.{f}') for f in ['capital_social','reserva_de_capital','reservas_de_lucro','reservas_de_reavaliacao','outras_reservas','lucros_ou_prejuizos_acumulados','acoes_em_tesouraria'])
    if pl > 0: check('PL interno', sum_pl, pl)
    else: ok += 1
    sum_anc=sum(_n(data,f'ativo_nao_circulante.{f}') for f in ['titulos_a_receber','estoques','adiantamentos','impostos_a_recuperar','despesas_pagas_antecipadamente','conta_corrente_socios_control_colig','outros_realizavel_a_longo_prazo'])
    if anc > 0: check('ANC interno', sum_anc, anc)
    else: ok += 1
    sum_ap=sum(_n(data,f'ativo_permanente.{f}') for f in ['investimentos','imobilizado','intangivel_diferido'])
    if ap > 0: check('AP interno', sum_ap, ap)
    else: ok += 1
    sum_pnc=sum(_n(data,f'passivo_nao_circulante.{f}') for f in ['fornecedores','financiamentos_com_instituicoes_de_credito','salarios_contribuicoes','tributos','adiantamentos','conta_corrente_socios_coligadas_controladas','outros_passivos_nao_circulantes','provisoes'])
    if pnc > 0: check('PNC interno', sum_pnc, pnc)
    else: ok += 1
    # DRE
    rol=_n(data,'dre.receita_operacional_liquida'); rob=_n(data,'dre.receita_operacional_bruta'); ded=_n(data,'dre.total_deducoes'); inc=_n(data,'dre.incentivos_a_exportacoes')
    if rol > 0: check('ROL=ROB-Ded', rob-ded+inc, rol)
    else: ok += 1
    lb=_n(data,'dre.lucro_bruto'); cmv=_n(data,'dre.custo_servicos_produtos_mercadorias_vendidas')
    if rol > 0 and lb != 0: check('LB=ROL-CMV', rol-cmv, lb)
    else: ok += 1
    desp=_n(data,'dre.total_despesas_operacionais'); ebit=_n(data,'dre.lucro_operacional')
    if lb != 0: check('EBIT=LB-Desp', lb-desp, ebit)
    else: ok += 1
    lf=_n(data,'dre.lucro_financeiro'); ep=_n(data,'dre.resultado_de_equivalencia_patrimonial'); lair=_n(data,'dre.lucro_antes_imposto_de_renda')
    if lair != 0:
        rf = lf - ebit if lf != 0 else 0
        rno=_n(data,'dre.receita_nao_operacional'); dno=_n(data,'dre.despesa_nao_operacional')
        scm=_n(data,'dre.saldo_correcao_monetaria'); raa=_n(data,'dre.resultado_alienacao_ativos')
        check('LAIR=EBIT+RF+EP', ebit+rf+ep+rno-dno+scm+raa, lair)
    else: ok += 1
    ir=_n(data,'dre.provisao_imposto_de_renda')+_n(data,'dre.csll'); ll=_n(data,'dre.lucro_liquido')
    if lair != 0 and ll != 0: check('LL=LAIR-IR', lair-ir, ll)
    else: ok += 1
    total = ok + warn + error
    return {"ok": ok, "warn": warn, "error": error, "total": total, "pct_ok": round(ok/total*100, 1) if total > 0 else 100, "issues": issues}

FIELDS_PER_RECORD = 70  # Estimated fields per tipo_entidade + periodo


@router.get("/metrics")
def get_metrics():
    """Global metrics across all documents."""
    totals = execute_sql(f"""
        SELECT
            (SELECT COUNT(DISTINCT document_name) FROM {RESULTS_TABLE}) AS total_docs,
            (SELECT COUNT(*) FROM {CORRECTIONS_TABLE}) AS total_corrections,
            (SELECT COUNT(*) FROM {CORRECTIONS_TABLE} WHERE COALESCE(status, 'pendente') = 'pendente') AS pending_corrections,
            (SELECT COUNT(*) FROM {CORRECTIONS_TABLE} WHERE status = 'confirmado') AS confirmed_corrections,
            (SELECT COUNT(DISTINCT document_name) FROM {CORRECTIONS_TABLE}) AS docs_with_corrections
    """)

    by_field = execute_sql(f"""
        SELECT campo,
               SUM(CASE WHEN COALESCE(status, 'pendente') = 'pendente' THEN 1 ELSE 0 END) AS pendente,
               SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmado,
               COUNT(*) AS total
        FROM {CORRECTIONS_TABLE}
        GROUP BY campo
        ORDER BY total DESC
        LIMIT 15
    """)

    by_type = execute_sql(f"""
        SELECT
            CASE WHEN comentario IS NULL OR comentario = '' THEN 'Sem descrição' ELSE comentario END AS tipo,
            COUNT(*) AS total
        FROM {CORRECTIONS_TABLE}
        GROUP BY 1
        ORDER BY total DESC
    """)

    # Corrections by user (who created + who confirmed)
    by_user = execute_sql(f"""
        SELECT
            COALESCE(criado_por, 'unknown') AS usuario,
            COUNT(*) AS total_correcoes,
            SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmadas,
            CAST(MAX(criado_em) AS STRING) AS ultima_correcao
        FROM {CORRECTIONS_TABLE}
        GROUP BY 1
        ORDER BY total_correcoes DESC
    """)

    # Recent corrections with user detail
    recent = execute_sql(f"""
        SELECT document_name, campo, valor_extraido, valor_correto, comentario,
               COALESCE(criado_por, 'unknown') AS criado_por,
               CAST(criado_em AS STRING) AS criado_em,
               COALESCE(confirmado_por, '') AS confirmado_por,
               CAST(confirmado_em AS STRING) AS confirmado_em,
               COALESCE(status, 'pendente') AS status
        FROM {CORRECTIONS_TABLE}
        ORDER BY criado_em DESC
        LIMIT 20
    """)

    # All documents with their corrections count and accuracy
    by_doc = execute_sql(f"""
        WITH doc_records AS (
            SELECT document_name, razao_social,
                   COUNT(*) AS total_records
            FROM {RESULTS_TABLE}
            GROUP BY document_name, razao_social
        ),
        doc_corrections AS (
            SELECT document_name,
                   SUM(CASE WHEN COALESCE(status, 'pendente') = 'pendente' THEN 1 ELSE 0 END) AS pendente,
                   SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmado,
                   COUNT(*) AS total
            FROM {CORRECTIONS_TABLE}
            GROUP BY document_name
        )
        SELECT dr.document_name,
               dr.razao_social,
               COALESCE(dc.pendente, 0) AS pendente,
               COALESCE(dc.confirmado, 0) AS confirmado,
               COALESCE(dc.total, 0) AS total,
               dr.total_records,
               ROUND((1 - COALESCE(dc.confirmado, 0) / (dr.total_records * {FIELDS_PER_RECORD})) * 100, 1) AS accuracy_pct
        FROM doc_records dr
        LEFT JOIN doc_corrections dc ON dr.document_name = dc.document_name
        ORDER BY dc.total DESC NULLS LAST, dr.razao_social
        LIMIT 50
    """)

    t = totals[0] if totals else {}
    total_docs = int(t.get("total_docs") or 0)
    total_corrections = int(t.get("total_corrections") or 0)
    pending_corrections = int(t.get("pending_corrections") or 0)
    confirmed_corrections = int(t.get("confirmed_corrections") or 0)
    docs_with_corrections = int(t.get("docs_with_corrections") or 0)

    # Count total records (tipo_entidade × periodo) across all documents
    total_records_result = execute_sql(f"SELECT COUNT(*) AS cnt FROM {RESULTS_TABLE}")
    total_records = int(total_records_result[0].get("cnt") or 0) if total_records_result else 0
    fields_reviewed = total_records * FIELDS_PER_RECORD

    accuracy = round((1 - confirmed_corrections / fields_reviewed) * 100, 1) if fields_reviewed > 0 else None

    return {
        "total_docs": total_docs,
        "total_corrections": total_corrections,
        "pending_corrections": pending_corrections,
        "confirmed_corrections": confirmed_corrections,
        "docs_with_corrections": docs_with_corrections,
        "accuracy_pct": accuracy,
        "by_field": by_field,
        "by_type": by_type,
        "by_doc": by_doc,
        "by_user": by_user,
        "recent": recent,
    }


@router.get("/metrics/{document_name}")
def get_document_metrics(document_name: str):
    """Per-document metrics breakdown by tipo_entidade and periodo."""
    doc_info = execute_sql(
        f"""SELECT razao_social FROM {RESULTS_TABLE} WHERE document_name = :name LIMIT 1""",
        [{"name": "name", "value": document_name}]
    )

    totals = execute_sql(
        f"""
        SELECT
            COUNT(*) AS total_corrections,
            SUM(CASE WHEN COALESCE(status, 'pendente') = 'pendente' THEN 1 ELSE 0 END) AS pending_corrections,
            SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmed_corrections,
            COUNT(DISTINCT COALESCE(tipo_entidade, '') || '__' || COALESCE(periodo, '')) AS records_with_corrections
        FROM {CORRECTIONS_TABLE}
        WHERE document_name = :name
        """,
        [{"name": "name", "value": document_name}]
    )

    # All records for this document (with or without corrections)
    by_record = execute_sql(
        f"""
        WITH doc_records AS (
            SELECT COALESCE(tipo_entidade, 'INDIVIDUAL') AS tipo_entidade,
                   COALESCE(periodo, '') AS periodo
            FROM {RESULTS_TABLE}
            WHERE document_name = :name
        ),
        record_corrections AS (
            SELECT COALESCE(tipo_entidade, '') AS tipo_entidade,
                   COALESCE(periodo, '') AS periodo,
                   SUM(CASE WHEN COALESCE(status, 'pendente') = 'pendente' THEN 1 ELSE 0 END) AS pendente,
                   SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmado,
                   COUNT(*) AS total
            FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name
            GROUP BY 1, 2
        )
        SELECT dr.tipo_entidade,
               dr.periodo,
               COALESCE(rc.pendente, 0) AS pendente,
               COALESCE(rc.confirmado, 0) AS confirmado,
               COALESCE(rc.total, 0) AS total,
               ROUND((1 - COALESCE(rc.confirmado, 0) / {FIELDS_PER_RECORD}) * 100, 1) AS accuracy_pct
        FROM doc_records dr
        LEFT JOIN record_corrections rc
            ON dr.tipo_entidade = rc.tipo_entidade
            AND dr.periodo = rc.periodo
        ORDER BY rc.total DESC NULLS LAST, dr.tipo_entidade, dr.periodo
        """,
        [{"name": "name", "value": document_name}]
    )

    by_field = execute_sql(
        f"""
        SELECT campo,
               SUM(CASE WHEN COALESCE(status, 'pendente') = 'pendente' THEN 1 ELSE 0 END) AS pendente,
               SUM(CASE WHEN status = 'confirmado' THEN 1 ELSE 0 END) AS confirmado,
               COUNT(*) AS total
        FROM {CORRECTIONS_TABLE}
        WHERE document_name = :name
        GROUP BY campo
        ORDER BY total DESC
        LIMIT 15
        """,
        [{"name": "name", "value": document_name}]
    )

    by_type = execute_sql(
        f"""
        SELECT
            CASE WHEN comentario IS NULL OR comentario = '' THEN 'Sem descrição' ELSE comentario END AS tipo,
            COUNT(*) AS total
        FROM {CORRECTIONS_TABLE}
        WHERE document_name = :name
        GROUP BY 1
        ORDER BY total DESC
        """,
        [{"name": "name", "value": document_name}]
    )

    t = totals[0] if totals else {}
    total_corrections = int(t.get("total_corrections") or 0)
    pending_corrections = int(t.get("pending_corrections") or 0)
    confirmed_corrections = int(t.get("confirmed_corrections") or 0)
    records_with_corrections = int(t.get("records_with_corrections") or 0)

    # Overall document accuracy
    total_records = len(by_record)
    total_fields = total_records * FIELDS_PER_RECORD
    doc_accuracy = round((1 - confirmed_corrections / total_fields) * 100, 1) if total_fields > 0 else None

    razao_social = doc_info[0].get("razao_social") if doc_info else None

    return {
        "document_name": document_name,
        "razao_social": razao_social,
        "total_corrections": total_corrections,
        "pending_corrections": pending_corrections,
        "confirmed_corrections": confirmed_corrections,
        "records_with_corrections": records_with_corrections,
        "total_records": total_records,
        "accuracy_pct": doc_accuracy,
        "by_record": by_record,
        "by_field": by_field,
        "by_type": by_type,
    }


@router.get("/metrics/validations")
def get_validations_summary():
    """Run all validation checks across all documents and return summary."""
    rows = execute_sql(f"""
        SELECT document_name, tipo_entidade, periodo, extracted_json, razao_social
        FROM {RESULTS_TABLE}
        ORDER BY document_name, tipo_entidade, periodo
    """)

    by_doc = {}
    global_ok, global_warn, global_error, global_total = 0, 0, 0, 0

    for row in rows:
        try:
            data = json.loads(row["extracted_json"]) if isinstance(row["extracted_json"], str) else row["extracted_json"]
        except (json.JSONDecodeError, TypeError):
            continue

        v = _run_validations(data)
        global_ok += v["ok"]
        global_warn += v["warn"]
        global_error += v["error"]
        global_total += v["total"]

        doc_name = row["document_name"]
        rs = row.get("razao_social") or doc_name
        if doc_name not in by_doc:
            by_doc[doc_name] = {"razao_social": rs, "ok": 0, "warn": 0, "error": 0, "total": 0, "records": 0, "issues": []}
        by_doc[doc_name]["ok"] += v["ok"]
        by_doc[doc_name]["warn"] += v["warn"]
        by_doc[doc_name]["error"] += v["error"]
        by_doc[doc_name]["total"] += v["total"]
        by_doc[doc_name]["records"] += 1
        by_doc[doc_name]["issues"].extend(v["issues"])

    docs_list = []
    for doc_name, d in sorted(by_doc.items(), key=lambda x: x[1]["error"] + x[1]["warn"], reverse=True):
        pct = round(d["ok"] / d["total"] * 100, 1) if d["total"] > 0 else 100
        docs_list.append({
            "document_name": doc_name,
            "razao_social": d["razao_social"],
            "records": d["records"],
            "ok": d["ok"],
            "warn": d["warn"],
            "error": d["error"],
            "total": d["total"],
            "pct_ok": pct,
            "issues": list(set(d["issues"])),
        })

    global_pct = round(global_ok / global_total * 100, 1) if global_total > 0 else 100

    return {
        "global": {
            "ok": global_ok,
            "warn": global_warn,
            "error": global_error,
            "total": global_total,
            "pct_ok": global_pct,
            "total_docs": len(by_doc),
            "docs_clean": sum(1 for d in by_doc.values() if d["error"] == 0 and d["warn"] == 0),
        },
        "by_doc": docs_list,
    }
