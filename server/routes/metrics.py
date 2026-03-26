from fastapi import APIRouter
from ..db import execute_sql
from ..config import RESULTS_TABLE, CORRECTIONS_TABLE

router = APIRouter()

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
