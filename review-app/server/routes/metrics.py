from fastapi import APIRouter
from ..db import execute_sql
from ..config import RESULTS_TABLE, CORRECTIONS_TABLE

router = APIRouter()


@router.get("/metrics")
def get_metrics():
    totals = execute_sql(f"""
        SELECT
            (SELECT COUNT(*) FROM {RESULTS_TABLE}) AS total_docs,
            (SELECT COUNT(*) FROM {CORRECTIONS_TABLE}) AS total_corrections,
            (SELECT COUNT(DISTINCT document_name) FROM {CORRECTIONS_TABLE}) AS docs_with_corrections
    """)

    by_field = execute_sql(f"""
        SELECT campo, COUNT(*) AS total
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

    by_doc = execute_sql(f"""
        SELECT c.document_name, COUNT(*) AS corrections, r.razao_social
        FROM {CORRECTIONS_TABLE} c
        LEFT JOIN {RESULTS_TABLE} r ON c.document_name = r.document_name
        GROUP BY c.document_name, r.razao_social
        ORDER BY corrections DESC
        LIMIT 10
    """)

    t = totals[0] if totals else {}
    total_docs = int(t.get("total_docs") or 0)
    total_corrections = int(t.get("total_corrections") or 0)
    docs_with_corrections = int(t.get("docs_with_corrections") or 0)

    fields_reviewed = total_docs * 70
    accuracy = round((1 - total_corrections / fields_reviewed) * 100, 1) if fields_reviewed > 0 else None

    return {
        "total_docs": total_docs,
        "total_corrections": total_corrections,
        "docs_with_corrections": docs_with_corrections,
        "accuracy_pct": accuracy,
        "by_field": by_field,
        "by_type": by_type,
        "by_doc": by_doc,
    }
