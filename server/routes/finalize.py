"""Finalização de documentos — submissão definitiva, irreversível."""
from fastapi import APIRouter, HTTPException, Request
from ..db import execute_sql, execute_update
from ..config import RESULTS_FINAL_TABLE
from .corrections import _current_user, _update_resultados_final

router = APIRouter()


def _is_finalized(document_name: str, tipo_entidade: str = "", periodo: str = "") -> bool:
    """Retorna True se QUALQUER registro do documento (filtrado por te/per se informado) está finalizado."""
    where = "document_name = :name"
    params = [{"name": "name", "value": document_name}]
    if tipo_entidade or periodo:
        where += " AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per"
        params.append({"name": "te",  "value": tipo_entidade or ""})
        params.append({"name": "per", "value": periodo or ""})
    rows = execute_sql(
        f"SELECT 1 FROM {RESULTS_FINAL_TABLE} WHERE {where} AND status = 'finalizado' LIMIT 1",
        params,
    )
    return bool(rows)


@router.get("/finalize/{document_name}")
def get_finalize_status(document_name: str):
    """Retorna status por (tipo_entidade, periodo) e flag agregada."""
    rows = execute_sql(
        f"""SELECT COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo,
                   COALESCE(status, 'em_revisao') AS status,
                   CAST(finalizado_em AS STRING) AS finalizado_em,
                   COALESCE(finalizado_por, '') AS finalizado_por
            FROM {RESULTS_FINAL_TABLE}
            WHERE document_name = :name""",
        [{"name": "name", "value": document_name}],
    )
    overall = "finalizado" if rows and all(r["status"] == "finalizado" for r in rows) else "em_revisao"
    return {"document_name": document_name, "status": overall, "records": rows}


@router.post("/finalize/{document_name}")
def finalize_document(document_name: str, request: Request):
    """Finaliza TODOS os registros do documento (todos os pares tipo_entidade/periodo).
    Irreversível: nenhum admin pode reabrir via UI."""
    user = _current_user(request)

    # Pegar todos os pares (tipo_entidade, periodo) já em resultados (fonte da verdade)
    from ..config import RESULTS_TABLE
    pairs = execute_sql(
        f"""SELECT COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo
            FROM {RESULTS_TABLE}
            WHERE document_name = :name""",
        [{"name": "name", "value": document_name}],
    )
    if not pairs:
        raise HTTPException(404, "Documento não encontrado em resultados")

    # Bloquear se algum registro já está finalizado
    if _is_finalized(document_name):
        raise HTTPException(409, "Documento já está finalizado")

    # Garantir que resultados_final existe para cada par (consolida correções pendentes)
    for p in pairs:
        _update_resultados_final(document_name, p["tipo_entidade"], p["periodo"], user)

    # Marcar todos os registros como finalizado
    execute_update(
        f"""UPDATE {RESULTS_FINAL_TABLE}
            SET status = 'finalizado',
                finalizado_em = NOW(),
                finalizado_por = :por
            WHERE document_name = :name""",
        [
            {"name": "name", "value": document_name},
            {"name": "por",  "value": user},
        ],
    )

    return {
        "document_name": document_name,
        "status": "finalizado",
        "finalizado_por": user,
    }
