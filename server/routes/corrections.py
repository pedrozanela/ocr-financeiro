from fastapi import APIRouter, Request
from pydantic import BaseModel
from ..db import execute_sql, execute_update
from ..config import CORRECTIONS_TABLE

router = APIRouter()


class Correction(BaseModel):
    document_name: str
    tipo_entidade: str = ""
    periodo: str = ""
    campo: str
    valor_extraido: str
    valor_correto: str
    comentario: str = ""


def _current_user(request: Request) -> str:
    """Try to get the authenticated user from Databricks Apps proxy headers."""
    for header in ("X-Forwarded-User", "X-Forwarded-Email", "X-Databricks-User"):
        v = request.headers.get(header, "")
        if v:
            return v
    return ""


@router.get("/corrections/{document_name}")
def get_corrections(document_name: str):
    rows = execute_sql(
        f"""
        SELECT campo, COALESCE(tipo_entidade, '') AS tipo_entidade,
               COALESCE(periodo, '') AS periodo,
               valor_extraido, valor_correto, comentario,
               COALESCE(status, 'pendente') AS status,
               CAST(confirmado_em AS STRING) AS confirmado_em,
               COALESCE(confirmado_por, '') AS confirmado_por,
               CAST(criado_em AS STRING) AS criado_em
        FROM {CORRECTIONS_TABLE}
        WHERE document_name = :name
        ORDER BY criado_em DESC
        """,
        [{"name": "name", "value": document_name}],
    )
    # Key: campo__tipo_entidade__periodo  →  so frontend can look up by record
    return {f"{r['campo']}__{r['tipo_entidade']}__{r['periodo']}": r for r in rows}


@router.post("/corrections")
def save_correction(c: Correction):
    te = c.tipo_entidade or ""
    per = c.periodo or ""
    # Upsert: delete existing for same (doc, campo, te, per) then insert
    execute_update(
        f"""DELETE FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per""",
        [
            {"name": "name",  "value": c.document_name},
            {"name": "campo", "value": c.campo},
            {"name": "te",    "value": te},
            {"name": "per",   "value": per},
        ],
    )
    execute_update(
        f"""INSERT INTO {CORRECTIONS_TABLE}
            (document_name, tipo_entidade, periodo, campo, valor_extraido, valor_correto, comentario, status)
            VALUES (:name, :te, :per, :campo, :extraido, :correto, :comentario, 'pendente')""",
        [
            {"name": "name",       "value": c.document_name},
            {"name": "te",         "value": te},
            {"name": "per",        "value": per},
            {"name": "campo",      "value": c.campo},
            {"name": "extraido",   "value": c.valor_extraido},
            {"name": "correto",    "value": c.valor_correto},
            {"name": "comentario", "value": c.comentario},
        ],
    )
    return {"status": "ok"}


@router.post("/corrections/{document_name}/{campo}/confirm")
def confirm_correction(
    document_name: str,
    campo: str,
    request: Request,
    tipo_entidade: str = "",
    periodo: str = "",
):
    confirmed_por = _current_user(request)
    execute_update(
        f"""UPDATE {CORRECTIONS_TABLE}
            SET status = 'confirmado',
                confirmado_em = CURRENT_TIMESTAMP(),
                confirmado_por = :por
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per""",
        [
            {"name": "name",  "value": document_name},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": tipo_entidade},
            {"name": "per",   "value": periodo},
            {"name": "por",   "value": confirmed_por},
        ],
    )
    # Return the confirmation metadata so frontend can update state
    rows = execute_sql(
        f"""SELECT CAST(confirmado_em AS STRING) AS confirmado_em, COALESCE(confirmado_por, '') AS confirmado_por
            FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per
            LIMIT 1""",
        [
            {"name": "name",  "value": document_name},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": tipo_entidade},
            {"name": "per",   "value": periodo},
        ],
    )
    row = rows[0] if rows else {}
    return {
        "status": "ok",
        "confirmado_em":  row.get("confirmado_em", ""),
        "confirmado_por": row.get("confirmado_por", confirmed_por),
    }


@router.delete("/corrections/{document_name}/{campo}")
def delete_correction(
    document_name: str,
    campo: str,
    tipo_entidade: str = "",
    periodo: str = "",
):
    execute_update(
        f"""DELETE FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per""",
        [
            {"name": "name",  "value": document_name},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": tipo_entidade},
            {"name": "per",   "value": periodo},
        ],
    )
    return {"status": "ok"}
