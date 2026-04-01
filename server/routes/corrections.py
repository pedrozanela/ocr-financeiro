import json
from fastapi import APIRouter, Request
from pydantic import BaseModel
from ..db import execute_sql, execute_update
from ..config import CORRECTIONS_TABLE, RESULTS_TABLE, RESULTS_FINAL_TABLE

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
    """Get authenticated user from Databricks Apps proxy headers."""
    for header in ("X-Forwarded-Email", "X-Forwarded-User", "X-Databricks-User"):
        v = request.headers.get(header, "")
        if v:
            return v
    return "unknown"


def _update_resultados_final(document_name: str, tipo_entidade: str, periodo: str, user: str):
    """Rebuild resultados_final for a specific record by applying all corrections on top of extracted_json."""
    # Get original extracted_json
    rows = execute_sql(
        f"""SELECT extracted_json, razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores
            FROM {RESULTS_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per
            LIMIT 1""",
        [
            {"name": "name", "value": document_name},
            {"name": "te",   "value": tipo_entidade},
            {"name": "per",  "value": periodo},
        ],
    )
    if not rows:
        return
    row = rows[0]
    try:
        data = json.loads(row["extracted_json"])
    except (json.JSONDecodeError, TypeError):
        return

    # Get all corrections for this record
    corrections = execute_sql(
        f"""SELECT campo, valor_correto
            FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per""",
        [
            {"name": "name", "value": document_name},
            {"name": "te",   "value": tipo_entidade},
            {"name": "per",  "value": periodo},
        ],
    )

    # Apply corrections to the JSON
    for c in corrections:
        campo = c["campo"]
        valor = c["valor_correto"]
        # Navigate nested path (e.g., "dre.lucro_liquido")
        parts = campo.split(".")
        obj = data
        for p in parts[:-1]:
            if isinstance(obj, dict) and p in obj:
                obj = obj[p]
            else:
                obj = None
                break
        if obj is not None and isinstance(obj, dict):
            # Try to set as number, fallback to string
            try:
                obj[parts[-1]] = float(valor)
            except (ValueError, TypeError):
                obj[parts[-1]] = valor

    corrected_json = json.dumps(data, ensure_ascii=False)
    esc_json = corrected_json.replace("'", "''")
    esc_name = document_name.replace("'", "''")
    esc_rs = str(row.get("razao_social") or "").replace("'", "''")
    esc_cnpj = str(row.get("cnpj") or "").replace("'", "''")
    esc_td = str(row.get("tipo_demonstrativo") or "").replace("'", "''")
    esc_moeda = str(row.get("moeda") or "").replace("'", "''")
    esc_escala = str(row.get("escala_valores") or "").replace("'", "''")
    esc_user = user.replace("'", "''")

    execute_update(
        f"""MERGE INTO {RESULTS_FINAL_TABLE} AS t
            USING (SELECT :name AS document_name, :te AS tipo_entidade, :per AS periodo) AS s
              ON t.document_name = s.document_name
              AND COALESCE(t.tipo_entidade, '') = s.tipo_entidade
              AND COALESCE(t.periodo, '') = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json = :json,
                razao_social = :rs,
                cnpj = :cnpj,
                tipo_demonstrativo = :td,
                moeda = :moeda,
                escala_valores = :escala,
                atualizado_em = CURRENT_TIMESTAMP(),
                atualizado_por = :user
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, razao_social, cnpj,
                 tipo_demonstrativo, moeda, escala_valores, atualizado_em, atualizado_por)
            VALUES (:name, :te, :per, :json, :rs, :cnpj, :td, :moeda, :escala,
                    CURRENT_TIMESTAMP(), :user)""",
        [
            {"name": "name",   "value": document_name},
            {"name": "te",     "value": tipo_entidade},
            {"name": "per",    "value": periodo},
            {"name": "json",   "value": corrected_json},
            {"name": "rs",     "value": str(row.get("razao_social") or "")},
            {"name": "cnpj",   "value": str(row.get("cnpj") or "")},
            {"name": "td",     "value": str(row.get("tipo_demonstrativo") or "")},
            {"name": "moeda",  "value": str(row.get("moeda") or "")},
            {"name": "escala", "value": str(row.get("escala_valores") or "")},
            {"name": "user",   "value": user},
        ],
    )


@router.get("/me")
def get_me(request: Request):
    return {"email": _current_user(request)}


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
    return {f"{r['campo']}__{r['tipo_entidade']}__{r['periodo']}": r for r in rows}


@router.post("/corrections")
def save_correction(c: Correction, request: Request):
    te = c.tipo_entidade or ""
    per = c.periodo or ""
    user = _current_user(request)

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
            (document_name, tipo_entidade, periodo, campo, valor_extraido, valor_correto,
             comentario, status, criado_por)
            VALUES (:name, :te, :per, :campo, :extraido, :correto, :comentario, 'pendente', :user)""",
        [
            {"name": "name",       "value": c.document_name},
            {"name": "te",         "value": te},
            {"name": "per",        "value": per},
            {"name": "campo",      "value": c.campo},
            {"name": "extraido",   "value": c.valor_extraido},
            {"name": "correto",    "value": c.valor_correto},
            {"name": "comentario", "value": c.comentario},
            {"name": "user",       "value": user},
        ],
    )

    # Update resultados_final with correction applied
    _update_resultados_final(c.document_name, te, per, user)

    return {"status": "ok", "user": user}


@router.post("/corrections/{document_name}/{campo}/confirm")
def confirm_correction(
    document_name: str,
    campo: str,
    request: Request,
    tipo_entidade: str = "",
    periodo: str = "",
):
    user = _current_user(request)
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
            {"name": "por",   "value": user},
        ],
    )
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
        "confirmado_por": row.get("confirmado_por", user),
    }


@router.delete("/corrections/{document_name}/{campo}")
def delete_correction(
    document_name: str,
    campo: str,
    request: Request,
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
    # Rebuild resultados_final without this correction
    user = _current_user(request)
    _update_resultados_final(document_name, tipo_entidade or "", periodo or "", user)
    return {"status": "ok"}
