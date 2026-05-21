import json
from typing import List
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
    # Quando True, já marca a correção como confirmada (usado pelo botão Salvar do novo UI
    # e pelo Confirmar tudo). Quando False (legado), entra como pendente.
    auto_confirm: bool = False


class BulkCorrections(BaseModel):
    document_name: str
    tipo_entidade: str = ""
    periodo: str = ""
    items: List[dict]  # [{campo, valor_extraido, valor_correto, comentario?}]


def _current_user(request: Request) -> str:
    """Get authenticated user from Databricks Apps proxy headers."""
    for header in ("X-Forwarded-Email", "X-Forwarded-User", "X-Databricks-User"):
        v = request.headers.get(header, "")
        if v:
            return v
    return "unknown"


def _raise_if_finalized(document_name: str, tipo_entidade: str = "", periodo: str = ""):
    """Bloqueia edição se o documento já foi submetido. Verifica por (te, per) se informado,
    senão checa o documento inteiro."""
    where = "document_name = :name AND status = 'finalizado'"
    params = [{"name": "name", "value": document_name}]
    if tipo_entidade or periodo:
        where += " AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per"
        params.append({"name": "te",  "value": tipo_entidade or ""})
        params.append({"name": "per", "value": periodo or ""})
    rows = execute_sql(
        f"SELECT 1 FROM {RESULTS_FINAL_TABLE} WHERE {where} LIMIT 1",
        params,
    )
    if rows:
        from fastapi import HTTPException
        raise HTTPException(409, "Documento já foi submetido — edições não são permitidas")


def _to_int_or_none(v):
    """Convert value to int, or None if empty/invalid."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _update_resultados_final(document_name: str, tipo_entidade: str, periodo: str, user: str):
    """Rebuild resultados_final for a specific record by applying all corrections on top of extracted_json."""
    # Get original extracted_json
    rows = execute_sql(
        f"""SELECT extracted_json, razao_social, cnpj, tipo_demonstrativo,
                   tipo_documento, numeromeses, moeda, escala_valores
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
            # Try to set as number, fallback to string. Preserve int when whole.
            try:
                f = float(valor)
                obj[parts[-1]] = int(f) if f.is_integer() and "." not in str(valor) else f
            except (ValueError, TypeError):
                obj[parts[-1]] = valor

    # resultados_final guarda apenas a estrutura financeira "limpa" — sem fontes
    # (racional do LLM) nem _postprocessed (metadados de pos-processamento).
    # Isso vira o JSON oficial para consumo downstream.
    clean = {k: v for k, v in data.items() if k not in ("fontes", "_postprocessed")}
    corrected_json = json.dumps(clean, ensure_ascii=False)

    # Preferir os valores corrigidos do JSON (incluem correções de enums via UI)
    # Identificação fica em data["identificacao"] no JSON
    ident = data.get("identificacao", {}) if isinstance(data, dict) else {}
    td_val = _to_int_or_none(ident.get("tipo_demonstrativo", row.get("tipo_demonstrativo")))
    tdoc_val = _to_int_or_none(ident.get("tipo_documento", row.get("tipo_documento")))
    nm_val = _to_int_or_none(ident.get("numeroMeses", row.get("numeromeses")))
    rs_val = ident.get("razao_social") or row.get("razao_social") or ""
    cnpj_val = ident.get("cnpj") or row.get("cnpj") or ""
    moeda_val = ident.get("moeda") or row.get("moeda") or ""
    escala_val = ident.get("escala_valores") or row.get("escala_valores") or ""

    execute_update(
        f"""INSERT INTO {RESULTS_FINAL_TABLE}
                (document_name, tipo_entidade, periodo, extracted_json, razao_social, cnpj,
                 tipo_demonstrativo, tipo_documento, numeroMeses, moeda, escala_valores,
                 atualizado_em, atualizado_por)
            VALUES (:name, :te, :per, :json, :rs, :cnpj, :td, :tdoc, :nm, :moeda, :escala,
                    NOW(), :user)
            ON CONFLICT (document_name, tipo_entidade, periodo) DO UPDATE SET
                extracted_json = EXCLUDED.extracted_json,
                razao_social = EXCLUDED.razao_social,
                cnpj = EXCLUDED.cnpj,
                tipo_demonstrativo = EXCLUDED.tipo_demonstrativo,
                tipo_documento = EXCLUDED.tipo_documento,
                numeroMeses = EXCLUDED.numeroMeses,
                moeda = EXCLUDED.moeda,
                escala_valores = EXCLUDED.escala_valores,
                atualizado_em = NOW(),
                atualizado_por = EXCLUDED.atualizado_por""",
        [
            {"name": "name",   "value": document_name},
            {"name": "te",     "value": tipo_entidade},
            {"name": "per",    "value": periodo},
            {"name": "json",   "value": corrected_json},
            {"name": "rs",     "value": str(rs_val)},
            {"name": "cnpj",   "value": str(cnpj_val)},
            {"name": "td",     "value": td_val},
            {"name": "tdoc",   "value": tdoc_val},
            {"name": "nm",     "value": nm_val},
            {"name": "moeda",  "value": str(moeda_val)},
            {"name": "escala", "value": str(escala_val)},
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


def _insert_correction(c_doc: str, te: str, per: str, campo: str,
                       valor_extraido: str, valor_correto: str, comentario: str,
                       user: str, auto_confirm: bool):
    """Upsert de uma correção. Quando auto_confirm=True, já entra como 'confirmado'."""
    execute_update(
        f"""DELETE FROM {CORRECTIONS_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade, '') = :te AND COALESCE(periodo, '') = :per""",
        [
            {"name": "name",  "value": c_doc},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": te},
            {"name": "per",   "value": per},
        ],
    )
    if auto_confirm:
        execute_update(
            f"""INSERT INTO {CORRECTIONS_TABLE}
                (document_name, tipo_entidade, periodo, campo, valor_extraido, valor_correto,
                 comentario, status, criado_em, confirmado_em, confirmado_por)
                VALUES (:name, :te, :per, :campo, :extraido, :correto, :comentario,
                        'confirmado', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), :por)""",
            [
                {"name": "name",       "value": c_doc},
                {"name": "te",         "value": te},
                {"name": "per",        "value": per},
                {"name": "campo",      "value": campo},
                {"name": "extraido",   "value": valor_extraido},
                {"name": "correto",    "value": valor_correto},
                {"name": "comentario", "value": comentario},
                {"name": "por",        "value": user},
            ],
        )
    else:
        execute_update(
            f"""INSERT INTO {CORRECTIONS_TABLE}
                (document_name, tipo_entidade, periodo, campo, valor_extraido, valor_correto,
                 comentario, status, criado_em)
                VALUES (:name, :te, :per, :campo, :extraido, :correto, :comentario,
                        'pendente', CURRENT_TIMESTAMP())""",
            [
                {"name": "name",       "value": c_doc},
                {"name": "te",         "value": te},
                {"name": "per",        "value": per},
                {"name": "campo",      "value": campo},
                {"name": "extraido",   "value": valor_extraido},
                {"name": "correto",    "value": valor_correto},
                {"name": "comentario", "value": comentario},
            ],
        )


@router.post("/corrections")
def save_correction(c: Correction, request: Request):
    te = c.tipo_entidade or ""
    per = c.periodo or ""
    user = _current_user(request)
    _raise_if_finalized(c.document_name, te, per)

    _insert_correction(
        c.document_name, te, per, c.campo,
        c.valor_extraido, c.valor_correto, c.comentario,
        user, c.auto_confirm,
    )

    # NOTA: resultados_final NAO e atualizado aqui. Consolidacao acontece
    # apenas no POST /api/finalize/{doc} (botao Submeter).
    return {"status": "ok", "user": user, "auto_confirm": c.auto_confirm}


@router.post("/corrections/bulk")
def save_bulk_corrections(payload: BulkCorrections, request: Request):
    """Cria/atualiza várias correções de uma vez, todas marcadas como 'confirmado'.
    Usado pelo botão 'Confirmar tudo' que marca campos LLM como Revisado sem alterar valor."""
    te = payload.tipo_entidade or ""
    per = payload.periodo or ""
    user = _current_user(request)
    _raise_if_finalized(payload.document_name, te, per)

    saved = 0
    for item in payload.items:
        campo = item.get("campo") or ""
        if not campo:
            continue
        _insert_correction(
            payload.document_name, te, per, campo,
            str(item.get("valor_extraido", "") or ""),
            str(item.get("valor_correto", "") or ""),
            str(item.get("comentario", "") or ""),
            user, True,  # bulk = sempre auto_confirm
        )
        saved += 1

    # NOTA: resultados_final NAO e atualizado aqui — apenas no Submeter.
    return {"status": "ok", "user": user, "count": saved}


@router.post("/corrections/{document_name}/{campo}/confirm")
def confirm_correction(
    document_name: str,
    campo: str,
    request: Request,
    tipo_entidade: str = "",
    periodo: str = "",
):
    user = _current_user(request)
    _raise_if_finalized(document_name, tipo_entidade, periodo)
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
    _ = request  # mantido na assinatura (FastAPI) — sem uso de auditoria aqui
    _raise_if_finalized(document_name, tipo_entidade, periodo)
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
    # NOTA: resultados_final NAO e atualizado aqui — apenas no Submeter.
    return {"status": "ok"}
