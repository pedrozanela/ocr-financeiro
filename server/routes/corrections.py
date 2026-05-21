"""Workflow de revisao em andamento.

Tabela: revisoes_em_andamento (uma linha por campo revisado, status: llm/corrigido/confirmado).
URLs mantem /api/corrections/* para compat com o frontend, mas o backend usa o novo schema.
"""
import json
from typing import List, Optional
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from ..db import execute_sql, execute_update
from ..config import REVISOES_TABLE, RESULTS_FINAL_TABLE

router = APIRouter()


class Correction(BaseModel):
    document_name: str
    tipo_entidade: str = ""
    periodo: str = ""
    campo: str
    valor_extraido: str
    valor_correto: str
    comentario: str = ""  # vai para tipo_erro_detalhe
    tipo_erro: Optional[str] = None
    auto_confirm: bool = False  # legado/compat: sem efeito real, status decidido pelo valor


class BulkCorrections(BaseModel):
    document_name: str
    tipo_entidade: str = ""
    periodo: str = ""
    items: List[dict]


def _current_user(request: Request) -> str:
    for header in ("X-Forwarded-Email", "X-Forwarded-User", "X-Databricks-User"):
        v = request.headers.get(header, "")
        if v:
            return v
    return "unknown"


def _raise_if_finalized(document_name: str, tipo_entidade: str = "", periodo: str = ""):
    where = "document_name = :name AND status = 'finalizado'"
    params = [{"name": "name", "value": document_name}]
    if tipo_entidade or periodo:
        where += " AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per"
        params.append({"name": "te",  "value": tipo_entidade or ""})
        params.append({"name": "per", "value": periodo or ""})
    rows = execute_sql(f"SELECT 1 FROM {RESULTS_FINAL_TABLE} WHERE {where} LIMIT 1", params)
    if rows:
        raise HTTPException(409, "Documento já foi submetido — edições não são permitidas")


def _num(s):
    """Converte string para float, retorna None se não numerica."""
    if s is None or s == "":
        return None
    try:
        return float(str(s).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _upsert_revisao(
    document_name: str, te: str, per: str, campo: str,
    valor_extraido: str, valor_correto: str,
    user: str, tipo_erro: Optional[str], tipo_erro_detalhe: str,
):
    """Upsert em revisoes_em_andamento. status derivado da comparacao de valores:
       - valores iguais (ou correto == extraido) → 'confirmado'
       - valores diferentes → 'corrigido'
    """
    v_extraido = _num(valor_extraido)
    v_correto = _num(valor_correto)
    # Status: corrigido se mudou de fato, senao confirmado (analista validou sem alterar)
    if v_correto is not None and v_extraido is not None and abs(v_correto - v_extraido) > 1e-6:
        status = "corrigido"
    elif v_correto != v_extraido:  # nulls / strings diferentes
        status = "corrigido"
    else:
        status = "confirmado"

    execute_update(
        f"""INSERT INTO {REVISOES_TABLE}
              (document_name, campo, tipo_entidade, periodo,
               valor_extraido, valor_corrente, status,
               tipo_erro, tipo_erro_detalhe, revisado_por, revisado_em, criado_em)
            VALUES (:name, :campo, :te, :per,
                    :ve, :vc, :status,
                    :te_err, :te_det, :por, NOW(), NOW())
            ON CONFLICT (document_name, campo, tipo_entidade, periodo) DO UPDATE SET
              valor_extraido    = EXCLUDED.valor_extraido,
              valor_corrente    = EXCLUDED.valor_corrente,
              status            = EXCLUDED.status,
              tipo_erro         = EXCLUDED.tipo_erro,
              tipo_erro_detalhe = EXCLUDED.tipo_erro_detalhe,
              revisado_por      = EXCLUDED.revisado_por,
              revisado_em       = NOW()""",
        [
            {"name": "name",   "value": document_name},
            {"name": "campo",  "value": campo},
            {"name": "te",     "value": te},
            {"name": "per",    "value": per},
            {"name": "ve",     "value": v_extraido},
            {"name": "vc",     "value": v_correto},
            {"name": "status", "value": status},
            {"name": "te_err", "value": tipo_erro},
            {"name": "te_det", "value": tipo_erro_detalhe or None},
            {"name": "por",    "value": user},
        ],
    )
    return status


@router.get("/me")
def get_me(request: Request):
    return {"email": _current_user(request)}


@router.get("/corrections/{document_name}")
def get_corrections(document_name: str):
    """Retorna o estado de revisao para o frontend, no formato compativel
    com o que ele ja espera: dict keyed por campo__te__per."""
    rows = execute_sql(
        f"""SELECT campo, COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo,
                   valor_extraido, valor_corrente,
                   tipo_erro,
                   COALESCE(tipo_erro_detalhe, '') AS comentario,
                   status,
                   CAST(revisado_em AS STRING) AS confirmado_em,
                   COALESCE(revisado_por, '') AS confirmado_por,
                   CAST(criado_em AS STRING) AS criado_em
            FROM {REVISOES_TABLE}
            WHERE document_name = :name
              AND status != 'llm'
            ORDER BY criado_em DESC""",
        [{"name": "name", "value": document_name}],
    )
    # Frontend espera valor_extraido e valor_correto como STRING
    out = {}
    for r in rows:
        ve = r.get("valor_extraido")
        vc = r.get("valor_corrente")
        out[f"{r['campo']}__{r['tipo_entidade']}__{r['periodo']}"] = {
            "campo": r["campo"],
            "tipo_entidade": r["tipo_entidade"],
            "periodo": r["periodo"],
            "valor_extraido": "" if ve is None else str(ve),
            "valor_correto":  "" if vc is None else str(vc),
            "comentario": r["comentario"],
            "tipo_erro": r.get("tipo_erro"),
            "status": r["status"],  # corrigido ou confirmado
            "confirmado_em": r["confirmado_em"],
            "confirmado_por": r["confirmado_por"],
            "criado_em": r["criado_em"],
        }
    return out


@router.post("/corrections")
def save_correction(c: Correction, request: Request):
    te = c.tipo_entidade or ""
    per = c.periodo or ""
    user = _current_user(request)
    _raise_if_finalized(c.document_name, te, per)

    status = _upsert_revisao(
        c.document_name, te, per, c.campo,
        c.valor_extraido, c.valor_correto,
        user, c.tipo_erro, c.comentario,
    )
    return {"status": "ok", "user": user, "revisao_status": status}


@router.post("/corrections/bulk")
def save_bulk_corrections(payload: BulkCorrections, request: Request):
    """Bulk: usado pelo botao 'Confirmar tudo' — marca campos LLM puros como confirmados
    sem alterar valor."""
    te = payload.tipo_entidade or ""
    per = payload.periodo or ""
    user = _current_user(request)
    _raise_if_finalized(payload.document_name, te, per)

    saved = 0
    for item in payload.items:
        campo = item.get("campo") or ""
        if not campo:
            continue
        _upsert_revisao(
            payload.document_name, te, per, campo,
            str(item.get("valor_extraido", "") or ""),
            str(item.get("valor_correto", "") or ""),
            user, item.get("tipo_erro"), str(item.get("comentario", "") or ""),
        )
        saved += 1
    return {"status": "ok", "user": user, "count": saved}


@router.post("/corrections/{document_name}/{campo}/confirm")
def confirm_correction(
    document_name: str, campo: str, request: Request,
    tipo_entidade: str = "", periodo: str = "",
):
    """Marca um registro existente como confirmado (sem alterar valor).
    Compat com a UI antiga — equivalente a salvar sem mudar valor."""
    user = _current_user(request)
    _raise_if_finalized(document_name, tipo_entidade, periodo)
    execute_update(
        f"""UPDATE {REVISOES_TABLE}
              SET status       = 'confirmado',
                  revisado_em  = NOW(),
                  revisado_por = :por
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per""",
        [
            {"name": "name",  "value": document_name},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": tipo_entidade},
            {"name": "per",   "value": periodo},
            {"name": "por",   "value": user},
        ],
    )
    rows = execute_sql(
        f"""SELECT CAST(revisado_em AS STRING) AS confirmado_em,
                   COALESCE(revisado_por,'') AS confirmado_por
            FROM {REVISOES_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per
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
    document_name: str, campo: str, request: Request,
    tipo_entidade: str = "", periodo: str = "",
):
    """Restaura o campo para 'llm' (desfaz a revisao). DELETE da linha em revisoes_em_andamento."""
    _ = request
    _raise_if_finalized(document_name, tipo_entidade, periodo)
    execute_update(
        f"""DELETE FROM {REVISOES_TABLE}
            WHERE document_name = :name AND campo = :campo
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per""",
        [
            {"name": "name",  "value": document_name},
            {"name": "campo", "value": campo},
            {"name": "te",    "value": tipo_entidade},
            {"name": "per",   "value": periodo},
        ],
    )
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Helper usado por finalize.py
# ---------------------------------------------------------------------------

def _update_resultados_final(document_name: str, tipo_entidade: str, periodo: str, user: str):
    """Reconstroi resultados_final aplicando revisoes em cima de resultados.
    JSON gravado e 'limpo' — sem fontes nem _postprocessed."""
    from ..config import RESULTS_TABLE

    rows = execute_sql(
        f"""SELECT extracted_json, razao_social, cnpj, tipo_demonstrativo,
                   tipo_documento, numeromeses, moeda, escala_valores
            FROM {RESULTS_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per
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
        data = json.loads(row["extracted_json"]) if isinstance(row["extracted_json"], str) else row["extracted_json"]
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, dict):
        return

    # Aplica revisoes em cima do JSON cru
    revisoes = execute_sql(
        f"""SELECT campo, valor_corrente
            FROM {REVISOES_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per
              AND status = 'corrigido'""",  # so corrigidos alteram o JSON; confirmado = mesmo valor
        [
            {"name": "name", "value": document_name},
            {"name": "te",   "value": tipo_entidade},
            {"name": "per",  "value": periodo},
        ],
    )
    for r in revisoes:
        campo = r["campo"]
        valor = r["valor_corrente"]
        parts = campo.split(".")
        obj = data
        for p in parts[:-1]:
            if isinstance(obj, dict) and p in obj:
                obj = obj[p]
            else:
                obj = None
                break
        if obj is not None and isinstance(obj, dict):
            if valor is None:
                obj[parts[-1]] = 0
            else:
                try:
                    f = float(valor)
                    obj[parts[-1]] = int(f) if f.is_integer() else f
                except (ValueError, TypeError):
                    obj[parts[-1]] = valor

    # Limpa fontes e _postprocessed antes de gravar
    clean = {k: v for k, v in data.items() if k not in ("fontes", "_postprocessed")}
    corrected_json = json.dumps(clean, ensure_ascii=False)

    ident = data.get("identificacao", {}) if isinstance(data, dict) else {}

    def _to_int(v):
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    td_val = _to_int(ident.get("tipo_demonstrativo", row.get("tipo_demonstrativo")))
    tdoc_val = _to_int(ident.get("tipo_documento", row.get("tipo_documento")))
    nm_val = _to_int(ident.get("numeroMeses", row.get("numeromeses")))
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
              extracted_json     = EXCLUDED.extracted_json,
              razao_social       = EXCLUDED.razao_social,
              cnpj               = EXCLUDED.cnpj,
              tipo_demonstrativo = EXCLUDED.tipo_demonstrativo,
              tipo_documento     = EXCLUDED.tipo_documento,
              numeroMeses        = EXCLUDED.numeroMeses,
              moeda              = EXCLUDED.moeda,
              escala_valores     = EXCLUDED.escala_valores,
              atualizado_em      = NOW(),
              atualizado_por     = EXCLUDED.atualizado_por""",
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
