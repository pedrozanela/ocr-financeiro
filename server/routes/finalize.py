"""Finalização de documentos — envia para Techfin (PARC) e consolida estado local.

Por par (tipo_entidade, periodo) do documento:
  1. Consolida JSON aplicando revisoes em cima do extracted_json.
  2. Monta payload Techfin via mapper.
  3. POST /databricks/v1/balanco — síncrono, timeout 60s.
       - 2xx: sucesso normal.
       - 409: idempotente (re-submissão) → tratado como sucesso silencioso.
       - 4xx/5xx ou timeout: aborta, devolve erro pro usuário.
  4. UPSERT resultados_final com extracted_json + techfin_response + status='finalizado'.
  5. Snapshot revisoes → feedback_llm (append-only audit log).

Após todos os pares OK, limpa revisoes_em_andamento.

Re-submissão é uma operação válida (sobrescreve). Não bloqueamos se status='finalizado'.
"""
import json
import logging

import requests
from fastapi import APIRouter, HTTPException, Request

from ..db import execute_sql, execute_update
from ..config import RESULTS_FINAL_TABLE, REVISOES_TABLE, FEEDBACK_TABLE, RESULTS_TABLE
from ..integrations.techfin.client import TechfinClient
from ..integrations.techfin.mapper import map_extracted_to_techfin
from .corrections import _current_user

router = APIRouter()
logger = logging.getLogger("finalize")


@router.get("/finalize/{document_name}")
def get_finalize_status(document_name: str):
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


def _load_resultado(document_name: str, tipo_entidade: str, periodo: str) -> dict | None:
    rows = execute_sql(
        f"""SELECT extracted_json, razao_social, cnpj, tipo_demonstrativo,
                   tipo_documento, numeromeses, moeda, escala_valores,
                   COALESCE(modelo_versao, 'unknown') AS modelo_versao
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
    return rows[0] if rows else None


def _consolidate_extracted(resultado: dict, document_name: str, tipo_entidade: str, periodo: str) -> dict:
    """Aplica revisões 'corrigido' sobre o extracted_json original.
    Remove fontes e _postprocessed do JSON final (limpo para downstream)."""
    raw = resultado["extracted_json"]
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (json.JSONDecodeError, TypeError):
        data = {}
    if not isinstance(data, dict):
        data = {}

    revisoes = execute_sql(
        f"""SELECT campo, valor_corrente
            FROM {REVISOES_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per
              AND status = 'corrigido'""",
        [
            {"name": "name", "value": document_name},
            {"name": "te",   "value": tipo_entidade},
            {"name": "per",  "value": periodo},
        ],
    )
    for r in revisoes:
        parts = r["campo"].split(".")
        obj = data
        for p in parts[:-1]:
            if p not in obj or not isinstance(obj[p], dict):
                obj[p] = {}
            obj = obj[p]
        v = r["valor_corrente"]
        if v is None:
            obj[parts[-1]] = 0
        else:
            try:
                f = float(v)
                obj[parts[-1]] = int(f) if f.is_integer() else f
            except (ValueError, TypeError):
                obj[parts[-1]] = v

    # JSON limpo (sem metadados)
    clean = {k: v for k, v in data.items() if k not in ("fontes", "_postprocessed")}
    return clean


def _upsert_resultado_final(
    document_name: str, tipo_entidade: str, periodo: str,
    extracted_clean: dict, resultado: dict, techfin_response: dict, user: str,
):
    """Grava resultados_final com JSON consolidado + resposta Techfin. UPSERT por (doc, te, per)."""
    ident = extracted_clean.get("identificacao", {}) or {}

    def _to_int(v):
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    td_val   = _to_int(ident.get("tipo_demonstrativo", resultado.get("tipo_demonstrativo")))
    tdoc_val = _to_int(ident.get("tipo_documento",    resultado.get("tipo_documento")))
    nm_val   = _to_int(ident.get("numeroMeses",       resultado.get("numeromeses")))
    rs_val    = ident.get("razao_social")    or resultado.get("razao_social")  or ""
    cnpj_val  = ident.get("cnpj")            or resultado.get("cnpj")          or ""
    moeda_val = ident.get("moeda")           or resultado.get("moeda")         or ""
    escala_val = ident.get("escala_valores") or resultado.get("escala_valores") or ""

    execute_update(
        f"""INSERT INTO {RESULTS_FINAL_TABLE}
              (document_name, tipo_entidade, periodo, extracted_json, razao_social, cnpj,
               tipo_demonstrativo, tipo_documento, numeroMeses, moeda, escala_valores,
               atualizado_em, atualizado_por,
               status, finalizado_em, finalizado_por, techfin_response)
            VALUES (:name, :te, :per, :json, :rs, :cnpj,
                    :td, :tdoc, :nm, :moeda, :escala,
                    NOW(), :user,
                    'finalizado', NOW(), :user, :tfresp)
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
              atualizado_por     = EXCLUDED.atualizado_por,
              status             = 'finalizado',
              finalizado_em      = NOW(),
              finalizado_por     = EXCLUDED.finalizado_por,
              techfin_response   = EXCLUDED.techfin_response""",
        [
            {"name": "name",   "value": document_name},
            {"name": "te",     "value": tipo_entidade},
            {"name": "per",    "value": periodo},
            {"name": "json",   "value": json.dumps(extracted_clean, ensure_ascii=False)},
            {"name": "rs",     "value": str(rs_val)},
            {"name": "cnpj",   "value": str(cnpj_val)},
            {"name": "td",     "value": td_val},
            {"name": "tdoc",   "value": tdoc_val},
            {"name": "nm",     "value": nm_val},
            {"name": "moeda",  "value": str(moeda_val)},
            {"name": "escala", "value": str(escala_val)},
            {"name": "user",   "value": user},
            {"name": "tfresp", "value": json.dumps(techfin_response, ensure_ascii=False)},
        ],
    )


def _snapshot_feedback(
    document_name: str, tipo_entidade: str, periodo: str,
    resultado: dict, user: str,
) -> int:
    """Copia revisoes_em_andamento (status != 'llm') → feedback_llm (append-only)."""
    try:
        raw = resultado["extracted_json"]
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (json.JSONDecodeError, TypeError):
        data = {}
    fontes = (data or {}).get("fontes", {}) or {}
    modelo_versao = resultado.get("modelo_versao") or "unknown"

    revisoes = execute_sql(
        f"""SELECT campo, valor_extraido, valor_corrente, status,
                   tipo_erro, tipo_erro_detalhe, revisado_por,
                   CAST(revisado_em AS STRING) AS revisado_em
            FROM {REVISOES_TABLE}
            WHERE document_name = :name
              AND COALESCE(tipo_entidade,'') = :te AND COALESCE(periodo,'') = :per
              AND status != 'llm'""",
        [
            {"name": "name", "value": document_name},
            {"name": "te",   "value": tipo_entidade},
            {"name": "per",  "value": periodo},
        ],
    )
    count = 0
    for r in revisoes:
        fonte = fontes.get(r["campo"])
        fonte_json = json.dumps(fonte, ensure_ascii=False) if fonte is not None else None
        execute_update(
            f"""INSERT INTO {FEEDBACK_TABLE}
                  (document_name, campo, tipo_entidade, periodo,
                   valor_llm, valor_final, acao, tipo_erro, tipo_erro_detalhe,
                   fonte_llm, revisado_por, revisado_em, submetido_em,
                   modelo_versao, prompt_versao)
                VALUES (:name, :campo, :te, :per,
                        :vl, :vf, :acao, :tipo_err, :tipo_det,
                        :fonte, :por, :rev_em, NOW(),
                        :mod, :prompt)""",
            [
                {"name": "name",     "value": document_name},
                {"name": "campo",    "value": r["campo"]},
                {"name": "te",       "value": tipo_entidade},
                {"name": "per",      "value": periodo},
                {"name": "vl",       "value": r["valor_extraido"]},
                {"name": "vf",       "value": r["valor_corrente"]},
                {"name": "acao",     "value": r["status"]},
                {"name": "tipo_err", "value": r.get("tipo_erro")},
                {"name": "tipo_det", "value": r.get("tipo_erro_detalhe")},
                {"name": "fonte",    "value": fonte_json},
                {"name": "por",      "value": r.get("revisado_por") or user},
                {"name": "rev_em",   "value": r.get("revisado_em")},
                {"name": "mod",      "value": modelo_versao},
                {"name": "prompt",   "value": "unknown"},
            ],
        )
        count += 1
    return count


def _send_to_techfin(client: TechfinClient, payload: dict, document_name: str, te: str, per: str) -> dict:
    """Envia payload pra Techfin. Trata 409 como sucesso silencioso (re-submissão = upsert)."""
    try:
        return client.submit_balanco(payload)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        body = (e.response.text if e.response is not None else "")[:500]
        if status == 409:
            logger.info("Techfin retornou 409 para %s (%s/%s) — tratando como upsert idempotente",
                        document_name, te, per)
            return {"status": "duplicate", "code": 409, "message": body}
        logger.error("Techfin HTTP %s para %s (%s/%s): %s", status, document_name, te, per, body)
        raise HTTPException(
            status_code=502,
            detail=_user_friendly_http_error(status, body),
        )
    except requests.Timeout:
        logger.warning("Techfin timeout para %s (%s/%s)", document_name, te, per)
        raise HTTPException(504, "Tempo esgotado ao enviar para Techfin. Tente novamente.")
    except requests.RequestException as e:
        logger.exception("Erro de rede enviando %s para Techfin", document_name)
        raise HTTPException(502, f"Erro de rede ao enviar para Techfin: {e}")


def _user_friendly_http_error(status: int, body: str) -> str:
    if status == 400:
        return f"Dados inválidos para a Techfin. Revise os campos. Detalhe: {body[:200]}"
    if status == 401:
        return "Autenticação com Techfin falhou. Contate o suporte."
    if status == 403:
        return "Sem permissão para enviar este documento. Contate o suporte."
    if status == 429:
        return "Muitas requisições para Techfin. Aguarde e tente novamente."
    if 500 <= status < 600:
        return "Erro no servidor Techfin. Tente novamente em alguns minutos."
    return f"Erro ao enviar para Techfin (HTTP {status}). {body[:200]}"


@router.get("/finalize/{document_name}/preview")
def preview_payload(document_name: str):
    """Retorna o payload que seria enviado à Techfin para cada par (te, per).
    Útil pra revisar antes de submeter de verdade. Não modifica nada."""
    pairs = execute_sql(
        f"""SELECT COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo
            FROM {RESULTS_TABLE}
            WHERE document_name = :name""",
        [{"name": "name", "value": document_name}],
    )
    if not pairs:
        raise HTTPException(404, "Documento não encontrado")

    previews = []
    for p in pairs:
        te, per = p["tipo_entidade"], p["periodo"]
        resultado = _load_resultado(document_name, te, per)
        if not resultado:
            continue
        extracted_clean = _consolidate_extracted(resultado, document_name, te, per)
        payload = map_extracted_to_techfin(
            extracted_clean,
            tipo_entidade=te or "INDIVIDUAL",
            razao_social=resultado.get("razao_social") or "",
        )
        previews.append({"tipo_entidade": te, "periodo": per, "payload": payload})
    return {"document_name": document_name, "would_send_to": TechfinClient.BALANCO_URL, "records": previews}


@router.post("/finalize/{document_name}")
def finalize_document(document_name: str, request: Request, dry_run: bool = False):
    """Submete o documento à Techfin e consolida estado local.

    Query params:
      dry_run=true  → NÃO chama Techfin. Grava resultados_final com techfin_response
                       contendo o payload + flag dry_run=true. Util pra testar o fluxo
                       sem depender da API externa.

    Para cada par (tipo_entidade, periodo):
      1. Consolida JSON aplicando revisões.
      2. Monta payload e envia para Techfin (timeout 60s, 409 = upsert), exceto em dry_run.
      3. UPSERT resultados_final (extracted_json + techfin_response + status='finalizado').
      4. Snapshot feedback_llm.

    Após todos os pares OK: limpa revisoes_em_andamento. Re-submissão é idempotente.
    """
    user = _current_user(request)

    pairs = execute_sql(
        f"""SELECT COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo
            FROM {RESULTS_TABLE}
            WHERE document_name = :name""",
        [{"name": "name", "value": document_name}],
    )
    if not pairs:
        raise HTTPException(404, "Documento não encontrado em resultados")

    client = None if dry_run else TechfinClient()
    total_feedback = 0
    techfin_results = []

    for p in pairs:
        te  = p["tipo_entidade"]
        per = p["periodo"]
        resultado = _load_resultado(document_name, te, per)
        if not resultado:
            logger.warning("Par %s/%s sem resultado original para %s, pulando", te, per, document_name)
            continue

        # 1. Consolida JSON (aplica revisões corrigidas)
        extracted_clean = _consolidate_extracted(resultado, document_name, te, per)

        # 2. Monta payload
        payload = map_extracted_to_techfin(
            extracted_clean,
            tipo_entidade=te or "INDIVIDUAL",
            razao_social=resultado.get("razao_social") or "",
        )

        # 3. Envia Techfin (ou simula em dry_run)
        if dry_run:
            from datetime import datetime, timezone
            techfin_response = {
                "dry_run": True,
                "would_send_to": TechfinClient.BALANCO_URL,
                "payload": payload,
                "simulated_at": datetime.now(timezone.utc).isoformat(),
                "simulated_by": user,
            }
            logger.info("DRY-RUN: %s (%s/%s) — Techfin não foi chamada", document_name, te, per)
        else:
            techfin_response = _send_to_techfin(client, payload, document_name, te, per)
        techfin_results.append({"tipo_entidade": te, "periodo": per, "response": techfin_response})

        # 4. UPSERT resultados_final
        _upsert_resultado_final(document_name, te, per, extracted_clean, resultado, techfin_response, user)

        # 5. Snapshot feedback_llm
        total_feedback += _snapshot_feedback(document_name, te, per, resultado, user)

    # 6. Limpa estado transitório
    execute_update(
        f"DELETE FROM {REVISOES_TABLE} WHERE document_name = :name",
        [{"name": "name", "value": document_name}],
    )

    return {
        "document_name": document_name,
        "status": "finalizado",
        "dry_run": dry_run,
        "finalizado_por": user,
        "feedback_count": total_feedback,
        "techfin_results": techfin_results,
    }
