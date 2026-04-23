from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from ..db import execute_sql
from ..config import RESULTS_TABLE, SOURCE_TABLE, PDF_VOLUME_PATH, get_client
from .upload import _runs

router = APIRouter()


@router.get("/documents")
def list_documents():
    # One row per document (use ANY_VALUE for non-key columns; prefer CONSOLIDADO data)
    rows = execute_sql(f"""
        SELECT
            document_name,
            ANY_VALUE(razao_social) AS razao_social,
            ANY_VALUE(cnpj) AS cnpj,
            MAX(periodo) AS periodo,
            MAX(TRY_CAST(get_json_object(extracted_json, '$.ativo_total') AS DOUBLE)) AS ativo_total,
            MAX(TRY_CAST(get_json_object(extracted_json, '$.dre.lucro_liquido') AS DOUBLE)) AS lucro_liquido
        FROM {RESULTS_TABLE}
        GROUP BY document_name
        ORDER BY document_name
    """)
    return rows


@router.get("/documents/{document_name}")
def get_document(document_name: str):
    rows = execute_sql(
        f"""SELECT document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                CAST(processado_em AS STRING) AS processado_em, COALESCE(modelo_versao, '') AS modelo_versao,
                COALESCE(modo_extracao, '') AS modo_extracao
            FROM {RESULTS_TABLE}
            WHERE document_name = :name
            ORDER BY tipo_entidade, periodo DESC""",
        [{"name": "name", "value": document_name}],
    )
    if not rows:
        raise HTTPException(404, "Documento não encontrado")

    import json
    records = []
    for row in rows:
        raw = row["extracted_json"]
        data = json.loads(raw) if isinstance(raw, str) else raw
        raw_assessment = row.get("assessment_json")
        assessment = json.loads(raw_assessment) if isinstance(raw_assessment, str) and raw_assessment else []
        records.append({
            "tipo_entidade": row.get("tipo_entidade"),
            "periodo": row.get("periodo"),
            "data": data,
            "assessment": assessment,
            "processado_em": row.get("processado_em"),
            "modelo_versao": row.get("modelo_versao"),
            "modo_extracao": row.get("modo_extracao"),
        })

    return {
        "document_name": document_name,
        "records": records,
        # backward-compat: expose first record's data at top level
        "data": records[0]["data"] if records else None,
    }


@router.post("/documents/{document_name}/reprocess")
def reprocess_document(document_name: str):
    from .upload import _get_batch_job_id
    from ..db import execute_update
    client = get_client()
    try:
        # Remove from resultados so batch_job picks it up as new
        execute_update(
            f"DELETE FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        job_id = _get_batch_job_id(client)
        # Passa pdf_name para processar APENAS este documento (modo single)
        run = client.jobs.run_now(job_id=job_id, notebook_params={"pdf_name": document_name})
        _runs[document_name] = run.run_id
        return {"document_name": document_name, "status": "processing", "run_id": run.run_id}
    except Exception as e:
        raise HTTPException(500, f"Erro ao disparar job: {e}")


@router.get("/documents/{document_name}/ocr-text")
def get_document_ocr_text(document_name: str):
    rows = execute_sql(
        f"SELECT document_text, atualizado_em, atualizado_por FROM {SOURCE_TABLE} WHERE document_name = :name LIMIT 1",
        [{"name": "name", "value": document_name}],
    )
    if not rows or not rows[0].get("document_text"):
        raise HTTPException(404, "Texto OCR não disponível para este documento")
    return {
        "document_text": rows[0]["document_text"],
        "atualizado_em": str(rows[0].get("atualizado_em") or ""),
        "atualizado_por": rows[0].get("atualizado_por") or "",
    }


@router.get("/documents/{document_name}/pdf")
def get_document_pdf(document_name: str):
    client = get_client()
    fname = document_name if document_name.endswith(".pdf") else f"{document_name}.pdf"
    pdf_path = f"{PDF_VOLUME_PATH}/{fname}"
    try:
        dl = client.files.download(pdf_path)
        content = dl.contents.read()
        return Response(
            content=content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{fname}"'},
        )
    except Exception as e:
        raise HTTPException(404, f"PDF não encontrado: {e}")
