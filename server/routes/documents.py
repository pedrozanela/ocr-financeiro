from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from ..db import execute_sql
from ..config import RESULTS_TABLE, PDF_VOLUME_PATH, get_client
from .upload import _extract_text_ai_parse, _call_ocr_endpoint, _save_result, _status

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
        f"""SELECT document_name, tipo_entidade, periodo, extracted_json, assessment_json
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
        })

    return {
        "document_name": document_name,
        "records": records,
        # backward-compat: expose first record's data at top level
        "data": records[0]["data"] if records else None,
    }


def _reprocess_background(document_name: str, volume_path: str):
    try:
        _status[document_name] = {"status": "processing", "step": "parsing"}
        text = _extract_text_ai_parse(volume_path)
        if not text.strip():
            _status[document_name] = {"status": "error", "detail": "Não foi possível extrair texto do PDF."}
            return
        _status[document_name] = {"status": "processing", "step": "ocr"}
        client = get_client()
        results = _call_ocr_endpoint(text, client)
        _status[document_name] = {"status": "processing", "step": "saving"}
        _save_result(document_name, results, client)
        _status[document_name] = {"status": "done", "records": len(results)}
    except Exception as e:
        _status[document_name] = {"status": "error", "detail": str(e)}


@router.post("/documents/{document_name}/reprocess")
def reprocess_document(document_name: str, background_tasks: BackgroundTasks):
    fname = document_name if document_name.endswith(".pdf") else f"{document_name}.pdf"
    volume_path = f"{PDF_VOLUME_PATH}/{fname}"
    _status[document_name] = {"status": "processing", "step": "parsing"}
    background_tasks.add_task(_reprocess_background, document_name, volume_path)
    return {"document_name": document_name, "status": "processing"}


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
