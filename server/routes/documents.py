from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from ..db import execute_sql
from ..config import RESULTS_TABLE, PDF_VOLUME_PATH, get_client
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
                CAST(processado_em AS STRING) AS processado_em, COALESCE(modelo_versao, '') AS modelo_versao
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
        })

    return {
        "document_name": document_name,
        "records": records,
        # backward-compat: expose first record's data at top level
        "data": records[0]["data"] if records else None,
    }


@router.post("/documents/{document_name}/reprocess")
def reprocess_document(document_name: str):
    from ..config import BATCH_JOB_ID
    if not BATCH_JOB_ID:
        raise HTTPException(500, "BATCH_JOB_ID nao configurado.")
    client = get_client()
    try:
        run = client.jobs.run_now(job_id=BATCH_JOB_ID)
        _runs[document_name] = run.run_id
        return {"document_name": document_name, "status": "processing", "run_id": run.run_id}
    except Exception as e:
        raise HTTPException(500, f"Erro ao disparar job: {e}")


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
