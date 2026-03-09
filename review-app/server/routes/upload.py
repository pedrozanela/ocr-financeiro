import io
import json
import os
import requests as _requests
from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File
from ..config import get_client, PDF_VOLUME_PATH, RESULTS_TABLE, OCR_ENDPOINT, DATABRICKS_HOST

router = APIRouter()

# In-memory status tracker (resets on app restart; sufficient for this demo)
_status: dict[str, dict] = {}


def _extract_text_from_pdf(data: bytes) -> str:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    pages = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pages.append(text)
    return "\n\n".join(pages)


def _call_ocr_endpoint(text: str, client) -> list:
    """HTTP call with explicit timeout — avoids SDK hang on scale-to-zero cold start."""
    token = os.environ.get("OCR_PAT")
    if not token:
        import base64
        try:
            secret = client.secrets.get_secret(scope="pedro-zanela-scope", key="techfin-ocr-pat")
            token = base64.b64decode(secret.value).decode()
        except Exception:
            token = client.config.token
    host = client.config.host or DATABRICKS_HOST
    if not host.startswith("http"):
        host = f"https://{host}"
    url = f"{host.rstrip('/')}/serving-endpoints/{OCR_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    resp = _requests.post(
        url,
        headers=headers,
        json={"dataframe_records": [{"text": text}]},
        timeout=300,
    )
    resp.raise_for_status()
    r = resp.json().get("predictions", resp.json())
    if isinstance(r, list) and len(r) == 1:
        r = r[0]
    if isinstance(r, str):
        r = json.loads(r)
    if isinstance(r, dict):
        r = [r]
    return r


def _save_result(document_name: str, results: list, client):
    from ..db import execute_sql
    if isinstance(results, dict):
        results = [results]

    def _get(obj, path: str):
        parts = path.split(".")
        cur = obj
        for p in parts:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
        return cur

    for result in results:
        extracted_json = json.dumps(result, ensure_ascii=False)
        params = [
            {"name": "doc",  "value": document_name},
            {"name": "te",   "value": str(_get(result, "tipo_entidade") or "")},
            {"name": "per",  "value": str(_get(result, "identificacao.periodo") or "")},
            {"name": "json", "value": extracted_json},
            {"name": "rs",   "value": str(_get(result, "razao_social") or "")},
            {"name": "cnpj", "value": str(_get(result, "cnpj") or "")},
            {"name": "at",   "value": str(_get(result, "ativo_total") or "")},
            {"name": "ll",   "value": str(_get(result, "dre.lucro_liquido") or "")},
        ]
        execute_sql(f"""
            MERGE INTO {RESULTS_TABLE} AS t
            USING (SELECT :doc AS document_name, :te AS tipo_entidade, :per AS periodo) AS s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json = :json,
                razao_social   = :rs,
                cnpj           = :cnpj,
                ativo_total    = TRY_CAST(:at AS DOUBLE),
                lucro_liquido  = TRY_CAST(:ll AS DOUBLE)
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json,
                 razao_social, cnpj, ativo_total, lucro_liquido)
            VALUES (:doc, :te, :per, :json, :rs, :cnpj,
                    TRY_CAST(:at AS DOUBLE), TRY_CAST(:ll AS DOUBLE))
        """, params)


def _process_background(document_name: str, text: str):
    """Runs OCR + save in background so the upload response returns immediately."""
    try:
        _status[document_name] = {"status": "processing", "step": "ocr"}
        client = get_client()
        results = _call_ocr_endpoint(text, client)
        _status[document_name] = {"status": "processing", "step": "saving"}
        _save_result(document_name, results, client)
        _status[document_name] = {"status": "done", "records": len(results)}
    except Exception as e:
        _status[document_name] = {"status": "error", "detail": str(e)}


@router.post("/documents/upload")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Apenas arquivos PDF são aceitos.")

    data = await file.read()
    document_name = file.filename
    client = get_client()

    # 1. Save PDF to volume (fast)
    volume_path = f"{PDF_VOLUME_PATH}/{document_name}"
    try:
        client.files.upload(volume_path, io.BytesIO(data), overwrite=True)
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar PDF no volume: {e}")

    # 2. Extract text (fast, local)
    try:
        text = _extract_text_from_pdf(data)
    except Exception as e:
        raise HTTPException(500, f"Erro ao extrair texto do PDF: {e}")

    if not text.strip():
        raise HTTPException(422, "Não foi possível extrair texto do PDF. O arquivo pode ser uma imagem escaneada sem OCR.")

    # 3. Queue OCR in background and return immediately
    _status[document_name] = {"status": "processing", "step": "ocr"}
    background_tasks.add_task(_process_background, document_name, text)

    return {"document_name": document_name, "status": "processing"}


@router.get("/documents/{document_name}/status")
def get_upload_status(document_name: str):
    return _status.get(document_name, {"status": "unknown"})
