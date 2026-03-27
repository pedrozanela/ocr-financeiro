import io
import json
import os
import requests as _requests
from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File
from ..config import get_client, PDF_VOLUME_PATH, RESULTS_TABLE, OCR_ENDPOINT, DATABRICKS_HOST, SERVERLESS_WAREHOUSE_ID

router = APIRouter()

# In-memory status tracker (resets on app restart; sufficient for this demo)
_status: dict[str, dict] = {}


def _extract_text_ai_parse(volume_path: str) -> str:
    """Extract text from a PDF stored in a Unity Catalog Volume using ai_parse_document."""
    from ..db import execute_sql
    rows = execute_sql(
        f"""
        SELECT concat_ws('\\n\\n',
          transform(
            try_cast(ai_parse_document(content):document:elements AS ARRAY<VARIANT>),
            element -> try_cast(element:content AS STRING)
          )
        ) AS text
        FROM read_files('{volume_path}', format => 'binaryFile')
        """,
        warehouse_id=SERVERLESS_WAREHOUSE_ID,
    )
    if not rows:
        return ""
    return rows[0].get("text") or ""


def _call_ocr_endpoint(text: str, client) -> list:
    """HTTP call with explicit timeout — avoids SDK hang on scale-to-zero cold start."""
    token = os.environ.get("OCR_PAT")
    if not token:
        import base64
        try:
            from ..config import SECRET_SCOPE, SECRET_KEY
            secret = client.secrets.get_secret(scope=SECRET_SCOPE, key=SECRET_KEY)
            token = base64.b64decode(secret.value).decode()
        except Exception:
            token = client.config.token
    host = client.config.host or DATABRICKS_HOST
    if not host.startswith("http"):
        host = f"https://{host}"
    url = f"{host.rstrip('/')}/serving-endpoints/{OCR_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for attempt in range(3):
        try:
            resp = _requests.post(
                url,
                headers=headers,
                json={"dataframe_records": [{"text": text}]},
                timeout=600,
            )
            resp.raise_for_status()
            break
        except (_requests.exceptions.Timeout, _requests.exceptions.ConnectionError) as e:
            if attempt == 2:
                raise RuntimeError(f"Endpoint ocupado ou lento. Tente novamente em alguns minutos.")
            import time; time.sleep(30)
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
        result = dict(result)  # shallow copy to avoid mutating caller's list
        assessment = result.pop("_assessment", [])
        usage = result.pop("_usage", {})
        assessment_json = json.dumps(assessment, ensure_ascii=False)
        token_usage_json = json.dumps(usage, ensure_ascii=False)
        extracted_json = json.dumps(result, ensure_ascii=False)
        td   = str(_get(result, "identificacao.tipo_demonstrativo") or "")
        moe  = str(_get(result, "identificacao.moeda") or "")
        esc  = str(_get(result, "identificacao.escala_valores") or "")
        params = [
            {"name": "doc",        "value": document_name},
            {"name": "te",         "value": str(_get(result, "tipo_entidade") or "")},
            {"name": "per",        "value": str(_get(result, "identificacao.periodo") or "")},
            {"name": "json",       "value": extracted_json},
            {"name": "assessment", "value": assessment_json},
            {"name": "usage",      "value": token_usage_json},
            {"name": "rs",         "value": str(_get(result, "razao_social") or "")},
            {"name": "cnpj",       "value": str(_get(result, "cnpj") or "")},
            {"name": "td",         "value": td},
            {"name": "moe",        "value": moe},
            {"name": "esc",        "value": esc},
        ]
        execute_sql(f"""
            MERGE INTO {RESULTS_TABLE} AS t
            USING (SELECT :doc AS document_name, :te AS tipo_entidade, :per AS periodo) AS s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json     = :json,
                assessment_json    = :assessment,
                token_usage_json   = :usage,
                razao_social       = :rs,
                cnpj               = :cnpj,
                tipo_demonstrativo = :td,
                moeda              = :moe,
                escala_valores     = :esc
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                 token_usage_json, razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores)
            VALUES (:doc, :te, :per, :json, :assessment, :usage, :rs, :cnpj, :td, :moe, :esc)
        """, params)


def _process_background(document_name: str, volume_path: str):
    """Parse PDF with ai_parse_document → OCR endpoint → save."""
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


@router.post("/documents/upload")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Apenas arquivos PDF são aceitos.")

    data = await file.read()
    document_name = file.filename
    client = get_client()

    # 1. Save PDF to volume (fast, sync)
    volume_path = f"{PDF_VOLUME_PATH}/{document_name}"
    try:
        client.files.upload(volume_path, io.BytesIO(data), overwrite=True)
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar PDF no volume: {e}")

    # 2. Queue parsing + OCR in background and return immediately
    _status[document_name] = {"status": "processing", "step": "parsing"}
    background_tasks.add_task(_process_background, document_name, volume_path)

    return {"document_name": document_name, "status": "processing"}


@router.get("/documents/{document_name}/status")
def get_upload_status(document_name: str):
    return _status.get(document_name, {"status": "unknown"})
