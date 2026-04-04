import io
import os
from fastapi import APIRouter, HTTPException, UploadFile, File, Request
from ..config import get_client, PDF_VOLUME_PATH, RESULTS_TABLE, UC_CATALOG, UC_SCHEMA, VISION_JOB_ID
from ..db import execute_update

router = APIRouter()

# Track which document triggered which job run
_runs: dict[str, int] = {}

# Cache the batch job ID (looked up once by name)
_batch_job_id: int | None = None



def _get_batch_job_id(client) -> int:
    """Find the batch_job ID by name. Cached after first lookup."""
    global _batch_job_id
    if _batch_job_id:
        return _batch_job_id
    # Try env var first (if set by app.yaml or bundle)
    import os
    env_id = os.environ.get("BATCH_JOB_ID", "0")
    if env_id and env_id != "0":
        _batch_job_id = int(env_id)
        return _batch_job_id
    # Fall back to looking up by name
    for job in client.jobs.list(name="ocr-financeiro-batch-job"):
        _batch_job_id = job.job_id
        return _batch_job_id
    raise RuntimeError("Job 'ocr-financeiro-batch-job' nao encontrado. Execute o bundle deploy primeiro.")


@router.post("/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Apenas arquivos PDF são aceitos.")

    data = await file.read()
    document_name = file.filename
    client = get_client()

    # 1. Save PDF to volume
    volume_path = f"{PDF_VOLUME_PATH}/{document_name}"
    try:
        client.files.upload(volume_path, io.BytesIO(data), overwrite=True)
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar PDF no volume: {e}")

    # 2. Remove existing result so batch_job reprocesses even if already extracted
    try:
        execute_update(
            f"DELETE FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
    except Exception as e:
        print(f"[upload] WARNING: could not delete existing result for {document_name}: {e}")

    # 3. Trigger the batch_job to process the new PDF
    try:
        job_id = _get_batch_job_id(client)
        print(f"[upload] Triggering batch job {job_id} for {document_name}")
        run = client.jobs.run_now(job_id=job_id)
        _runs[document_name] = run.run_id
        print(f"[upload] Job run {run.run_id} started for {document_name}")
    except Exception as e:
        print(f"[upload] ERROR triggering job for {document_name}: {e}")
        return {"document_name": document_name, "status": "uploaded",
                "detail": f"PDF salvo no volume. Job nao disparado: {e}"}

    return {"document_name": document_name, "status": "processing",
            "run_id": _runs.get(document_name)}


@router.post("/documents/upload-performance")
async def upload_document_performance(request: Request, file: UploadFile = File(...)):
    """Upload PDF e processa via Vision OCR (Claude claude-sonnet-4-6 → extrator-financeiro)."""
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Apenas arquivos PDF são aceitos.")

    data = await file.read()
    document_name = file.filename
    client = get_client()

    # 1. Save PDF to volume
    volume_path = f"{PDF_VOLUME_PATH}/{document_name}"
    try:
        client.files.upload(volume_path, io.BytesIO(data), overwrite=True)
    except Exception as e:
        raise HTTPException(500, f"Erro ao salvar PDF no volume: {e}")

    # 2. Remove existing result so vision notebook writes fresh
    try:
        execute_update(
            f"DELETE FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
    except Exception as e:
        print(f"[upload-performance] WARNING: could not delete existing result for {document_name}: {e}")

    # 3. Trigger vision job via run_now (job ID: VISION_JOB_ID)
    try:
        run = client.jobs.run_now(
            job_id=VISION_JOB_ID,
            notebook_params={"pdf_name": document_name},
        )
        _runs[document_name] = run.run_id
        print(f"[upload-performance] Vision run {run.run_id} started for {document_name}")
    except Exception as e:
        print(f"[upload-performance] ERROR triggering vision job for {document_name}: {e}")
        return {"document_name": document_name, "status": "uploaded",
                "detail": f"PDF salvo no volume. Vision job não disparado: {e}"}

    return {"document_name": document_name, "status": "processing",
            "run_id": _runs.get(document_name)}


@router.get("/documents/{document_name}/status")
def get_upload_status(document_name: str):
    run_id = _runs.get(document_name)
    if not run_id:
        return {"status": "unknown"}

    try:
        client = get_client()
        run = client.jobs.get_run(run_id)
        state = run.state
        life = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
        result = state.result_state.value if state and state.result_state else None

        duration_s = round(run.run_duration / 1000, 1) if run.run_duration else None

        if life == "TERMINATED" and result == "SUCCESS":
            return {"status": "done", "timing": {"total_s": duration_s}}
        elif life == "TERMINATED":
            msg = state.state_message or f"Job failed: {result}"
            return {"status": "error", "detail": msg, "timing": {"total_s": duration_s}}
        else:
            return {"status": "processing", "step": life.lower(), "timing": {"elapsed_s": duration_s}}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
