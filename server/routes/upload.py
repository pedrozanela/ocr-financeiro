import io
from fastapi import APIRouter, HTTPException, UploadFile, File
from ..config import get_client, PDF_VOLUME_PATH, BATCH_JOB_ID

router = APIRouter()

# Track which document triggered which job run
_runs: dict[str, int] = {}


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

    # 2. Trigger the batch_job to process the new PDF
    if BATCH_JOB_ID:
        try:
            run = client.jobs.run_now(job_id=BATCH_JOB_ID)
            _runs[document_name] = run.run_id
        except Exception as e:
            # Job trigger failed — PDF is saved, user can run job manually
            return {"document_name": document_name, "status": "uploaded",
                    "detail": f"PDF salvo no volume. Job nao disparado: {e}"}

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

        if life == "TERMINATED" and result == "SUCCESS":
            return {"status": "done"}
        elif life == "TERMINATED":
            msg = state.state_message or f"Job failed: {result}"
            return {"status": "error", "detail": msg}
        elif life in ("PENDING", "RUNNING", "BLOCKED"):
            return {"status": "processing", "step": life.lower()}
        else:
            return {"status": "processing", "step": life.lower()}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
