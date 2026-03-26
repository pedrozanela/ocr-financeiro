from fastapi import APIRouter
from ..config import get_client, FEWSHOT_JOB_ID

router = APIRouter()


@router.post("/admin/update-model")
def trigger_model_update():
    """Trigger the fewshot update job to retrain the model with accumulated corrections."""
    w = get_client()
    run = w.jobs.run_now(job_id=FEWSHOT_JOB_ID)
    return {
        "status": "triggered",
        "run_id": run.run_id,
        "message": "Job de atualização do modelo disparado. O endpoint será atualizado automaticamente.",
    }


@router.get("/admin/update-model/status/{run_id}")
def get_model_update_status(run_id: int):
    """Check status of a model update job run."""
    w = get_client()
    run = w.jobs.get_run(run_id=run_id)
    state = run.state
    return {
        "life_cycle_state": state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN",
        "result_state": state.result_state.value if state.result_state else "",
        "state_message": state.state_message or "",
    }
