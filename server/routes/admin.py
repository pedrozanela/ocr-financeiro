import json
from fastapi import APIRouter
from ..db import execute_sql, execute_update
from ..config import get_client, FEWSHOT_JOB_ID, CORRECTIONS_TABLE, RESULTS_TABLE

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


@router.post("/admin/reconcile-corrections")
def reconcile_corrections():
    """Compare corrections with current extracted values. Mark as 'resolvido' if model now matches."""
    # Get all non-resolved corrections
    corrections = execute_sql(f"""
        SELECT c.document_name, c.tipo_entidade, c.periodo, c.campo, c.valor_correto,
               r.extracted_json
        FROM {CORRECTIONS_TABLE} c
        JOIN {RESULTS_TABLE} r
            ON c.document_name = r.document_name
            AND COALESCE(c.tipo_entidade, '') = COALESCE(r.tipo_entidade, '')
            AND COALESCE(c.periodo, '') = COALESCE(r.periodo, '')
        WHERE COALESCE(c.status, 'pendente') != 'resolvido'
    """)

    resolved = 0
    still_pending = 0

    for row in corrections:
        campo = row["campo"]
        valor_correto = row["valor_correto"]
        extracted_json = row["extracted_json"]

        try:
            data = json.loads(extracted_json) if isinstance(extracted_json, str) else extracted_json
        except (json.JSONDecodeError, TypeError):
            still_pending += 1
            continue

        # Navigate to the field value in extracted_json
        parts = campo.split(".")
        cur = data
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = None
                break

        if cur is None:
            still_pending += 1
            continue

        # Compare: is the current extracted value close to the corrected value?
        try:
            current_val = float(cur)
            corrected_val = float(valor_correto)
            match = abs(current_val - corrected_val) < 0.01 * max(abs(corrected_val), 1)
        except (ValueError, TypeError):
            match = str(cur).strip() == str(valor_correto).strip()

        if match:
            # Model now extracts the correct value — mark as resolved
            doc = row["document_name"].replace("'", "''")
            te = (row["tipo_entidade"] or "").replace("'", "''")
            per = (row["periodo"] or "").replace("'", "''")
            campo_esc = campo.replace("'", "''")
            execute_update(
                f"""UPDATE {CORRECTIONS_TABLE}
                    SET status = 'resolvido', resolvido_em = CURRENT_TIMESTAMP()
                    WHERE document_name = :name AND campo = :campo
                      AND COALESCE(tipo_entidade, '') = :te
                      AND COALESCE(periodo, '') = :per""",
                [
                    {"name": "name",  "value": row["document_name"]},
                    {"name": "campo", "value": campo},
                    {"name": "te",    "value": row["tipo_entidade"] or ""},
                    {"name": "per",   "value": row["periodo"] or ""},
                ],
            )
            resolved += 1
        else:
            still_pending += 1

    return {
        "resolved": resolved,
        "still_pending": still_pending,
        "total": resolved + still_pending,
    }
