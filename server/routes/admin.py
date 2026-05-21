import json
import os
from fastapi import APIRouter, HTTPException
from ..db import execute_sql
from ..config import get_client, FEWSHOT_JOB_ID, FEEDBACK_TABLE, RESULTS_TABLE

router = APIRouter()


@router.get("/rules")
def get_classification_rules():
    """Retorna as regras de classificação usadas pelo modelo de extração.
    Permite ao usuário entender o racional por trás das classificações feitas
    pelo modelo e acompanhar mudanças de regras ao longo do tempo."""
    # O arquivo fica em model/regras_classificacao.json relativo à raiz do projeto
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base, "model", "regras_classificacao.json")
    try:
        with open(path) as f:
            rules = json.load(f)
        return {"rules": rules, "total": len(rules)}
    except FileNotFoundError:
        raise HTTPException(404, "Arquivo de regras não encontrado")
    except json.JSONDecodeError as e:
        raise HTTPException(500, f"Erro ao ler regras: {e}")


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
    """Análise read-only: das correções históricas em feedback_llm (acao='corrigido'),
    quantas o modelo atual já extrai corretamente (compara com extracted_json atual)?
    Útil para medir progresso do modelo. Não muda dados — feedback_llm é append-only."""
    corrections = execute_sql(f"""
        SELECT f.document_name, f.tipo_entidade, f.periodo, f.campo,
               f.valor_final, f.modelo_versao,
               r.extracted_json
        FROM {FEEDBACK_TABLE} f
        JOIN {RESULTS_TABLE} r
            ON f.document_name = r.document_name
            AND COALESCE(f.tipo_entidade, '') = COALESCE(r.tipo_entidade, '')
            AND COALESCE(f.periodo, '') = COALESCE(r.periodo, '')
        WHERE f.acao = 'corrigido'
    """)

    now_correct = 0
    still_wrong = 0
    by_modelo: dict = {}

    for row in corrections:
        campo = row["campo"]
        valor_final = row["valor_final"]
        extracted_json = row["extracted_json"]
        modelo_v = row.get("modelo_versao") or "unknown"

        try:
            data = json.loads(extracted_json) if isinstance(extracted_json, str) else extracted_json
        except (json.JSONDecodeError, TypeError):
            still_wrong += 1
            continue

        parts = campo.split(".")
        cur = data
        for p in parts:
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                cur = None
                break

        if cur is None:
            still_wrong += 1
            match = False
        else:
            try:
                current_val = float(cur)
                corrected_val = float(valor_final)
                match = abs(current_val - corrected_val) < 0.01 * max(abs(corrected_val), 1)
            except (ValueError, TypeError):
                match = str(cur).strip() == str(valor_final).strip()

            if match: now_correct += 1
            else:     still_wrong += 1

        bucket = by_modelo.setdefault(modelo_v, {"now_correct": 0, "still_wrong": 0})
        if match: bucket["now_correct"] += 1
        else:     bucket["still_wrong"] += 1

    total = now_correct + still_wrong
    return {
        "now_correct": now_correct,
        "still_wrong": still_wrong,
        "total": total,
        "pct_resolved_by_model": round(now_correct / total * 100, 1) if total > 0 else 0,
        "by_modelo_versao": [
            {"modelo_versao": k, **v, "total": v["now_correct"] + v["still_wrong"]}
            for k, v in sorted(by_modelo.items())
        ],
    }
