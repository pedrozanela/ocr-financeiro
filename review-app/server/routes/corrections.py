from fastapi import APIRouter
from pydantic import BaseModel
from ..db import execute_sql, execute_update
from ..config import CORRECTIONS_TABLE

router = APIRouter()


class Correction(BaseModel):
    document_name: str
    campo: str
    valor_extraido: str
    valor_correto: str
    comentario: str = ""


@router.get("/corrections/{document_name}")
def get_corrections(document_name: str):
    rows = execute_sql(
        f"""
        SELECT campo, valor_extraido, valor_correto, comentario,
               CAST(criado_em AS STRING) AS criado_em
        FROM {CORRECTIONS_TABLE}
        WHERE document_name = :name
        ORDER BY criado_em DESC
        """,
        [{"name": "name", "value": document_name}],
    )
    # Return as dict keyed by campo for easy lookup
    return {r["campo"]: r for r in rows}


@router.post("/corrections")
def save_correction(c: Correction):
    # Upsert: delete existing + insert new
    execute_update(
        f"DELETE FROM {CORRECTIONS_TABLE} WHERE document_name = :name AND campo = :campo",
        [{"name": "name", "value": c.document_name}, {"name": "campo", "value": c.campo}],
    )
    execute_update(
        f"""
        INSERT INTO {CORRECTIONS_TABLE} (document_name, campo, valor_extraido, valor_correto, comentario)
        VALUES (:name, :campo, :extraido, :correto, :comentario)
        """,
        [
            {"name": "name",       "value": c.document_name},
            {"name": "campo",      "value": c.campo},
            {"name": "extraido",   "value": c.valor_extraido},
            {"name": "correto",    "value": c.valor_correto},
            {"name": "comentario", "value": c.comentario},
        ],
    )
    return {"status": "ok"}


@router.delete("/corrections/{document_name}/{campo}")
def delete_correction(document_name: str, campo: str):
    execute_update(
        f"DELETE FROM {CORRECTIONS_TABLE} WHERE document_name = :name AND campo = :campo",
        [{"name": "name", "value": document_name}, {"name": "campo", "value": campo}],
    )
    return {"status": "ok"}
