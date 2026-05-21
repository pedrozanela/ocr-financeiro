from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from ..db import execute_sql, execute_update
from ..config import RESULTS_TABLE, RESULTS_FINAL_TABLE, SOURCE_TABLE, REVISOES_TABLE, PDF_VOLUME_PATH, get_client
from .upload import _runs

router = APIRouter()


@router.get("/documents")
def list_documents():
    # One row per document (use ANY_VALUE for non-key columns; prefer CONSOLIDADO data)
    rows = execute_sql(f"""
        SELECT
            document_name,
            MAX(razao_social) AS razao_social,
            MAX(cnpj) AS cnpj,
            MAX(periodo) AS periodo,
            MAX((extracted_json->>'ativo_total')::numeric) AS ativo_total,
            MAX((extracted_json->'dre'->>'lucro_liquido')::numeric) AS lucro_liquido
        FROM {RESULTS_TABLE}
        GROUP BY document_name
        ORDER BY document_name
    """)
    return rows


@router.get("/documents/{document_name}")
def get_document(document_name: str):
    rows = execute_sql(
        f"""SELECT document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                CAST(processado_em AS STRING) AS processado_em, COALESCE(modelo_versao, '') AS modelo_versao,
                COALESCE(modo_extracao, '') AS modo_extracao
            FROM {RESULTS_TABLE}
            WHERE document_name = :name
            ORDER BY tipo_entidade, periodo DESC""",
        [{"name": "name", "value": document_name}],
    )
    if not rows:
        raise HTTPException(404, "Documento não encontrado")

    import json
    # Buscar status de finalização por (tipo_entidade, periodo)
    finals = execute_sql(
        f"""SELECT COALESCE(tipo_entidade,'') AS tipo_entidade,
                   COALESCE(periodo,'') AS periodo,
                   COALESCE(status, 'em_revisao') AS status,
                   CAST(finalizado_em AS STRING) AS finalizado_em,
                   COALESCE(finalizado_por, '') AS finalizado_por,
                   techfin_response
            FROM {RESULTS_FINAL_TABLE}
            WHERE document_name = :name""",
        [{"name": "name", "value": document_name}],
    )
    finals_map = {(f["tipo_entidade"], f["periodo"]): f for f in finals}

    records = []
    for row in rows:
        raw = row["extracted_json"]
        data = json.loads(raw) if isinstance(raw, str) else raw
        raw_assessment = row.get("assessment_json")
        assessment = json.loads(raw_assessment) if isinstance(raw_assessment, str) and raw_assessment else []
        te = row.get("tipo_entidade") or ""
        per = row.get("periodo") or ""
        f = finals_map.get((te, per), {})
        records.append({
            "tipo_entidade": row.get("tipo_entidade"),
            "periodo": row.get("periodo"),
            "data": data,
            "assessment": assessment,
            "processado_em": row.get("processado_em"),
            "modelo_versao": row.get("modelo_versao"),
            "modo_extracao": row.get("modo_extracao"),
            "status": f.get("status", "em_revisao"),
            "finalizado_em": f.get("finalizado_em") or "",
            "finalizado_por": f.get("finalizado_por") or "",
            "techfin_response": f.get("techfin_response"),
        })

    # Documento é considerado finalizado quando TODOS os registros estão finalizados
    overall_status = "finalizado" if records and all(r["status"] == "finalizado" for r in records) else "em_revisao"

    return {
        "document_name": document_name,
        "records": records,
        "status": overall_status,
        # backward-compat: expose first record's data at top level
        "data": records[0]["data"] if records else None,
    }


@router.get("/documents/{document_name}/reprocess-preview")
def reprocess_preview(document_name: str):
    """Retorna contagens para o modal de confirmacao do reprocess.

    - preservados: campos com status != 'llm' (corrigido + confirmado) -> mantidos
    - podem_mudar: total de campos extraidos - preservados -> podem ter valor diferente
    - total_campos: total de campos numericos no extracted_json mais recente
    """
    import json
    rows = execute_sql(
        f"""SELECT
              (SELECT COUNT(*) FROM {REVISOES_TABLE}
                WHERE document_name = :name AND status != 'llm') AS preservados,
              (SELECT MAX(extracted_json) FROM {RESULTS_TABLE}
                WHERE document_name = :name) AS extracted_json
        """,
        [{"name": "name", "value": document_name}],
    )
    if not rows:
        raise HTTPException(404, "Documento nao encontrado")
    row = rows[0]
    preservados = int(row.get("preservados") or 0)
    raw = row.get("extracted_json")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except Exception:
        data = {}

    # Conta campos numericos no JSON (recursivo, ignora identificacao/fontes/_postprocessed)
    def count_numeric_fields(obj, prefix=''):
        n = 0
        if not isinstance(obj, dict):
            return 0
        for k, v in obj.items():
            if k in ('fontes', '_postprocessed', 'identificacao'):
                continue
            if isinstance(v, dict):
                n += count_numeric_fields(v, f"{prefix}{k}.")
            elif isinstance(v, (int, float)):
                n += 1
        return n
    total_campos = count_numeric_fields(data)
    podem_mudar = max(0, total_campos - preservados)

    return {
        "document_name": document_name,
        "total_campos": total_campos,
        "preservados": preservados,
        "podem_mudar": podem_mudar,
    }


@router.post("/documents/{document_name}/reprocess")
def reprocess_document(document_name: str):
    """Reprocessa preservando revisoes (status != 'llm').

    Fluxo:
    1. Snapshot do extracted_json atual em revisoes_em_andamento como status='llm'
       com valor_anterior = valor atual (so para campos que ainda nao tem linha).
    2. Marca reprocessado_em = NOW() em todas linhas existentes do doc.
    3. DELETE resultados + resultados_final (o LLM vai re-extrair).
    4. Dispara batch_job.

    Pos-reprocess (quando job termina):
    - Linhas com status != 'llm': preservadas (valor_corrente intacto).
    - Linhas com status = 'llm' E valor_anterior IS NOT NULL: UI compara com novo
      resultados.extracted_json e mostra badge 'mudou' se diferente.
    """
    from .upload import _get_batch_job_id
    import json
    client = get_client()
    try:
        # 1. Snapshot dos valores atuais do extracted_json
        rows = execute_sql(
            f"""SELECT tipo_entidade, periodo, extracted_json FROM {RESULTS_TABLE}
                WHERE document_name = :name""",
            [{"name": "name", "value": document_name}],
        )

        def flatten(obj, prefix=''):
            """Achata extracted_json em pares (path, valor_numerico)."""
            out = {}
            if not isinstance(obj, dict):
                return out
            for k, v in obj.items():
                if k in ('fontes', '_postprocessed', 'identificacao'):
                    continue
                if isinstance(v, dict):
                    out.update(flatten(v, f"{prefix}{k}."))
                elif isinstance(v, (int, float)):
                    out[f"{prefix}{k}"] = float(v)
            return out

        for r in rows:
            te = r.get("tipo_entidade") or ""
            per = r.get("periodo") or ""
            raw = r.get("extracted_json")
            try:
                data = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                continue
            campos_atual = flatten(data)

            # Insere snapshot para campos llm puros (sem linha em revisoes)
            for campo, valor in campos_atual.items():
                execute_update(
                    f"""INSERT INTO {REVISOES_TABLE}
                          (document_name, campo, tipo_entidade, periodo,
                           valor_extraido, valor_corrente, status,
                           valor_anterior, reprocessado_em, criado_em)
                        VALUES (:name, :campo, :te, :per, :v, :v, 'llm',
                                :v, NOW(), NOW())
                        ON CONFLICT (document_name, campo, tipo_entidade, periodo)
                          DO UPDATE SET reprocessado_em = NOW()
                    """,
                    [
                        {"name": "name",  "value": document_name},
                        {"name": "campo", "value": campo},
                        {"name": "te",    "value": te},
                        {"name": "per",   "value": per},
                        {"name": "v",     "value": valor},
                    ],
                )

        # 2. DELETE resultados + resultados_final (LLM vai re-extrair)
        execute_update(
            f"DELETE FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        execute_update(
            f"DELETE FROM {RESULTS_FINAL_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        # NAO deleta REVISOES_TABLE — preserva revisoes do analista

        # 3. Dispara batch_job
        job_id = _get_batch_job_id(client)
        run = client.jobs.run_now(job_id=job_id, notebook_params={"pdf_name": document_name})
        _runs[document_name] = run.run_id
        return {"document_name": document_name, "status": "processing", "run_id": run.run_id}
    except Exception as e:
        raise HTTPException(500, f"Erro ao disparar job: {e}")


@router.delete("/documents/{document_name}")
def delete_document(document_name: str):
    client = get_client()
    deleted_tables = []
    # 1. Remove from resultados
    try:
        execute_update(
            f"DELETE FROM {RESULTS_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        deleted_tables.append("resultados")
    except Exception as e:
        print(f"[delete] WARNING resultados: {e}")
    # 2. Remove from documentos (source)
    try:
        execute_update(
            f"DELETE FROM {SOURCE_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        deleted_tables.append("documentos")
    except Exception as e:
        print(f"[delete] WARNING documentos: {e}")
    # 3. Remove from revisoes_em_andamento (estado transitorio)
    try:
        execute_update(
            f"DELETE FROM {REVISOES_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        deleted_tables.append("revisoes_em_andamento")
    except Exception as e:
        print(f"[delete] WARNING revisoes: {e}")
    # Nota: feedback_llm e correcoes_legado nao sao deletados — sao audit logs append-only.
    # 4. Remove from resultados_final
    try:
        execute_update(
            f"DELETE FROM {RESULTS_FINAL_TABLE} WHERE document_name = :name",
            [{"name": "name", "value": document_name}],
        )
        deleted_tables.append("resultados_final")
    except Exception as e:
        print(f"[delete] WARNING resultados_final: {e}")
    # 5. Remove PDF from volume
    fname = document_name if document_name.endswith(".pdf") else f"{document_name}.pdf"
    try:
        client.files.delete(f"{PDF_VOLUME_PATH}/{fname}")
        deleted_tables.append("pdf")
    except Exception as e:
        print(f"[delete] WARNING pdf: {e}")
    return {"document_name": document_name, "deleted_from": deleted_tables}


@router.get("/documents/{document_name}/ocr-text")
def get_document_ocr_text(document_name: str):
    rows = execute_sql(
        f"SELECT document_text, atualizado_em, atualizado_por FROM {SOURCE_TABLE} WHERE document_name = :name LIMIT 1",
        [{"name": "name", "value": document_name}],
    )
    if not rows or not rows[0].get("document_text"):
        raise HTTPException(404, "Texto OCR não disponível para este documento")
    return {
        "document_text": rows[0]["document_text"],
        "atualizado_em": str(rows[0].get("atualizado_em") or ""),
        "atualizado_por": rows[0].get("atualizado_por") or "",
    }


@router.get("/documents/{document_name}/pdf")
def get_document_pdf(document_name: str):
    client = get_client()
    # Garantir extensão (case-insensitive). Tenta o nome como veio, depois variantes.
    base_name = document_name
    candidates = []
    if base_name.lower().endswith(".pdf"):
        # Já tem extensão — testa exatamente como veio
        candidates.append(base_name)
        # Variantes de capitalização (.PDF, .pdf, .Pdf)
        stem = base_name[:-4]
        for ext in (".PDF", ".pdf", ".Pdf"):
            cand = stem + ext
            if cand not in candidates:
                candidates.append(cand)
    else:
        # Sem extensão — adiciona ambas
        candidates.append(f"{base_name}.pdf")
        candidates.append(f"{base_name}.PDF")

    last_err = None
    for fname in candidates:
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
            last_err = e
    raise HTTPException(404, f"PDF não encontrado: {last_err}")


# ─── Sidebar state ───────────────────────────────────────────────────────────

@router.get("/documentos/sidebar-state")
def sidebar_state():
    """Estado consolidado para a sidebar:
      - Lista de documentos com status (nao_revisado / em_revisao / submetido).
      - Contagem de revisões já tocadas e total de campos extraídos no JSON do LLM.
      - Timestamp version pra polling (MAX dos timestamps relevantes).
    """
    rows = execute_sql(f"""
        WITH doc_info AS (
            SELECT d.document_name,
                   d.ingested_at,
                   MAX(r.razao_social) AS razao_social,
                   MAX(r.cnpj)         AS cnpj
            FROM {SOURCE_TABLE} d
            LEFT JOIN {RESULTS_TABLE} r ON r.document_name = d.document_name
            GROUP BY d.document_name, d.ingested_at
        ),
        revisoes_count AS (
            SELECT document_name,
                   SUM(CASE WHEN status = 'llm' THEN 1 ELSE 0 END)  AS llm_count,
                   SUM(CASE WHEN status != 'llm' THEN 1 ELSE 0 END) AS revisado_count
            FROM {REVISOES_TABLE}
            GROUP BY document_name
        ),
        finalizados AS (
            SELECT document_name,
                   MAX(finalizado_em) AS finalizado_em,
                   MAX(finalizado_por) AS finalizado_por
            FROM {RESULTS_FINAL_TABLE}
            WHERE status = 'finalizado'
            GROUP BY document_name
        )
        SELECT d.document_name,
               d.razao_social,
               d.cnpj,
               CAST(d.ingested_at AS STRING) AS ingested_at,
               COALESCE(rc.revisado_count, 0) AS revisado_count,
               CAST(f.finalizado_em AS STRING) AS submetido_em,
               COALESCE(f.finalizado_por, '') AS finalizado_por,
               CASE
                 WHEN f.finalizado_em IS NOT NULL AND COALESCE(rc.revisado_count, 0) = 0 THEN 'submetido'
                 WHEN COALESCE(rc.revisado_count, 0) > 0 THEN 'em_revisao'
                 ELSE 'nao_revisado'
               END AS status
        FROM doc_info d
        LEFT JOIN revisoes_count rc ON rc.document_name = d.document_name
        LEFT JOIN finalizados f     ON f.document_name = d.document_name
        ORDER BY d.ingested_at DESC NULLS LAST
    """)

    # Version: MAX dos timestamps de mutação. Se nada existe ainda, fica '1970-01-01'.
    # Usa ::text direto (CAST AS STRING não funciona com GREATEST aninhado pelo regex do db.py)
    version_rows = execute_sql(f"""
        SELECT (GREATEST(
            COALESCE((SELECT MAX(ingested_at)   FROM {SOURCE_TABLE}),        '1970-01-01'::timestamptz),
            COALESCE((SELECT MAX(revisado_em)   FROM {REVISOES_TABLE}),      '1970-01-01'::timestamptz),
            COALESCE((SELECT MAX(criado_em)     FROM {REVISOES_TABLE}),      '1970-01-01'::timestamptz),
            COALESCE((SELECT MAX(atualizado_em) FROM {RESULTS_FINAL_TABLE}), '1970-01-01'::timestamptz),
            COALESCE((SELECT MAX(finalizado_em) FROM {RESULTS_FINAL_TABLE}), '1970-01-01'::timestamptz)
        ))::text AS version
    """)
    version = version_rows[0]["version"] if version_rows else ""

    return {"documentos": rows, "version": version}
