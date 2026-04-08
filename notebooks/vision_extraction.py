# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin Vision OCR — Modo Performance
# MAGIC
# MAGIC Pipeline híbrido para produção:
# MAGIC 1. **PyMuPDF** — seleciona páginas financeiras por palavras-chave (sem custo)
# MAGIC 2. **Claude claude-sonnet-4-6 Vision** — extrai texto puro/tabelas das imagens (substitui `ai_parse_document`)
# MAGIC 3. **extrator-financeiro** — recebe o texto e faz o mapeamento para o schema
# MAGIC
# MAGIC Salva em `resultados` + sincroniza `resultados_final` (tabelas oficiais).

# COMMAND ----------

# MAGIC %pip install PyMuPDF --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json, time, base64, requests
import fitz       # PyMuPDF

# COMMAND ----------
# MAGIC %md ## 1. Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",      "")
dbutils.widgets.text("schema",       "ocr_financeiro")
dbutils.widgets.text("volume_path",  "")
dbutils.widgets.text("pdf_name",     "")
dbutils.widgets.text("dpi",          "144")
dbutils.widgets.text("extractor_endpoint", "extrator-financeiro")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH        = dbutils.widgets.get("volume_path") or f"/Volumes/{_cat}/{_sch}/documentos_pdf"
PDF_NAME           = dbutils.widgets.get("pdf_name").strip()
DPI                = int(dbutils.widgets.get("dpi"))
EXTRACTOR_ENDPOINT = dbutils.widgets.get("extractor_endpoint")

RESULTS_TABLE = f"{_cat}.{_sch}.resultados"
FINAL_TABLE   = f"{_cat}.{_sch}.resultados_final"
DOCS_TABLE    = f"{_cat}.{_sch}.documentos"

VISION_ENDPOINT  = "databricks-claude-sonnet-4-6"
MODELO_VERSAO    = f"vision-{VISION_ENDPOINT}"
WORKSPACE_URL    = spark.conf.get("spark.databricks.workspaceUrl")
if not WORKSPACE_URL.startswith("http"):
    WORKSPACE_URL = f"https://{WORKSPACE_URL}"
TOKEN            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
CURRENT_USER     = spark.sql("SELECT current_user()").collect()[0][0]
VISION_URL       = f"{WORKSPACE_URL}/serving-endpoints/{VISION_ENDPOINT}/invocations"
VISION_HEADERS   = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

EXTRACTOR_URL     = f"{WORKSPACE_URL}/serving-endpoints/{EXTRACTOR_ENDPOINT}/invocations"
EXTRACTOR_HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

PRICE_VISION_IN  = 3.0  / 1_000_000
PRICE_VISION_OUT = 15.0 / 1_000_000

if not PDF_NAME:
    dbutils.notebook.exit("pdf_name não informado")

print(f"Catalog          : {_cat}.{_sch}")
print(f"Volume           : {VOLUME_PATH}")
print(f"PDF              : {PDF_NAME}")
print(f"Vision endpoint  : {VISION_ENDPOINT}  |  DPI: {DPI}")
print(f"Extractor endpoint: {EXTRACTOR_ENDPOINT}")

# COMMAND ----------
# MAGIC %md ## 2. Seleção de páginas (PyMuPDF local)

# COMMAND ----------

PAGE_SIGNALS = {
    "bp_ativo": [
        ["total do ativo circulante"],
        ["total ativo circulante"],
        ["balanço patrimonial", "ativo circulante"],
        ["balancos patrimoniais", "ativo circulante"],
        ["balanço patrimonial", "circulante"],
        ["balancos patrimoniais", "circulante"],
        ["a t i v o", "circulante"],
        ["ativo", "não circulante", "imobilizado"],
        ["ativo", "nao circulante", "imobilizado"],
        ["total do ativo"],
        ["total do ativo"],
    ],
    "bp_passivo": [
        ["total do passivo circulante"],
        ["total passivo circulante"],
        ["total do patrimônio líquido"],
        ["total do patrimonio liquido"],
        ["total do passivo e patrimônio"],
        ["total do passivo e patrimonio"],
        # Balancete analítico (SPED): passivo não tem "total do passivo circulante" explícito
        ["p a s s i v o"],
        ["passivo", "circulante", "fornecedores"],
        ["passivo", "circulante", "obrigações"],
        ["passivo", "circulante", "obrigacoes"],
        ["patrimônio líquido", "capital"],
        ["patrimonio liquido", "capital"],
    ],
    "dre": [
        ["receita líquida",  "lucro bruto"],
        ["receita liquida",  "lucro bruto"],
        ["receita bruta",    "lucro bruto"],
    ],
    "oci": [
        ["total dos resultados abrangentes"],
        ["resultado abrangente", "lucro"],
    ],
}

MAX_PAGES_TO_SEND = 12


def select_financial_pages(doc: fitz.Document) -> tuple[list[int], str]:
    total_text = sum(len(doc[i].get_text("text")) for i in range(len(doc)))
    avg_text   = total_text / max(len(doc), 1)

    if avg_text < 50:
        print(f"    ⚠ PDF escaneado → fallback primeiras {MAX_PAGES_TO_SEND} páginas")
        return list(range(min(len(doc), MAX_PAGES_TO_SEND))), "fallback_all"

    selected = []
    for i in range(len(doc)):
        tl   = doc[i].get_text("text").lower()
        tags = [g for g, sigs in PAGE_SIGNALS.items()
                if any(all(t in tl for t in combo) for combo in sigs)]
        if tags:
            print(f"    Página {i+1:>3}: {' + '.join(t.upper() for t in tags)}")
            selected.append(i)

    if not selected:
        print(f"    ⚠ Nenhuma página identificada → fallback {MAX_PAGES_TO_SEND} páginas")
        return list(range(min(len(doc), MAX_PAGES_TO_SEND))), "fallback_all"

    expanded = set(selected)
    for i in selected:
        if i - 1 >= 0:       expanded.add(i - 1)
        if i + 1 < len(doc): expanded.add(i + 1)

    return sorted(expanded)[:MAX_PAGES_TO_SEND], "keyword_match"


# COMMAND ----------
# MAGIC %md ## 3. Funções: Vision OCR + extrator

# COMMAND ----------

OCR_SYSTEM_PROMPT = """Você é um sistema de OCR especializado em demonstrações financeiras brasileiras.
Sua única tarefa é transcrever com fidelidade absoluta o texto e as tabelas presentes nas imagens.

Regras:
- Transcreva o texto EXATAMENTE como aparece, sem interpretar, resumir ou classificar.
- Para tabelas financeiras, use formato HTML <table> preservando todas as linhas, colunas e cabeçalhos.
- Mantenha os cabeçalhos de coluna exatamente como estão (ex: "Controladora 31/12/2024", "Consolidado 31/12/2024").
- Preserve os rótulos de linha exatamente como estão (ex: "Caixa e equivalentes de caixa", "Contas a receber").
- Preserve os números exatamente, incluindo pontuação (ex: 1.827.197, (452.654)).
- Não omita nenhuma linha ou coluna da tabela.
- Não adicione interpretações, comentários ou campos extras.
- Retorne APENAS o texto transcrito, sem explicações."""


def call_vision_ocr(images: list[dict], pdf_name: str,
                    selected_indices: list[int]) -> tuple[str, int, int]:
    content = [
        {
            "type": "text",
            "text": (
                f"Transcreva o conteúdo das {len(images)} página(s) do documento '{pdf_name}' "
                f"(páginas {', '.join(str(i+1) for i in selected_indices)}). "
                "Foque nas demonstrações financeiras (Balanço Patrimonial e DRE)."
            )
        }
    ]
    for img in images:
        content.append({"type": "text", "text": f"\n--- Página {img['page_num']} ---\n"})
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{img['base64_data']}"}
        })

    payload = {
        "messages": [
            {"role": "system", "content": OCR_SYSTEM_PROMPT},
            {"role": "user",   "content": content},
        ],
        "max_tokens": 8192,
    }
    resp = requests.post(VISION_URL, headers=VISION_HEADERS, json=payload, timeout=300)
    resp.raise_for_status()
    body       = resp.json()
    text       = body["choices"][0]["message"]["content"].strip()
    usage      = body.get("usage", {})
    in_tokens  = usage.get("prompt_tokens",     0)
    out_tokens = usage.get("completion_tokens", 0)
    return text, in_tokens, out_tokens


MAX_PAYLOAD_CHARS = 300_000  # ~1.2 MB de texto; payload JSON fica bem abaixo dos 16 MB


def call_extractor(text: str) -> tuple[list, int, int]:
    if len(text) > MAX_PAYLOAD_CHARS:
        print(f"    ⚠ Texto muito grande ({len(text):,} chars) → truncando para {MAX_PAYLOAD_CHARS:,}")
        text = text[:MAX_PAYLOAD_CHARS]

    resp = requests.post(
        EXTRACTOR_URL, headers=EXTRACTOR_HEADERS,
        json={"dataframe_records": [{"text": text}]},
        timeout=600,
    )
    if not resp.ok:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text[:1000]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} — detalhe: {detail}",
            response=resp,
        )
    resp.raise_for_status()
    body     = resp.json()
    metadata = body.get("metadata", {})
    in_tok   = int(metadata.get("input_tokens",  0)) or len(text) // 4
    out_tok  = int(metadata.get("output_tokens", 0)) or 2000
    r = body.get("predictions", body)
    if isinstance(r, list) and len(r) == 1: r = r[0]
    if isinstance(r, str):  r = json.loads(r)
    if isinstance(r, dict): r = [r]
    return r, in_tok, out_tok


def get_nested(d, path):
    for k in path.split("."): d = (d or {}).get(k)
    return d


def pages_to_images(doc: fitz.Document, page_indices: list[int], dpi: int) -> list[dict]:
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    images = []
    for i in page_indices:
        pix       = doc[i].get_pixmap(matrix=mat, alpha=False)
        img_bytes = pix.tobytes("jpeg", jpg_quality=88)
        images.append({
            "page_num":    i + 1,
            "base64_data": base64.standard_b64encode(img_bytes).decode(),
            "size_kb":     round(len(img_bytes) / 1024, 1),
        })
    return images


def save_result(pdf_name: str, results: list, v_in: int, v_out: int):
    """Salva em `resultados` via MERGE — mesmo schema do batch_job."""
    if isinstance(results, dict):
        results = [results]

    def esc(v): return str(v or "").replace("'", "''")
    doc = pdf_name.replace("'", "''")

    for result in results:
        result     = dict(result)
        assessment = result.pop("_assessment", [])
        result.pop("_usage", None)

        usage_info = {"vision_in_tokens": v_in, "vision_out_tokens": v_out,
                      "vision_cost_usd": round(v_in * PRICE_VISION_IN + v_out * PRICE_VISION_OUT, 6)}

        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        uj   = json.dumps(usage_info, ensure_ascii=False).replace("'", "''")
        ej   = json.dumps(result,     ensure_ascii=False).replace("'", "''")

        rs   = esc(get_nested(result, "razao_social"))
        cnpj = esc(get_nested(result, "cnpj"))
        per  = esc(get_nested(result, "identificacao.periodo"))
        te   = esc(get_nested(result, "tipo_entidade"))
        td   = esc(get_nested(result, "identificacao.tipo_demonstrativo"))
        moe  = esc(get_nested(result, "identificacao.moeda"))
        escv = esc(get_nested(result, "identificacao.escala_valores"))
        mv   = MODELO_VERSAO.replace("'", "''")

        spark.sql(f"""
            MERGE INTO {RESULTS_TABLE} t
            USING (SELECT '{doc}' AS document_name,
                          '{te}'  AS tipo_entidade,
                          '{per}' AS periodo) s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                extracted_json     = '{ej}',
                assessment_json    = '{aj}',
                token_usage_json   = '{uj}',
                razao_social       = '{rs}',
                cnpj               = '{cnpj}',
                tipo_demonstrativo = '{td}',
                moeda              = '{moe}',
                escala_valores     = '{escv}',
                processado_em      = CURRENT_TIMESTAMP(),
                modelo_versao      = '{mv}'
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                 token_usage_json, razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores,
                 processado_em, modelo_versao)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}', '{aj}',
                 '{uj}', '{rs}', '{cnpj}', '{td}', '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{mv}')
        """)


# COMMAND ----------
# MAGIC %md ## 4. Processar PDF

# COMMAND ----------

t0_total = time.time()
success  = False
error_msg = ""

try:
    with open(f"{VOLUME_PATH}/{PDF_NAME}", "rb") as f:
        pdf_bytes = f.read()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    num_pages = len(doc)
    print(f"[{PDF_NAME}] {num_pages} páginas — selecionando...")

    selected, method = select_financial_pages(doc)
    page_nums_str    = ",".join(str(i + 1) for i in selected)
    print(f"[{PDF_NAME}] {method}: {len(selected)} páginas → {page_nums_str}")

    images   = pages_to_images(doc, selected, DPI)
    doc.close()
    total_kb = sum(img["size_kb"] for img in images)
    print(f"[{PDF_NAME}] {len(images)} imagens ({total_kb:.0f} KB) — Vision OCR...")

    t_vision    = time.time()
    vision_text, v_in, v_out = call_vision_ocr(images, PDF_NAME, selected)
    vision_s    = round(time.time() - t_vision, 1)
    cost        = round(v_in * PRICE_VISION_IN + v_out * PRICE_VISION_OUT, 6)
    print(f"[{PDF_NAME}] Vision: {vision_s}s | {v_in}/{v_out} tokens | ${cost:.4f}")
    print(f"[{PDF_NAME}] Texto: {len(vision_text)} chars — chamando extrator...")

    # Salvar texto OCR na tabela documentos (permite auditoria posterior)
    _doc_esc  = PDF_NAME.replace("'", "''")
    _text_esc = vision_text.replace("'", "''")
    spark.sql(f"""
        MERGE INTO {DOCS_TABLE} t
        USING (SELECT '{_doc_esc}' AS document_name) s ON t.document_name = s.document_name
        WHEN MATCHED THEN UPDATE SET document_text = '{_text_esc}',
            atualizado_em = CURRENT_TIMESTAMP(), atualizado_por = '{CURRENT_USER}'
        WHEN NOT MATCHED THEN INSERT (document_name, document_text, ingested_at, atualizado_em, atualizado_por)
            VALUES ('{_doc_esc}', '{_text_esc}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{CURRENT_USER}')
    """)

    t_ext = time.time()
    results, _, _ = call_extractor(vision_text)
    ext_s = round(time.time() - t_ext, 1)

    valid   = [r for r in results if not (isinstance(r, dict) and r.get("error"))]
    errored = [r for r in results if  isinstance(r, dict) and r.get("error")]
    if errored and not valid:
        raise ValueError(f"extrator falhou: {errored[0].get('raw','')[:200]}")

    save_result(PDF_NAME, valid, v_in, v_out)

    combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo"))
              for r in valid if isinstance(r, dict)]
    print(f"✓ [{PDF_NAME}] extrator: {ext_s}s | {len(valid)} registro(s): {combos}")
    success = True

except Exception as e:
    error_msg = str(e)[:500]
    print(f"✗ [{PDF_NAME}] ERRO: {e}")
    raise

# COMMAND ----------
# MAGIC %md ## 5. Sincronizar resultados_final

# COMMAND ----------

if success:
    doc_esc = PDF_NAME.replace("'", "''")
    spark.sql(f"""
        MERGE INTO {FINAL_TABLE} AS t
        USING (
            SELECT document_name, tipo_entidade, periodo, extracted_json,
                   razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores
            FROM {RESULTS_TABLE}
            WHERE document_name = '{doc_esc}'
        ) AS s
        ON  t.document_name = s.document_name
        AND COALESCE(t.tipo_entidade, '') = COALESCE(s.tipo_entidade, '')
        AND COALESCE(t.periodo, '')       = COALESCE(s.periodo, '')
        WHEN MATCHED AND COALESCE(t.atualizado_por, '') LIKE 'job:%' OR t.atualizado_por IS NULL THEN UPDATE SET
            extracted_json     = s.extracted_json,
            razao_social       = s.razao_social,
            cnpj               = s.cnpj,
            tipo_demonstrativo = s.tipo_demonstrativo,
            moeda              = s.moeda,
            escala_valores     = s.escala_valores,
            atualizado_em      = CURRENT_TIMESTAMP(),
            atualizado_por     = 'job:{CURRENT_USER}'
        WHEN NOT MATCHED THEN INSERT
            (document_name, tipo_entidade, periodo, extracted_json,
             razao_social, cnpj, tipo_demonstrativo, moeda, escala_valores,
             atualizado_em, atualizado_por)
        VALUES
            (s.document_name, s.tipo_entidade, s.periodo, s.extracted_json,
             s.razao_social, s.cnpj, s.tipo_demonstrativo, s.moeda, s.escala_valores,
             CURRENT_TIMESTAMP(), 'job:{CURRENT_USER}')
    """)
    print(f"✓ resultados_final sincronizado para {PDF_NAME}")

# COMMAND ----------
# MAGIC %md ## 6. Relatório

# COMMAND ----------

elapsed = round(time.time() - t0_total, 1)
print("=" * 60)
print(f"  PDF             : {PDF_NAME}")
print(f"  Status          : {'✓ OK' if success else '✗ ERRO'}")
print(f"  Tempo total     : {elapsed}s")
if success:
    print(f"  Vision tokens in: {v_in:,}  (${v_in * PRICE_VISION_IN:.4f})")
    print(f"  Vision tokens out:{v_out:,}  (${v_out * PRICE_VISION_OUT:.4f})")
    print(f"  Custo Vision    : ${cost:.4f}")
print("=" * 60)
dbutils.notebook.exit("ok" if success else f"error: {error_msg}")
