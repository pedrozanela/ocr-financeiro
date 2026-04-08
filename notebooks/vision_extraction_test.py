# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin Vision OCR — Claude Vision → extrator-financeiro (TESTE ISOLADO)
# MAGIC
# MAGIC Pipeline híbrido:
# MAGIC 1. **PyMuPDF** — seleciona páginas financeiras por palavras-chave (sem custo)
# MAGIC 2. **Claude claude-sonnet-4-6 Vision** — extrai texto puro/tabelas das imagens (substitui `ai_parse_document`)
# MAGIC 3. **extrator-financeiro** — recebe o texto e faz o mapeamento para o schema (igual ao pipeline atual)
# MAGIC
# MAGIC Vantagem: elimina o bug de label do `ai_parse_document` lendo direto da imagem.
# MAGIC
# MAGIC **⚠️ Completamente isolado:** salva apenas em `resultados_vision_test`.

# COMMAND ----------

# MAGIC %pip install PyMuPDF --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json, time, base64, re, requests
import fitz       # PyMuPDF
import threading

# COMMAND ----------
# MAGIC %md ## 1. Configuração

# COMMAND ----------

dbutils.widgets.text("catalog",      "pedro_zanela")
dbutils.widgets.text("schema",       "ocr_financeiro")
dbutils.widgets.text("volume_path",  "")
dbutils.widgets.text("output_table", "resultados_vision_test")
dbutils.widgets.text("pdf_filter",   "")
dbutils.widgets.text("dpi",          "144")
# Endpoint do extrator fine-tuned (igual ao batch_job)
dbutils.widgets.text("extractor_endpoint", "extrator-financeiro")

_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH        = dbutils.widgets.get("volume_path") or f"/Volumes/{_cat}/{_sch}/documentos_pdf"
OUTPUT_TABLE       = f"{_cat}.{_sch}.{dbutils.widgets.get('output_table')}"
PDF_FILTER         = dbutils.widgets.get("pdf_filter").strip()
DPI                = int(dbutils.widgets.get("dpi"))
EXTRACTOR_ENDPOINT = dbutils.widgets.get("extractor_endpoint")

# Endpoint Vision (Foundation Model nativo — sem chave externa)
VISION_ENDPOINT = "databricks-claude-sonnet-4-6"
WORKSPACE_URL   = spark.conf.get("spark.databricks.workspaceUrl")
if not WORKSPACE_URL.startswith("http"):
    WORKSPACE_URL = f"https://{WORKSPACE_URL}"
TOKEN            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
VISION_URL       = f"{WORKSPACE_URL}/serving-endpoints/{VISION_ENDPOINT}/invocations"
VISION_HEADERS   = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Endpoint extrator (fine-tuned, igual ao batch_job)
EXTRACTOR_URL     = f"{WORKSPACE_URL}/serving-endpoints/{EXTRACTOR_ENDPOINT}/invocations"
EXTRACTOR_HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

PRICE_VISION_IN  = 3.0  / 1_000_000
PRICE_VISION_OUT = 15.0 / 1_000_000

print(f"Catalog          : {_cat}.{_sch}")
print(f"Volume           : {VOLUME_PATH}")
print(f"Output table     : {OUTPUT_TABLE}")
print(f"Vision endpoint  : {VISION_ENDPOINT}  |  DPI: {DPI}")
print(f"Extractor endpoint: {EXTRACTOR_ENDPOINT}")
print(f"Filtro PDF       : {PDF_FILTER or '(todos)'}")

# COMMAND ----------
# MAGIC %md ## 2. Criar tabela de saída

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
    document_name     STRING,
    tipo_entidade     STRING,
    periodo           STRING,
    razao_social      STRING,
    cnpj              STRING,
    extracted_json    STRING   COMMENT 'JSON extraído pelo extrator-financeiro',
    assessment_json   STRING,
    vision_text       STRING   COMMENT 'Texto extraído pelo Claude Vision',
    page_numbers      STRING,
    pages_sent        INT,
    selection_method  STRING,
    vision_in_tokens  INT,
    vision_out_tokens INT,
    vision_cost_usd   DOUBLE,
    processado_em     TIMESTAMP,
    error_msg         STRING
)
USING DELTA
COMMENT 'Teste: Vision OCR + extrator-financeiro'
""")
print(f"✓ Tabela {OUTPUT_TABLE} pronta")

# COMMAND ----------
# MAGIC %md ## 3. Seleção de páginas (PyMuPDF local)

# COMMAND ----------

PAGE_SIGNALS = {
    "bp_ativo": [
        ["total do ativo circulante"],
        ["total ativo circulante"],
        ["balanço patrimonial", "ativo circulante"],
        ["balancos patrimoniais", "ativo circulante"],
    ],
    "bp_passivo": [
        ["total do passivo circulante"],
        ["total passivo circulante"],
        ["total do patrimônio líquido"],
        ["total do patrimonio liquido"],
        ["total do passivo e patrimônio"],
        ["total do passivo e patrimonio"],
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
# MAGIC %md ## 4. Funções: Vision OCR + extrator

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
    """Chama Claude Vision para extrair texto puro das páginas selecionadas."""
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
    body         = resp.json()
    text         = body["choices"][0]["message"]["content"].strip()
    usage        = body.get("usage", {})
    in_tokens    = usage.get("prompt_tokens",     0)
    out_tokens   = usage.get("completion_tokens", 0)
    return text, in_tokens, out_tokens


def call_extractor(text: str) -> tuple[list, int, int]:
    """Chama o endpoint extrator-financeiro com o texto extraído pelo Vision."""
    resp = requests.post(
        EXTRACTOR_URL, headers=EXTRACTOR_HEADERS,
        json={"dataframe_records": [{"text": text}]},
        timeout=600,
    )
    resp.raise_for_status()
    body          = resp.json()
    metadata      = body.get("metadata", {})
    in_tok        = int(metadata.get("input_tokens",  0)) or len(text) // 4
    out_tok       = int(metadata.get("output_tokens", 0)) or 2000
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


def save_results(pdf_name, results, vision_text, page_nums_str, pages_sent,
                 method, v_in, v_out):
    cost = round(v_in * PRICE_VISION_IN + v_out * PRICE_VISION_OUT, 6)

    for result in results:
        if not isinstance(result, dict): continue
        assessment = result.pop("_assessment", [])

        def esc(v): return str(v or "").replace("'", "''")

        te   = esc(get_nested(result, "tipo_entidade"))
        per  = esc(get_nested(result, "identificacao.periodo"))
        rs   = esc(get_nested(result, "razao_social"))
        cnpj = esc(get_nested(result, "cnpj"))
        ej   = json.dumps(result,     ensure_ascii=False).replace("'", "''")
        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        vt   = (vision_text or "").replace("'", "''")[:50000]
        doc  = esc(pdf_name)

        spark.sql(f"""
            MERGE INTO {OUTPUT_TABLE} t
            USING (SELECT '{doc}' AS document_name,
                          '{te}'  AS tipo_entidade,
                          '{per}' AS periodo) s
              ON  t.document_name = s.document_name
              AND t.tipo_entidade = s.tipo_entidade
              AND t.periodo       = s.periodo
            WHEN MATCHED THEN UPDATE SET
                razao_social      = '{rs}',
                cnpj              = '{cnpj}',
                extracted_json    = '{ej}',
                assessment_json   = '{aj}',
                vision_text       = '{vt}',
                page_numbers      = '{esc(page_nums_str)}',
                pages_sent        = {pages_sent},
                selection_method  = '{esc(method)}',
                vision_in_tokens  = {v_in},
                vision_out_tokens = {v_out},
                vision_cost_usd   = {cost},
                processado_em     = CURRENT_TIMESTAMP(),
                error_msg         = NULL
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, razao_social, cnpj,
                 extracted_json, assessment_json, vision_text,
                 page_numbers, pages_sent, selection_method,
                 vision_in_tokens, vision_out_tokens, vision_cost_usd, processado_em)
            VALUES
                ('{doc}', '{te}', '{per}', '{rs}', '{cnpj}',
                 '{ej}', '{aj}', '{vt}',
                 '{esc(page_nums_str)}', {pages_sent}, '{esc(method)}',
                 {v_in}, {v_out}, {cost}, CURRENT_TIMESTAMP())
        """)

# COMMAND ----------
# MAGIC %md ## 5. Identificar PDFs

# COMMAND ----------

all_pdfs   = sorted([f.name for f in dbutils.fs.ls(VOLUME_PATH)
                     if f.name.lower().endswith(".pdf")])
to_process = ([p for p in all_pdfs if PDF_FILTER.lower() in p.lower()]
              if PDF_FILTER else all_pdfs)

print(f"PDFs no volume : {len(all_pdfs)}")
print(f"A processar    : {len(to_process)}")
for name in to_process: print(f"  • {name}")
if not to_process: dbutils.notebook.exit("no_pdfs")

# COMMAND ----------
# MAGIC %md ## 6. Processar

# COMMAND ----------

print_lock = threading.Lock()
stats = []


def process_one(pdf_name: str) -> dict:
    t0 = time.time()
    stat = {"pdf": pdf_name, "status": "error", "records": 0,
            "pages_sent": 0, "v_in": 0, "v_out": 0, "cost": 0.0,
            "method": "", "error_msg": ""}
    try:
        # 1. Ler PDF
        with open(f"{VOLUME_PATH}/{pdf_name}", "rb") as f:
            pdf_bytes = f.read()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        num_pages = len(doc)

        with print_lock:
            print(f"[{pdf_name}] {num_pages} páginas — selecionando...")

        # 2. Selecionar páginas financeiras
        selected, method = select_financial_pages(doc)
        stat["method"]    = method
        stat["pages_sent"] = len(selected)
        page_nums_str = ",".join(str(i + 1) for i in selected)

        with print_lock:
            print(f"[{pdf_name}] {method}: {len(selected)} páginas → {page_nums_str}")

        # 3. Converter para imagens
        images    = pages_to_images(doc, selected, DPI)
        doc.close()
        total_kb  = sum(img["size_kb"] for img in images)

        with print_lock:
            print(f"[{pdf_name}] {len(images)} imagens ({total_kb:.0f} KB) — Vision OCR...")

        # 4. Vision OCR: imagens → texto
        t_vision = time.time()
        vision_text, v_in, v_out = call_vision_ocr(images, pdf_name, selected)
        vision_s = round(time.time() - t_vision, 1)
        stat["v_in"]  = v_in
        stat["v_out"] = v_out
        stat["cost"]  = round(v_in * PRICE_VISION_IN + v_out * PRICE_VISION_OUT, 6)

        with print_lock:
            print(f"[{pdf_name}] Vision: {vision_s}s | {v_in}/{v_out} tokens | ${stat['cost']:.4f}")
            print(f"[{pdf_name}] Texto extraído: {len(vision_text)} chars — chamando extrator...")

        # 5. Extrator fine-tuned: texto → JSON estruturado
        t_ext = time.time()
        results, _, _ = call_extractor(vision_text)
        ext_s = round(time.time() - t_ext, 1)

        valid   = [r for r in results if not (isinstance(r, dict) and r.get("error"))]
        errored = [r for r in results if  isinstance(r, dict) and r.get("error")]
        if errored and not valid:
            raise ValueError(f"extrator falhou: {errored[0].get('raw','')[:200]}")

        save_results(pdf_name, valid, vision_text, page_nums_str,
                     len(selected), method, v_in, v_out)

        stat["records"] = len(valid)
        stat["status"]  = "ok"
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo"))
                  for r in valid if isinstance(r, dict)]
        with print_lock:
            print(f"✓ [{pdf_name}] extrator: {ext_s}s | {len(valid)} registro(s): {combos}")

    except Exception as e:
        stat["error_msg"] = str(e)[:300]
        with print_lock:
            print(f"✗ [{pdf_name}] ERRO: {e}")

    stat["elapsed_s"] = round(time.time() - t0, 1)
    return stat


t0_total = time.time()
for pdf_name in to_process:
    stats.append(process_one(pdf_name))
    print()
total_elapsed = time.time() - t0_total

# COMMAND ----------
# MAGIC %md ## 7. Relatório

# COMMAND ----------

ok     = [s for s in stats if s["status"] == "ok"]
errors = [s for s in stats if s["status"] != "ok"]
total_in  = sum(s["v_in"]  for s in stats)
total_out = sum(s["v_out"] for s in stats)
total_cost = sum(s["cost"] for s in stats)

print("=" * 70)
print(f"  Vision endpoint : {VISION_ENDPOINT}")
print(f"  Extractor       : {EXTRACTOR_ENDPOINT}")
print(f"  Tempo total     : {int(total_elapsed//60)}m {int(total_elapsed%60)}s")
print(f"  Sucesso         : {len(ok)} / {len(to_process)}")
print(f"  Vision tokens in: {total_in:,}  (${total_in * PRICE_VISION_IN:.4f})")
print(f"  Vision tokens out:{total_out:,}  (${total_out * PRICE_VISION_OUT:.4f})")
print(f"  Custo Vision    : ${total_cost:.4f}")
print("=" * 70)
for s in stats:
    status_str = "✓" if s["status"] == "ok" else f"✗ {s['error_msg'][:40]}"
    print(f"  {s['pdf'][:45]:<45} {s['elapsed_s']:>5.1f}s  {status_str}")
if errors:
    for s in errors: print(f"  ✗ {s['pdf']}: {s['error_msg']}")

# COMMAND ----------
# MAGIC %md ## 8. Preview

# COMMAND ----------

display(spark.sql(f"""
    SELECT document_name, tipo_entidade, periodo, razao_social,
           selection_method, page_numbers, pages_sent,
           CAST(get_json_object(extracted_json, '$.ativo_total') AS DOUBLE)         AS ativo_total,
           CAST(get_json_object(extracted_json, '$.dre.lucro_liquido') AS DOUBLE)    AS lucro_liquido,
           CAST(get_json_object(extracted_json, '$.ativo_circulante.total_ativo_circulante') AS DOUBLE) AS total_ac,
           CAST(get_json_object(extracted_json, '$.passivo_circulante.total_passivo_circulante') AS DOUBLE) AS total_pc,
           vision_in_tokens, vision_out_tokens, vision_cost_usd, processado_em
    FROM {OUTPUT_TABLE}
    ORDER BY document_name, tipo_entidade, periodo DESC
"""))
