# Databricks notebook source
# MAGIC %md
# MAGIC # TechFin OCR — Batch Job
# MAGIC Detecta PDFs novos no volume UC, extrai texto via `ai_parse_document`,
# MAGIC chama o endpoint OCR e salva em `resultados`. A tabela `resultados_final`
# MAGIC NAO e gravada por este job — apenas pelo botao "Submeter" no app.

# COMMAND ----------

# MAGIC %pip install pikepdf psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
import requests
from pyspark.sql.functions import expr, concat_ws

# Configuração via widgets (compatível com DABs e Serverless)
dbutils.widgets.text("catalog",       "")
dbutils.widgets.text("schema",        "ocr_financeiro")
dbutils.widgets.text("volume_path",   "")
dbutils.widgets.text("endpoint",      "extrator-financeiro")
dbutils.widgets.text("pdf_name",      "")
dbutils.widgets.text("lakebase_host", "")
dbutils.widgets.text("lakebase_db",   "ocr_financeiro")
dbutils.widgets.text("lakebase_project", "ocr-financeiro")
_cat = dbutils.widgets.get("catalog")
_sch = dbutils.widgets.get("schema")

VOLUME_PATH     = dbutils.widgets.get("volume_path") or f"/Volumes/{_cat}/{_sch}/documentos_pdf"
PDF_NAME_FILTER = dbutils.widgets.get("pdf_name").strip()
RESULTS_TABLE   = f"{_cat}.{_sch}.resultados"
FINAL_TABLE     = f"{_cat}.{_sch}.resultados_final"
SOURCE_TABLE    = f"{_cat}.{_sch}.documentos"
OCR_ENDPOINT    = dbutils.widgets.get("endpoint")
LAKEBASE_HOST   = dbutils.widgets.get("lakebase_host").strip()
LAKEBASE_DB     = dbutils.widgets.get("lakebase_db").strip()
LAKEBASE_PROJECT = dbutils.widgets.get("lakebase_project").strip()

DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if not DATABRICKS_HOST.startswith("http"):
    DATABRICKS_HOST = f"https://{DATABRICKS_HOST}"
ENDPOINT_URL = f"{DATABRICKS_HOST}/serving-endpoints/{OCR_ENDPOINT}/invocations"

PRICE_INPUT_PER_TOKEN  = 3.00 / 1_000_000
PRICE_OUTPUT_PER_TOKEN = 15.00 / 1_000_000

TOKEN        = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
HEADERS      = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]

print(f"Catalog  : {_cat}.{_sch}")
print(f"Volume   : {VOLUME_PATH}")
print(f"Endpoint : {OCR_ENDPOINT}")
print(f"Lakebase : {LAKEBASE_HOST or 'disabled (Delta only)'}")

# ── Lakebase connection helper ────────────────────────────────────────────────
_pg_conn = None

def _get_pg():
    """Get or create a psycopg2 connection to Lakebase."""
    global _pg_conn
    if not LAKEBASE_HOST:
        return None
    if _pg_conn and not _pg_conn.closed:
        try:
            with _pg_conn.cursor() as c:
                c.execute("SELECT 1")
            return _pg_conn
        except Exception:
            _pg_conn = None

    import psycopg2
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/production/endpoints/primary"
    if hasattr(w, "postgres"):
        cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
        token = cred.token
    else:
        resp = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
        token = resp.get("token", "")
    _pg_conn = psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
                                user=CURRENT_USER, password=token, sslmode="require")
    _pg_conn.autocommit = True
    return _pg_conn


def pg_upsert_resultado(doc, te, per, ej, aj, uj, rs, cnpj, td, tdoc, nm, moe, escv, modo):
    """Upsert into Lakebase resultados."""
    conn = _get_pg()
    if not conn:
        return
    from psycopg2.extras import Json as PgJson
    def _to_int(v):
        if v in (None, "", "NULL"):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO resultados (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                token_usage_json, razao_social, cnpj, tipo_demonstrativo, tipo_documento, numeroMeses,
                moeda, escala_valores,
                processado_em, modelo_versao, modo_extracao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (document_name, tipo_entidade, periodo) DO UPDATE SET
                extracted_json = EXCLUDED.extracted_json,
                assessment_json = EXCLUDED.assessment_json,
                token_usage_json = EXCLUDED.token_usage_json,
                razao_social = EXCLUDED.razao_social,
                cnpj = EXCLUDED.cnpj,
                tipo_demonstrativo = EXCLUDED.tipo_demonstrativo,
                tipo_documento = EXCLUDED.tipo_documento,
                numeroMeses = EXCLUDED.numeroMeses,
                moeda = EXCLUDED.moeda,
                escala_valores = EXCLUDED.escala_valores,
                processado_em = NOW(),
                modelo_versao = EXCLUDED.modelo_versao,
                modo_extracao = EXCLUDED.modo_extracao
        """, (doc, te, per, PgJson(json.loads(ej.replace("''", "'"))),
              PgJson(json.loads(aj.replace("''", "'"))),
              PgJson(json.loads(uj.replace("''", "'"))),
              rs, cnpj, _to_int(td), _to_int(tdoc), _to_int(nm),
              moe, escv, OCR_ENDPOINT, modo))


def pg_upsert_documento(doc, text):
    """Upsert into Lakebase documentos."""
    conn = _get_pg()
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO documentos (document_name, document_text, ingested_at, atualizado_em, atualizado_por)
            VALUES (%s, %s, NOW(), NOW(), %s)
            ON CONFLICT (document_name) DO UPDATE SET
                document_text = EXCLUDED.document_text,
                atualizado_em = NOW(),
                atualizado_por = EXCLUDED.atualizado_por
        """, (doc, text, CURRENT_USER))


# COMMAND ----------
# MAGIC %md ## 1. Identificar PDFs novos

# COMMAND ----------

volume_files = dbutils.fs.ls(VOLUME_PATH)
volume_pdfs  = {f.name for f in volume_files if f.name.lower().endswith(".pdf")}
processed    = {row["document_name"] for row in spark.sql(f"SELECT document_name FROM {RESULTS_TABLE}").collect()}

if PDF_NAME_FILTER:
    # Modo single: processar apenas o PDF especificado (ex: disparado pelo upload da UI)
    new_pdfs = [PDF_NAME_FILTER] if PDF_NAME_FILTER in volume_pdfs else []
    print(f"Modo single: {PDF_NAME_FILTER}")
else:
    # Modo batch: processar todos os PDFs novos do volume
    new_pdfs = sorted(volume_pdfs - processed)

print(f"Volume: {len(volume_pdfs)} PDFs | Já processados: {len(processed)} | A processar: {len(new_pdfs)}")
for name in new_pdfs:
    print(f"  • {name}")

if not new_pdfs:
    print("Nenhum PDF novo. Encerrando.")
    dbutils.notebook.exit("no_new_pdfs")

# COMMAND ----------
# MAGIC %md ## 2. Funções

# COMMAND ----------

def extract_text_ai_parse(pdf_name: str) -> tuple[str, int, bool]:
    """Extrai texto + nº de páginas via ai_parse_document.
    Retorna (text, num_pages, has_conversion_error).
    has_conversion_error indica que ai_parse retornou error_status — típico de PDF
    criptografado ou com restrições de extração."""
    volume_path = f"{VOLUME_PATH}/{pdf_name}"
    df = (
        spark.read.format("binaryFile").load(volume_path)
        .withColumn("parsed", expr("ai_parse_document(content)"))
        .withColumn("num_pages", expr("size(try_cast(parsed:document:pages AS ARRAY<VARIANT>))"))
        .withColumn("has_error", expr("size(try_cast(parsed:error_status AS ARRAY<VARIANT>)) > 0"))
        .withColumn("text", concat_ws("\n\n", expr("""
            transform(
                try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                element -> try_cast(element:content AS STRING)
            )
        """)))
        .select("text", "num_pages", "has_error")
    )
    rows = df.collect()
    if not rows:
        return "", 0, False
    return rows[0]["text"] or "", int(rows[0]["num_pages"] or 0), bool(rows[0]["has_error"])


# Markers (PT + EN) para identificar seções financeiras no texto do ai_parse.
# Usado quando o texto é muito grande para caber no contexto do extrator.
_FINANCIAL_SECTION_MARKERS = [
    # Balanço Patrimonial
    "BALANÇO PATRIMONIAL", "BALANCO PATRIMONIAL", "Balanço Patrimonial", "Balanco Patrimonial",
    "Balanços Patrimoniais", "Balancos Patrimoniais",
    "BALANCE SHEET", "Balance Sheet", "Balance sheet",
    "STATEMENT OF FINANCIAL POSITION", "Statement of Financial Position",
    "ATIVO CIRCULANTE", "Ativo Circulante", "Ativo circulante",
    "CURRENT ASSETS", "Current assets", "Current Assets",
    # DRE
    "DEMONSTRAÇÃO DO RESULTADO", "DEMONSTRACAO DO RESULTADO",
    "Demonstração do Resultado", "Demonstracao do Resultado",
    "Demonstrações do Resultado", "Demonstracoes do Resultado",
    "INCOME STATEMENT", "Income Statement", "Income statement",
    "STATEMENT OF INCOME", "Statement of Income",
    "PROFIT AND LOSS", "Profit and Loss", "Profit & Loss",
    "STATEMENT OF OPERATIONS", "Statement of Operations",
    "RECEITA OPERACIONAL", "Receita Operacional",
    "LUCRO BRUTO", "Lucro Bruto", "GROSS PROFIT", "Gross Profit",
]


MAX_TEXT_FOR_EXTRACTOR = 300_000  # Limite prático antes de filtrar ou fallback

def filter_financial_sections(text: str) -> str:
    """Extrai seções financeiras do texto quando muito grande para o extrator.

    Procura markers de BP/DRE (PT + EN). Se encontrar, retorna o trecho desde
    o primeiro marker até o último + buffer — tipicamente reduz 1M chars para
    ~100k chars do conteúdo relevante. Se não encontrar markers, retorna o
    texto original (caller decide se trunca ou faz fallback).
    """
    if len(text) <= MAX_TEXT_FOR_EXTRACTOR:
        return text

    text_upper = text.upper()
    # Encontra todas as posições dos markers
    positions = []
    for marker in _FINANCIAL_SECTION_MARKERS:
        m_upper = marker.upper()
        start = 0
        while True:
            pos = text_upper.find(m_upper, start)
            if pos < 0:
                break
            positions.append(pos)
            start = pos + len(m_upper)

    if not positions:
        # Nenhum marker — retorna texto original, caller trata
        return text

    # Pega do primeiro marker até o último + buffer, limitado ao tamanho máximo
    positions.sort()
    first = max(0, positions[0] - 2000)  # buffer para contexto antes do BP
    last = min(len(text), positions[-1] + 30000)  # buffer generoso após último marker
    filtered = text[first:last]

    # Se ainda grande demais, trunca mas mantém começo (onde tá BP/DRE)
    if len(filtered) > MAX_TEXT_FOR_EXTRACTOR:
        filtered = filtered[:MAX_TEXT_FOR_EXTRACTOR]

    return filtered


def decrypt_pdf_in_volume(pdf_name: str) -> bool:
    """Remove a criptografia/restrições do PDF no volume. Sobrescreve o arquivo.
    Retorna True se conseguiu decrypt e gravar."""
    import pikepdf
    volume_path = f"/Volumes/{_cat}/{_sch}/documentos_pdf/{pdf_name}"
    try:
        with pikepdf.open(volume_path) as pdf:
            # pikepdf remove encryption ao salvar sem argumentos de encryption
            pdf.save(volume_path + ".decrypted.tmp")
        # Rename atomically
        import shutil
        shutil.move(volume_path + ".decrypted.tmp", volume_path)
        return True
    except Exception as e:
        print(f"    [decrypt] falhou: {e}")
        return False


def run_vision_fallback(pdf_name: str) -> bool:
    """Dispara vision_extraction notebook para este PDF. Ele salva direto
    em documentos + resultados, sem passar pelo fluxo do batch_job.
    Retorna True se o notebook rodou com sucesso."""
    try:
        import os
        # Resolve vision_extraction path (mesmo diretório deste notebook)
        vision_path = "./vision_extraction"
        result = dbutils.notebook.run(
            vision_path,
            timeout_seconds=1200,
            arguments={
                "pdf_name": pdf_name,
                "catalog": _cat,
                "schema": _sch,
                "extractor_endpoint": OCR_ENDPOINT,
                "volume_path": VOLUME_PATH,
            },
        )
        print(f"    [vision fallback] OK — resultado: {result}")
        return True
    except Exception as e:
        print(f"    [vision fallback] falhou: {e}")
        return False


def call_endpoint(text: str, max_retries: int = 3) -> tuple[list, int, int]:
    """Chama endpoint OCR. Retorna (resultados, input_tokens, output_tokens)."""
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT_URL, headers=HEADERS,
                                 json={"dataframe_records": [{"text": text}]},
                                 timeout=600)
            if resp.status_code in (429, 503, 504):
                wait = 60 * (attempt + 1)
                print(f"    HTTP {resp.status_code} — retry {attempt+1}/{max_retries} em {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            body = resp.json()
            metadata      = body.get("metadata", {})
            input_tokens  = int(metadata.get("input_tokens",  0)) or len(text) // 4
            output_tokens = int(metadata.get("output_tokens", 0)) or 2000
            r = body.get("predictions", body)
            if isinstance(r, list) and len(r) == 1:
                r = r[0]
            if isinstance(r, str):
                r = json.loads(r)
            if isinstance(r, dict):
                r = [r]
            return r, input_tokens, output_tokens
        except requests.exceptions.Timeout:
            wait = 60 * (attempt + 1)
            print(f"    Timeout — retry {attempt+1}/{max_retries} em {wait}s")
            time.sleep(wait)
    raise Exception(f"Endpoint falhou após {max_retries} tentativas")


def get_nested(d: dict, path: str):
    for k in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def save_result(pdf_name: str, results: list, modo_extracao: str = "ai_parse"):
    """Salva resultados em `resultados` via MERGE com todas as colunas.
    modo_extracao: 'ai_parse' (fluxo padrão via ai_parse_document) ou 'vision'
    (via Vision OCR, seja pelo modo performance ou fallback)."""
    if isinstance(results, dict):
        results = [results]

    def esc(v): return str(v or "").replace("'", "''")
    doc = pdf_name.replace("'", "''")
    modo = modo_extracao.replace("'", "''")

    for result in results:
        result = dict(result)
        assessment = result.pop("_assessment", [])
        usage      = result.pop("_usage", {})

        aj   = json.dumps(assessment, ensure_ascii=False).replace("'", "''")
        uj   = json.dumps(usage,      ensure_ascii=False).replace("'", "''")
        ej   = json.dumps(result,     ensure_ascii=False).replace("'", "''")

        rs   = esc(get_nested(result, "razao_social"))
        cnpj = esc(get_nested(result, "cnpj"))
        per  = esc(get_nested(result, "identificacao.periodo"))
        te   = esc(get_nested(result, "tipo_entidade"))
        # Enums: int (NULL no SQL se ausente)
        def _int_or_null(v):
            try:
                return str(int(v)) if v not in (None, "") else "NULL"
            except (ValueError, TypeError):
                return "NULL"
        td   = _int_or_null(get_nested(result, "identificacao.tipo_demonstrativo"))
        tdoc = _int_or_null(get_nested(result, "identificacao.tipo_documento"))
        nm   = _int_or_null(get_nested(result, "identificacao.numeroMeses"))
        moe  = esc(get_nested(result, "identificacao.moeda"))
        escv = esc(get_nested(result, "identificacao.escala_valores"))

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
                tipo_demonstrativo = {td},
                tipo_documento     = {tdoc},
                numeroMeses        = {nm},
                moeda              = '{moe}',
                escala_valores     = '{escv}',
                processado_em      = CURRENT_TIMESTAMP(),
                modelo_versao      = '{OCR_ENDPOINT}',
                modo_extracao      = '{modo}'
            WHEN NOT MATCHED THEN INSERT
                (document_name, tipo_entidade, periodo, extracted_json, assessment_json,
                 token_usage_json, razao_social, cnpj, tipo_demonstrativo, tipo_documento,
                 numeroMeses, moeda, escala_valores,
                 processado_em, modelo_versao, modo_extracao)
            VALUES
                ('{doc}', '{te}', '{per}', '{ej}', '{aj}',
                 '{uj}', '{rs}', '{cnpj}', {td}, {tdoc}, {nm}, '{moe}', '{escv}',
                 CURRENT_TIMESTAMP(), '{OCR_ENDPOINT}', '{modo}')
        """)
        pg_upsert_resultado(doc, te, per, ej, aj, uj, rs, cnpj, td, tdoc, nm, moe, escv, modo)

# COMMAND ----------
# MAGIC %md ## 3. Processar PDFs novos (paralelo)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

MAX_WORKERS = 4
print_lock  = threading.Lock()

def process_one(pdf_name: str) -> dict:
    """Processa um PDF completo. Thread-safe."""
    doc_start = time.time()
    doc_stat  = {
        "pdf": pdf_name, "status": "error",
        "time_parse_s": 0.0, "time_llm_s": 0.0,
        "input_tokens": 0, "output_tokens": 0,
        "cost_usd": 0.0, "records": 0, "error_msg": "",
    }

    try:
        # ai_parse_document
        t0 = time.time()
        text, num_pages, has_error = extract_text_ai_parse(pdf_name)
        doc_stat["time_parse_s"] = round(time.time() - t0, 1)
        with print_lock:
            print(f"  [{pdf_name}] ai_parse: {num_pages} pág em {doc_stat['time_parse_s']}s")

        # Fallback 1: ai_parse retornou texto vazio ou erro de conversão → tentar decrypt + retry
        if (not text or not text.strip() or has_error):
            with print_lock:
                print(f"  [{pdf_name}] ai_parse falhou (texto vazio ou conversion error) — tentando decrypt...")
            if decrypt_pdf_in_volume(pdf_name):
                with print_lock:
                    print(f"  [{pdf_name}] decrypt OK, retentando ai_parse...")
                text, num_pages, has_error = extract_text_ai_parse(pdf_name)
                with print_lock:
                    print(f"  [{pdf_name}] ai_parse pós-decrypt: {num_pages} pág | has_error={has_error}")

        # Fallback 2: ainda falhou → usar Vision OCR (vision_extraction notebook)
        if (not text or not text.strip() or has_error):
            with print_lock:
                print(f"  [{pdf_name}] fallback para Vision OCR...")
            if run_vision_fallback(pdf_name):
                # vision_extraction salva direto em resultados + documentos — sucesso
                doc_stat["status"]    = "success"
                doc_stat["error_msg"] = "via_vision_ocr"
                doc_stat["time_parse_s"] = round(time.time() - doc_start, 1)
                return doc_stat
            doc_stat["error_msg"] = "no_text_after_fallbacks"
            with print_lock:
                print(f"  [{pdf_name}] ⚠ Sem texto mesmo após decrypt + Vision OCR")
            return doc_stat

        # Salvar texto na tabela documentos
        text_esc = text.replace("'", "''")
        doc_esc  = pdf_name.replace("'", "''")
        spark.sql(f"""
            MERGE INTO {SOURCE_TABLE} t
            USING (SELECT '{doc_esc}' AS document_name) s ON t.document_name = s.document_name
            WHEN MATCHED THEN UPDATE SET document_text = '{text_esc}',
                atualizado_em = CURRENT_TIMESTAMP(), atualizado_por = '{CURRENT_USER}'
            WHEN NOT MATCHED THEN INSERT (document_name, document_text, ingested_at, atualizado_em, atualizado_por)
                VALUES ('{doc_esc}', '{text_esc}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{CURRENT_USER}')
        """)
        pg_upsert_documento(pdf_name, text)

        # Filtro de seções financeiras: se o texto do ai_parse é muito grande,
        # tenta extrair apenas as seções de BP/DRE via markers PT/EN.
        # Evita estourar o contexto do Claude em PDFs grandes (ex: relatório anual 250+ pág).
        text_for_endpoint = text
        if len(text) > MAX_TEXT_FOR_EXTRACTOR:
            filtered = filter_financial_sections(text)
            if filtered is not text and len(filtered) < len(text):
                with print_lock:
                    print(f"  [{pdf_name}] texto grande ({len(text):,} chars) — filtrado para {len(filtered):,} chars via markers financeiros")
                text_for_endpoint = filtered

        # Endpoint OCR
        t0 = time.time()
        try:
            results, input_tokens, output_tokens = call_endpoint(text_for_endpoint)
        except Exception as e:
            # Fallback 3a: chamada ao endpoint falhou por completo (ex: texto muito
            # grande estourando o context do Claude, timeout, etc). Tenta Vision OCR
            # que filtra apenas as páginas financeiras do PDF.
            with print_lock:
                print(f"  [{pdf_name}] endpoint falhou ({type(e).__name__}: {str(e)[:120]}) — Vision OCR...")
            if run_vision_fallback(pdf_name):
                doc_stat["status"]    = "success"
                doc_stat["error_msg"] = f"via_vision_ocr_after_endpoint_error ({len(text)} chars)"
                doc_stat["time_total_s"] = round(time.time() - doc_start, 1)
                return doc_stat
            raise

        doc_stat["time_llm_s"]    = round(time.time() - t0, 1)
        doc_stat["input_tokens"]  = input_tokens
        doc_stat["output_tokens"] = output_tokens
        cost = (input_tokens * PRICE_INPUT_PER_TOKEN) + (output_tokens * PRICE_OUTPUT_PER_TOKEN)
        doc_stat["cost_usd"] = round(cost, 4)

        valid   = [r for r in results if not (isinstance(r, dict) and r.get("error"))]
        errored = [r for r in results if isinstance(r, dict) and r.get("error")]

        # Fallback 3b: extrator retornou tudo erro — indicativo de texto-lixo do ai_parse.
        # Tenta Vision OCR como recuperação (bypassa ai_parse totalmente).
        if errored and not valid:
            with print_lock:
                print(f"  [{pdf_name}] extrator retornou tudo erro — tentando Vision OCR...")
            if run_vision_fallback(pdf_name):
                doc_stat["status"]    = "success"
                doc_stat["error_msg"] = "via_vision_ocr_after_extractor_fail"
                doc_stat["time_total_s"] = round(time.time() - doc_start, 1)
                return doc_stat
            raise ValueError(f"parse_failed: {errored[0].get('raw','')[:200]}")

        save_result(pdf_name, valid)
        doc_stat["records"] = len(valid)
        doc_stat["status"]  = "ok"
        combos = [(get_nested(r, "tipo_entidade"), get_nested(r, "identificacao.periodo")) for r in valid]
        with print_lock:
            icon = "✓" if not errored else "⚠"
            print(f"  {icon} [{pdf_name}] {len(valid)} registro(s) {combos} em {doc_stat['time_llm_s']}s (${doc_stat['cost_usd']:.4f})")

    except Exception as e:
        doc_stat["error_msg"] = str(e)
        with print_lock:
            print(f"  ✗ [{pdf_name}] {e}")

    doc_stat["time_total_s"] = round(time.time() - doc_start, 1)
    return doc_stat


total_start = time.time()
stats       = []
errors      = []
successes   = []

print(f"Processando {len(new_pdfs)} PDF(s) com {MAX_WORKERS} workers paralelos...\n")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = {pool.submit(process_one, name): name for name in new_pdfs}
    for future in as_completed(futures):
        doc_stat = future.result()
        stats.append(doc_stat)
        if doc_stat["status"] == "ok":
            successes.append(doc_stat["pdf"])
        if doc_stat["error_msg"]:
            errors.append((doc_stat["pdf"], doc_stat["error_msg"]))

total_elapsed = time.time() - total_start

# COMMAND ----------
# MAGIC %md ## 4. resultados_final
# MAGIC
# MAGIC Tabela `resultados_final` NAO e gravada por este job. Ela e populada apenas
# MAGIC quando o usuario clica em "Submeter" no app (POST /api/finalize/{doc}).
# MAGIC Reprocessamento de um doc finalizado tambem apaga a linha em resultados_final
# MAGIC (POST /reprocess), reiniciando o ciclo de revisao.

# COMMAND ----------
# MAGIC %md ## 5. Relatório

# COMMAND ----------

total_input  = sum(s["input_tokens"]  for s in stats)
total_output = sum(s["output_tokens"] for s in stats)
total_cost   = sum(s["cost_usd"]      for s in stats)

print("=" * 60)
print(f"  Tempo total  : {int(total_elapsed//60)}m {int(total_elapsed%60)}s")
print(f"  Sucesso      : {len(successes)} / {len(new_pdfs)}")
print(f"  Erros        : {len(errors)}")
print(f"  Tokens entrada: {total_input:,}  (${total_input * PRICE_INPUT_PER_TOKEN:.4f})")
print(f"  Tokens saída  : {total_output:,}  (${total_output * PRICE_OUTPUT_PER_TOKEN:.4f})")
print(f"  Custo total  : ${total_cost:.4f}")
print("=" * 60)
for s in stats:
    status_str = "✓" if s["status"] == "ok" else f"✗ {s['error_msg'][:30]}"
    print(f"  {s['pdf'][:45]:<45} {s.get('time_total_s',0):>5.1f}s  {status_str}")

if errors:
    print()
    for name, err in errors:
        print(f"  ✗ {name}: {err}")

hard_errors = [(n, e) for n, e in errors if e != "no_text"]

# Relatório estruturado (aparece em jobs/runs/get-output → notebook_output.result)
exit_report = json.dumps({
    "status": "error" if hard_errors else "ok",
    "total": len(new_pdfs),
    "success": len(successes),
    "errors": [{"pdf": n, "error": e[:500]} for n, e in errors],
    "stats": [{"pdf": s["pdf"], "status": s["status"], "time_s": s.get("time_total_s", 0),
               "error": s.get("error_msg", "")[:500]} for s in stats],
}, ensure_ascii=False)

if hard_errors and len(hard_errors) > len(new_pdfs) // 2:
    raise Exception(f"Mais da metade falhou ({len(hard_errors)}/{len(new_pdfs)})\n{exit_report}")

dbutils.notebook.exit(exit_report)
