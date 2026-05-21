# Databricks notebook source
# MAGIC %md
# MAGIC # Migração `correcoes` → `revisoes_em_andamento` + `feedback_llm`
# MAGIC
# MAGIC Migra a tabela legada `correcoes` (formato monolítico) para a nova arquitetura:
# MAGIC - `revisoes_em_andamento`: estado transitório do workflow (limpo no Submeter)
# MAGIC - `feedback_llm`: histórico append-only (snapshot no Submeter)
# MAGIC
# MAGIC **Idempotente**: pode rodar várias vezes sem efeitos colaterais. Detecta o estado e age só no necessário.
# MAGIC
# MAGIC ## O que faz
# MAGIC 1. **Lakebase (Postgres)**: rename `correcoes`→`correcoes_legado`, cria tabelas novas se faltam, backfill, grants de sequence pro SP da app
# MAGIC 2. **Delta (UC)**: mesma coisa em ambiente Unity Catalog
# MAGIC
# MAGIC ## Quando rodar
# MAGIC - **Primeira vez no cliente**: depois do `setup_lakebase.py` e antes do deploy do app.
# MAGIC - **Re-execução**: segura — não cria duplicatas (ON CONFLICT DO NOTHING).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "ocr_financeiro")
dbutils.widgets.text("lakebase_host", "")
dbutils.widgets.text("lakebase_db", "ocr_financeiro")
dbutils.widgets.text("sp_client_id", "", label="SP client_id (para grants de sequence; vazio = pula)")

catalog       = dbutils.widgets.get("catalog").strip()
schema        = dbutils.widgets.get("schema").strip()
lakebase_host = dbutils.widgets.get("lakebase_host").strip()
lakebase_db   = dbutils.widgets.get("lakebase_db").strip()
sp_client_id  = dbutils.widgets.get("sp_client_id").strip()

print(f"Catalog:    {catalog or '(vazio — pular Delta)'}")
print(f"Schema:     {schema}")
print(f"Lakebase:   {lakebase_host or '(vazio — pular Lakebase)'} / {lakebase_db}")
print(f"App SP:     {sp_client_id or '(vazio — pular grants)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakebase: rename + create + backfill + grants

# COMMAND ----------

if lakebase_host:
    import psycopg2
    from databricks.sdk import WorkspaceClient

    # Gera credencial Lakebase
    w = WorkspaceClient()
    project = "ocr-financeiro"  # adapte se diferente
    branch  = "production"
    endpoint = "primary"
    endpoint_path = f"projects/{project}/branches/{branch}/endpoints/{endpoint}"
    resp = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path})
    token = resp.get("token", "")
    me = w.current_user.me()
    email = me.user_name

    conn = psycopg2.connect(
        host=lakebase_host, port=5432, dbname=lakebase_db,
        user=email, password=token, sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    # 1.1 Rename correcoes → correcoes_legado (se ainda não tiver sido feito)
    cur.execute("""SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='correcoes' AND table_schema='public')""")
    has_correcoes = cur.fetchone()[0]
    cur.execute("""SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='correcoes_legado' AND table_schema='public')""")
    has_legado = cur.fetchone()[0]
    if has_correcoes and not has_legado:
        cur.execute("ALTER TABLE correcoes RENAME TO correcoes_legado")
        print("  ✓ Renomeado: correcoes → correcoes_legado")
    elif has_legado:
        print("  · correcoes_legado já existe — skip rename")
    else:
        print("  · Não há tabela 'correcoes' nem 'correcoes_legado' (ambiente novo)")

    # 1.2 Cria revisoes_em_andamento se não existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS revisoes_em_andamento (
            document_name TEXT NOT NULL,
            campo TEXT NOT NULL,
            tipo_entidade TEXT NOT NULL DEFAULT '',
            periodo TEXT NOT NULL DEFAULT '',
            valor_extraido NUMERIC,
            valor_corrente NUMERIC,
            status TEXT NOT NULL DEFAULT 'llm',
            tipo_erro TEXT,
            tipo_erro_detalhe TEXT,
            revisado_por TEXT,
            revisado_em TIMESTAMP WITH TIME ZONE,
            criado_em TIMESTAMP WITH TIME ZONE DEFAULT now(),
            PRIMARY KEY (document_name, campo, tipo_entidade, periodo)
        );
        CREATE INDEX IF NOT EXISTS idx_revisoes_doc    ON revisoes_em_andamento(document_name);
        CREATE INDEX IF NOT EXISTS idx_revisoes_status ON revisoes_em_andamento(status);
    """)
    print("  ✓ revisoes_em_andamento OK")

    # 1.3 Cria feedback_llm se não existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS feedback_llm (
            id BIGSERIAL PRIMARY KEY,
            document_name TEXT NOT NULL,
            campo TEXT NOT NULL,
            tipo_entidade TEXT NOT NULL DEFAULT '',
            periodo TEXT NOT NULL DEFAULT '',
            valor_llm NUMERIC,
            valor_final NUMERIC,
            acao TEXT NOT NULL,
            tipo_erro TEXT,
            tipo_erro_detalhe TEXT,
            fonte_llm JSONB,
            revisado_por TEXT NOT NULL,
            revisado_em TIMESTAMP WITH TIME ZONE NOT NULL,
            submetido_em TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            modelo_versao TEXT,
            prompt_versao TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_feedback_doc        ON feedback_llm(document_name);
        CREATE INDEX IF NOT EXISTS idx_feedback_campo      ON feedback_llm(campo);
        CREATE INDEX IF NOT EXISTS idx_feedback_tipo_erro  ON feedback_llm(tipo_erro) WHERE tipo_erro IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_feedback_submetido  ON feedback_llm(submetido_em);
        CREATE INDEX IF NOT EXISTS idx_feedback_modelo     ON feedback_llm(modelo_versao);
    """)
    print("  ✓ feedback_llm OK")

    # 1.4 Backfill correcoes_legado → feedback_llm (idempotente via NOT EXISTS)
    if has_correcoes or has_legado:
        cur.execute("""
            INSERT INTO feedback_llm (
                document_name, campo, tipo_entidade, periodo,
                valor_llm, valor_final, acao, tipo_erro, tipo_erro_detalhe,
                fonte_llm, revisado_por, revisado_em, submetido_em,
                modelo_versao, prompt_versao
            )
            SELECT
                cl.document_name, cl.campo,
                COALESCE(cl.tipo_entidade, ''), COALESCE(cl.periodo, ''),
                NULLIF(cl.valor_extraido, '')::NUMERIC,
                NULLIF(cl.valor_correto, '')::NUMERIC,
                'corrigido',
                NULL,                           -- tipo_erro: legado não tem categoria
                cl.comentario,
                NULL,                           -- fonte_llm: legado não tem
                COALESCE(cl.confirmado_por, 'pre-migration'),
                COALESCE(cl.confirmado_em, cl.criado_em, now()),
                COALESCE(cl.resolvido_em, cl.confirmado_em, cl.criado_em, now()),
                'pre-migration',
                'pre-migration'
            FROM correcoes_legado cl
            WHERE cl.status != 'pendente'
              AND cl.valor_correto IS NOT NULL
              AND cl.valor_correto != ''
              AND NOT EXISTS (
                  -- evita duplicar se já foi backfillado antes
                  SELECT 1 FROM feedback_llm f
                  WHERE f.document_name = cl.document_name
                    AND f.campo = cl.campo
                    AND COALESCE(f.tipo_entidade,'') = COALESCE(cl.tipo_entidade,'')
                    AND COALESCE(f.periodo,'') = COALESCE(cl.periodo,'')
                    AND f.modelo_versao = 'pre-migration'
              )
        """)
        inserted_fb = cur.rowcount
        print(f"  ✓ Backfill feedback_llm: {inserted_fb} linhas novas")

        # 1.5 Backfill correcoes_legado → revisoes_em_andamento (pendentes)
        cur.execute("""
            INSERT INTO revisoes_em_andamento (
                document_name, campo, tipo_entidade, periodo,
                valor_extraido, valor_corrente, status,
                tipo_erro, tipo_erro_detalhe, revisado_por, revisado_em, criado_em
            )
            SELECT
                cl.document_name, cl.campo,
                COALESCE(cl.tipo_entidade, ''), COALESCE(cl.periodo, ''),
                NULLIF(cl.valor_extraido, '')::NUMERIC,
                NULLIF(COALESCE(cl.valor_correto, cl.valor_extraido), '')::NUMERIC,
                CASE
                  WHEN cl.valor_correto IS NOT NULL AND cl.valor_correto != ''
                       AND cl.valor_correto != cl.valor_extraido THEN 'corrigido'
                  WHEN cl.confirmado_em IS NOT NULL THEN 'confirmado'
                  ELSE 'llm'
                END,
                NULL,
                cl.comentario,
                cl.confirmado_por,
                cl.confirmado_em,
                COALESCE(cl.criado_em, now())
            FROM correcoes_legado cl
            WHERE cl.status = 'pendente'
            ON CONFLICT (document_name, campo, tipo_entidade, periodo) DO NOTHING
        """)
        inserted_rev = cur.rowcount
        print(f"  ✓ Backfill revisoes_em_andamento: {inserted_rev} linhas novas")

    # 1.6 Grants de sequence (BIGSERIAL precisa)
    if sp_client_id:
        cur.execute(f'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO "{sp_client_id}"')
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO "{sp_client_id}"')
        print(f"  ✓ Grants USAGE/SELECT/UPDATE em sequences concedidos ao SP")

    # 1.7 Sanity checks
    cur.execute("SELECT COUNT(*) FROM revisoes_em_andamento")
    rev_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM feedback_llm")
    fb_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM correcoes_legado") if has_correcoes or has_legado else None
    leg_count = cur.fetchone()[0] if (has_correcoes or has_legado) else 0

    print(f"\nLakebase pós-migração:")
    print(f"  revisoes_em_andamento: {rev_count}")
    print(f"  feedback_llm:          {fb_count}")
    print(f"  correcoes_legado:      {leg_count}")
    conn.close()
else:
    print("Lakebase pulado (lakebase_host vazio).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta (UC): rename + create + backfill

# COMMAND ----------

if catalog:
    cs = f"{catalog}.{schema}"
    # 2.1 Detecta tabelas
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {cs}").collect()]
    has_correcoes = 'correcoes' in tables
    has_legado    = 'correcoes_legado' in tables

    # 2.2 Rename
    if has_correcoes and not has_legado:
        spark.sql(f"ALTER TABLE {cs}.correcoes RENAME TO {cs}.correcoes_legado")
        print("  ✓ Delta: correcoes → correcoes_legado")
    elif has_legado:
        print("  · Delta: correcoes_legado já existe")
    else:
        print("  · Delta: sem tabela correcoes (ambiente novo)")

    # 2.3 Cria revisoes_em_andamento
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cs}.revisoes_em_andamento (
            document_name STRING NOT NULL,
            campo STRING NOT NULL,
            tipo_entidade STRING NOT NULL,
            periodo STRING NOT NULL,
            valor_extraido DOUBLE,
            valor_corrente DOUBLE,
            status STRING NOT NULL,
            tipo_erro STRING,
            tipo_erro_detalhe STRING,
            revisado_por STRING,
            revisado_em TIMESTAMP,
            criado_em TIMESTAMP
        ) USING DELTA
    """)
    print("  ✓ Delta: revisoes_em_andamento OK")

    # 2.4 Cria feedback_llm
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cs}.feedback_llm (
            id BIGINT,
            document_name STRING NOT NULL,
            campo STRING NOT NULL,
            tipo_entidade STRING NOT NULL,
            periodo STRING NOT NULL,
            valor_llm DOUBLE,
            valor_final DOUBLE,
            acao STRING NOT NULL,
            tipo_erro STRING,
            tipo_erro_detalhe STRING,
            fonte_llm STRING,
            revisado_por STRING NOT NULL,
            revisado_em TIMESTAMP NOT NULL,
            submetido_em TIMESTAMP NOT NULL,
            modelo_versao STRING,
            prompt_versao STRING
        ) USING DELTA
    """)
    print("  ✓ Delta: feedback_llm OK")

    # Nota: o app só lê/escreve no Lakebase. As tabelas Delta existem para que
    # eventuais jobs Spark (batch_job, vision_extraction) façam dual-write se necessário.
    # Sem backfill aqui por padrão — os dados ficam em correcoes_legado no Delta
    # como audit log e podem ser migrados via Spark se virar necessidade.

    print(f"\nDelta UC ({cs}) pós-migração: tabelas criadas, dados em correcoes_legado preservados.")
else:
    print("Delta UC pulado (catalog vazio).")

# COMMAND ----------

print("\n=== Migração concluída ===")
print("Próximos passos:")
print("1. Verifique o app pode ler /api/documentos/sidebar-state — todos os docs devem aparecer com status")
print("2. Pegue um doc não-revisado, edite um campo, salve — deve gravar em revisoes_em_andamento")
print("3. Submeta o doc — deve snapshot em feedback_llm e limpar revisoes_em_andamento")

dbutils.notebook.exit("ok")
