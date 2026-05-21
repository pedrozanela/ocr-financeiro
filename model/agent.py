"""
TechFin OCR v4 - Information Extraction Agent
Extrai dados financeiros estruturados de documentos (Balanço Patrimonial + DRE).

Melhoria 1: DE-PARA extraído do output_schema.json em runtime e inserido como seção
dedicada no system prompt, separado da definição estrutural do schema.

Melhoria 2: LLM Judge — segundo modelo avalia a qualidade da extração e sinaliza
campos com baixa confiança. Resultado armazenado em `_assessment` no JSON.

Code-based MLflow model: https://mlflow.org/docs/latest/models.html#models-from-code
"""
import copy
import json
import os
import mlflow
from mlflow.pyfunc import PythonModel

# ---------------------------------------------------------------------------
# Instruções de extração
# ---------------------------------------------------------------------------
INSTRUCTIONS = (
    "* O documento pode conter MÚLTIPLAS colunas de dados: diferentes tipos de entidade "
    "(Consolidado, Controladora/Individual) e/ou diferentes períodos (datas de referência). "
    "Você DEVE extrair TODAS as combinações presentes, gerando um elemento no array para cada "
    "combinação única de (tipo_entidade, periodo). Exemplos comuns: "
    "[Consolidado 2024-12-31, Controladora 2024-12-31], "
    "[Consolidado 2024-12-31, Consolidado 2023-12-31], "
    "[Consolidado 2024-12-31, Controladora 2024-12-31, Consolidado 2023-12-31, Controladora 2023-12-31].\n"
    "* REGRA CRÍTICA — multi-período: se o BP/DRE tiver N colunas com datas distintas "
    "(ex: YTD 30/06/2025 | 31/03/2025 | Auditado 2024 | Auditado 2023), gere UM elemento "
    "do array para CADA data, MESMO se o tipo_entidade for o mesmo. NUNCA descarte colunas "
    "de períodos anteriores — cada data é um registro separado. Exemplo correto para 4 colunas "
    "de INDIVIDUAL: [INDIVIDUAL 2025-06-30, INDIVIDUAL 2025-03-31, INDIVIDUAL 2024-12-31, INDIVIDUAL 2023-12-31].\n"
    "* Quando o documento apresentar colunas YTD (acumulado do ano) + trimestre isolado + períodos "
    "históricos auditados, cada um vira um registro separado — use a data de fechamento da coluna "
    "como `periodo` (ex: YTD 30/06/2025 → periodo='2025-06-30'; 1°T 31/03/2025 → periodo='2025-03-31'; "
    "Auditado 2024 → periodo='2024-12-31').\n"
    "* Para cada elemento, preencha `tipo_entidade` com CONSOLIDADO, CONTROLADORA ou INDIVIDUAL, "
    "conforme o cabeçalho da coluna correspondente no documento.\n"
    "* Substitua qualquer valor null, vazio ou não informado por zero.\n"
    "* Formate todos os números para exibir exatamente 2 casas decimais, usando ponto como separador, "
    "mesmo que o valor seja inteiro ou zero (ex: 834988.00, 0.00, 15.50).\n"
    "* Preencha o objeto `fontes` no JSON de saída seguindo a seção `## REGISTRO DE FONTES` abaixo.\n\n"
)

JUDGE_SYSTEM_PROMPT = """\
Você é um auditor especializado em demonstrações financeiras brasileiras.
Sua tarefa: avaliar a qualidade da extração de dados de um documento financeiro.

Dado o texto do documento e o JSON extraído, identifique campos com possíveis erros.

Retorne SOMENTE um JSON array com os campos suspeitos:
[{"campo": "caminho.do.campo", "confianca": "media|baixa", "motivo": "explicação (max 80 chars)"}]

Regras de verificação:
- Sinalize campos com valor ≠ 0 que parecem incorretos (valor trocado, sinal errado, escala errada)
- Sinalize campos com valor 0 quando o documento claramente indica valor diferente
- Não sinalize campos de totais (total_ativo_circulante, passivo_total, etc.)

Regras de classificação (sinalize se violadas):
- ativo_circulante.adiantamentos DEVE ser 0 (valores somados a outros_ativos_circulantes)
- patrimonio_liquido.reservas_de_lucro DEVE ser 0 (valores somados a lucros_ou_prejuizos_acumulados)
- passivo_circulante.outros_passivos_financeiros DEVE ser 0 (valores somados a outros_passivos_circulante)
- Direito de uso/arrendamento deve estar em imobilizado, não em intangivel_diferido
- Aplicações Financeiras e Consórcios do ANC devem estar em investimentos
- Provisões de contingências devem estar em provisoes (LP), não outros_passivos_nao_circulantes
- DRE deve usar valores ACUMULADOS, não trimestrais
- despesas_administrativas deve ser o valor residual, não o total agregado de despesas operacionais

- Se a extração parecer correta, retorne []
- Máximo 15 itens
- Retorne APENAS o JSON array, sem texto adicional.\
"""

# Registro de fontes — instrucao estruturada (string ou array)
REGISTRO_FONTES = """\
## REGISTRO DE FONTES

Para cada campo numérico do schema, registre em `fontes` o racional da extração.

**Quando o valor vem de um único item do PDF**: registre como string com o nome literal do item.

  Exemplo:
  "fontes": {
    "ativo_circulante.disponibilidades": "Caixa e equivalentes de caixa"
  }

**Quando o valor vem de uma soma ou diferença de múltiplos itens**: registre como
array de objetos, um por item, com:
  - `nome`: nome literal do item no documento
  - `valor`: valor numérico do item (sempre positivo, sem o sinal — o sinal vai em `operacao`)
  - `operacao`: "+" se somou, "-" se subtraiu

  Exemplo de soma:
  "fontes": {
    "ativo_circulante.outros_ativos_circulantes": [
      {"nome": "Adiantamentos", "valor": 12500.00, "operacao": "+"},
      {"nome": "Despesas antecipadas", "valor": 2363.00, "operacao": "+"}
    ]
  }

  Exemplo com subtração (receita líquida da DRE):
  "fontes": {
    "dre.receita_operacional_liquida": [
      {"nome": "Receita Bruta", "valor": 5000000.00, "operacao": "+"},
      {"nome": "Devoluções", "valor": 120000.00, "operacao": "-"},
      {"nome": "Impostos sobre Vendas", "valor": 680000.00, "operacao": "-"}
    ]
  }

**Regra de consistência**: a soma algébrica dos itens (valor × sinal de operação)
deve resultar no valor extraído do campo. Se a aritmética não bater, prefira
ajustar a fonte registrada em vez do valor do campo.

**Não use array com 1 item**: se há apenas uma fonte, registre como string. Arrays
têm sempre 2+ itens.
"""

# Regras absolutas — aplicadas antes de tudo, com precedência total
REGRAS_ABSOLUTAS = """\
## REGRAS ABSOLUTAS — APLICAR ANTES DE QUALQUER OUTRA REGRA

### 1. Reclassificação obrigatória: AFAC fora do Patrimônio Líquido

"Adiantamento para Futuro Aumento de Capital" (AFAC, Adto Futuro Aumento
de Capital, ou variantes) NÃO é Patrimônio Líquido conceitualmente.
Empresas frequentemente o classificam erradamente dentro do bloco
PATRIMÔNIO LÍQUIDO no documento — você DEVE reclassificar.

- AFAC dentro do bloco PATRIMÔNIO LÍQUIDO do documento → reclassifique
  para `passivo_nao_circulante.conta_corrente_socios_coligadas_controladas`.
  NUNCA deixe em reserva_de_capital, outras_reservas, ou qualquer campo do PL.

- AFAC em outras seções (AC/ANC/PC/PNC) do documento → classifique no
  cc_socios da seção correspondente, conforme o depara.
"""

# Template do system prompt — preenchido em runtime com artifacts separados
SYSTEM_PROMPT = """\
Você é um especialista em análise de demonstrações financeiras brasileiras.
Sua tarefa é extrair informações estruturadas de documentos financeiros (Balanço Patrimonial e DRE).

{regras_absolutas}

{depara}

{regras}

## INSTRUÇÕES DE EXTRAÇÃO

{instructions}

{registro_fontes}
{fewshot}
Retorne SOMENTE um JSON array válido seguindo exatamente o schema fornecido. Sem texto adicional.\
"""

# ---------------------------------------------------------------------------
# Helpers para processar o schema
# ---------------------------------------------------------------------------
_SECTION_LABELS = {
    "ativo_circulante":        "Ativo Circulante",
    "ativo_nao_circulante":    "Ativo Não Circulante",
    "ativo_permanente":        "Ativo Permanente",
    "passivo_circulante":      "Passivo Circulante",
    "passivo_nao_circulante":  "Passivo Não Circulante",
    "patrimonio_liquido":      "Patrimônio Líquido",
    "dre":                     "DRE — Demonstração do Resultado",
}


def build_depara_section(depara: dict) -> str:
    """Gera a seção '## DICIONÁRIO DE CONTAS' a partir do depara.json."""
    # Agrupa por seção de primeiro nível
    sections: dict[str, list] = {}
    for path, entry in depara.items():
        top = path.split(".")[0]
        sections.setdefault(top, []).append((path, entry["conceito"], entry["aliases"]))

    lines = [
        "## DICIONÁRIO DE CONTAS (DE-PARA)",
        "",
        "REGRA ABSOLUTA: se o nome de uma linha do documento corresponder (exato ou similar)",
        "a um dos aliases abaixo, SEMPRE mapeie para o campo indicado — mesmo que seu",
        "conhecimento contábil sugira outro campo. O de-para deste cliente tem PRECEDÊNCIA",
        "total sobre qualquer convenção contábil padrão.",
        "",
    ]
    for section_key, field_entries in sections.items():
        label = _SECTION_LABELS.get(section_key, section_key)
        lines.append(f"### {label}")
        lines.append("")
        for path, conceito, aliases in field_entries:
            if not aliases:
                continue
            lines.append(f"**{path}** — {conceito}")
            # Render aliases as bullet list for better scannability
            for alias in aliases:
                lines.append(f"  - {alias}")
            lines.append("")

    return "\n".join(lines)


def build_regras_section(regras: list) -> str:
    """Gera a seção '## REGRAS DE CLASSIFICAÇÃO CONTÁBIL' a partir do regras_classificacao.json."""
    if not regras:
        return ""
    lines = [
        "## REGRAS DE CLASSIFICAÇÃO CONTÁBIL",
        "",
        "As regras abaixo são OBRIGATÓRIAS e têm prioridade sobre qualquer interpretação individual.",
        "",
    ]
    for r in regras:
        lines.append(f"### {r['id']}. {r['titulo']}")
        lines.append(r["regra"])
        lines.append("")
    return "\n".join(lines)


def clean_schema(schema: dict) -> dict:
    """Retorna cópia limpa do schema (só estrutura, sem metadados extras)."""
    return copy.deepcopy(schema)


# ---------------------------------------------------------------------------
# Modelo MLflow
# ---------------------------------------------------------------------------
class TechFinExtractorAgent(PythonModel):

    def _get_client(self):
        """Cria o cliente OpenAI com as credenciais do ambiente.

        No Model Serving, o Databricks injeta DATABRICKS_CLIENT_ID +
        DATABRICKS_CLIENT_SECRET + DATABRICKS_HOST via automatic auth passthrough
        (quando resources são declarados no log_model). O WorkspaceClient() descobre
        essas variáveis automaticamente via unified auth e resolve o token OAuth.

        Fallback: lê DATABRICKS_TOKEN diretamente (PAT manual ou Apps).
        """
        from openai import OpenAI
        from databricks.sdk import WorkspaceClient

        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")

        if not token:
            # 1. OAuth M2M direto via HTTP — mais confiável que SDK, funciona em
            # qualquer versão. Serving endpoints injetam automaticamente as env vars
            # DATABRICKS_CLIENT_ID e DATABRICKS_CLIENT_SECRET.
            client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
            client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
            if client_id and client_secret:
                import requests as _rq
                oauth_host = host or os.environ.get("DATABRICKS_HOST", "")
                if oauth_host and not oauth_host.startswith("http"):
                    oauth_host = f"https://{oauth_host}"
                try:
                    r = _rq.post(
                        f"{oauth_host.rstrip('/')}/oidc/v1/token",
                        auth=(client_id, client_secret),
                        data={"grant_type": "client_credentials", "scope": "all-apis"},
                        timeout=30,
                    )
                    r.raise_for_status()
                    token = r.json().get("access_token", "")
                    host = oauth_host
                except Exception as e:
                    print(f"[agent] OAuth M2M direto falhou: {e}")

            # 2. Fallbacks via SDK caso o método direto não funcione
            if not token:
                w = WorkspaceClient()
                host = host or w.config.host or ""

                if getattr(w.config, "token", None):
                    token = w.config.token

                if not token:
                    try:
                        auth_headers = {}
                        w.config.authenticate(auth_headers.update)
                        bearer = auth_headers.get("Authorization", "")
                        if bearer.startswith("Bearer "):
                            token = bearer[len("Bearer "):]
                    except TypeError:
                        pass

                if not token:
                    try:
                        provider = w.config.credentials_provider()
                        if provider:
                            creds = provider("GET", w.config.host)
                            bearer = (creds or {}).get("Authorization", "")
                            if bearer.startswith("Bearer "):
                                token = bearer[len("Bearer "):]
                    except Exception:
                        pass

                if not token:
                    try:
                        t = w.config.oauth_token()
                        if t and getattr(t, "access_token", None):
                            token = t.access_token
                    except Exception:
                        pass

        if not token:
            raise RuntimeError(
                "No auth token found. Env vars checked: DATABRICKS_TOKEN, "
                "DATABRICKS_CLIENT_ID+DATABRICKS_CLIENT_SECRET."
            )
        if not host:
            raise RuntimeError("No host found. Set DATABRICKS_HOST env var.")

        if not host.startswith("http"):
            host = f"https://{host}"

        return OpenAI(
            api_key=token,
            base_url=f"{host.rstrip('/')}/serving-endpoints",
        )

    def load_context(self, context):
        # NÃO inicializar o client aqui — credenciais OAuth M2M são injetadas
        # pelo Databricks apenas em request time, não durante o load do modelo.
        # O client é criado em predict() via _get_client().

        with open(context.artifacts["output_schema"]) as f:
            raw_schema = json.load(f)

        # Schema limpo (só estrutura) → vai no user message
        self.output_schema = clean_schema(raw_schema)

        # DE-PARA de arquivo separado (ou fallback do schema antigo)
        depara_path = context.artifacts.get("depara")
        if depara_path and os.path.exists(depara_path):
            with open(depara_path) as f:
                depara_data = json.load(f)
            self.depara_section = build_depara_section(depara_data)
            # Extract detalhe_comum fields for anti-duplication post-processing
            self._detalhe_comum = {}
            for path, entry in depara_data.items():
                if entry.get("tipo") == "detalhe_comum":
                    group = path.split(".")[0]
                    field = path.split(".", 1)[1]
                    self._detalhe_comum.setdefault(group, []).append(field)
            # Build reverse alias index: alias_lower → list of target paths
            self._alias_to_path = {}
            for path, entry in depara_data.items():
                for alias in entry.get("aliases", []):
                    key = alias.strip().lower()
                    self._alias_to_path.setdefault(key, []).append(path)
        else:
            self.depara_section = ""
            self._detalhe_comum = {}

        # Regras de classificação contábil de arquivo separado
        regras_path = context.artifacts.get("regras_classificacao")
        if regras_path and os.path.exists(regras_path):
            with open(regras_path) as f:
                regras_data = json.load(f)
            self.regras_section = build_regras_section(regras_data)
        else:
            self.regras_section = ""

        # Few-shot examples de correções anteriores (artifact opcional)
        fewshot_path = context.artifacts.get("few_shot_examples")
        if fewshot_path and os.path.exists(fewshot_path):
            with open(fewshot_path) as f:
                self.fewshot_examples = json.load(f)
        else:
            self.fewshot_examples = []

    def _build_fewshot_section(self) -> str:
        """Gera seção de exemplos de correções anteriores para o prompt."""
        if not self.fewshot_examples:
            return ""
        lines = [
            "\n## EXEMPLOS DE CORREÇÕES ANTERIORES",
            "",
            "Os exemplos abaixo representam erros recorrentes encontrados em extrações anteriores.",
            "Use-os como referência para evitar repetir os mesmos erros.",
            "",
        ]
        for i, ex in enumerate(self.fewshot_examples, 1):
            freq = ex.get("frequencia", 1)
            lines.append(f"### {i}. `{ex['campo']}` ({freq}x corrigido)")
            if ex.get("fonte_doc"):
                lines.append(f"- Texto no documento: \"{ex['fonte_doc']}\"")
            lines.append(f"- Extração errada: {ex['valor_errado']}")
            lines.append(f"- Valor correto: {ex['valor_correto']}")
            lines.append(f"- Motivo: {ex['explicacao']}")
            lines.append("")
        return "\n".join(lines)

    def _system_prompt(self) -> str:
        return SYSTEM_PROMPT.format(
            regras_absolutas=REGRAS_ABSOLUTAS,
            depara=self.depara_section,
            regras=self.regras_section,
            instructions=INSTRUCTIONS,
            registro_fontes=REGISTRO_FONTES,
            fewshot=self._build_fewshot_section(),
        )

    def _user_prompt(self, text: str) -> str:
        schema_str = json.dumps(self.output_schema, ensure_ascii=False, indent=2)
        return (
            f"Extraia as informações financeiras do seguinte documento e retorne um JSON "
            f"seguindo exatamente este schema:\n\n{schema_str}\n\n"
            f"DOCUMENTO:\n{text}"
        )

    def _judge(self, client, text: str, result: dict) -> tuple[list, dict]:
        """Avalia qualidade da extração. Retorna (lista de campos suspeitos, usage dict)."""
        result_for_judge = copy.deepcopy(result)
        result_for_judge.pop("fontes", None)  # Remove fontes para reduzir tokens
        result_str = json.dumps(result_for_judge, ensure_ascii=False, indent=2)
        doc_preview = text[:5000] if len(text) > 5000 else text
        user_msg = (
            f"DOCUMENTO (trecho):\n{doc_preview}\n\n"
            f"JSON EXTRAÍDO:\n{result_str}"
        )
        try:
            response = client.chat.completions.create(
                model=os.environ.get("JUDGE_MODEL", os.environ.get("OCR_MODEL", "databricks-claude-sonnet-4")),
                messages=[
                    {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0,
                max_tokens=800,
            )
            usage = {
                "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
                "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            }
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            assessment = json.loads(raw)
            return (assessment if isinstance(assessment, list) else []), usage
        except Exception:
            return [], {}

    # Configuração de grupos com campo outros_* — usado no pós-processamento
    # para recalcular outros como resíduo (total - soma dos campos específicos).
    _GROUPS_WITH_OUTROS = {
        "ativo_circulante": {
            "total": "total_ativo_circulante",
            "outros": "outros_ativos_circulantes",
            "specific": ["disponibilidades", "titulos_a_receber", "estoques",
                         "adiantamentos", "impostos_a_recuperar",
                         "conta_corrente_socios_control_colig",
                         "outros_ativos_financeiros"],
        },
        "ativo_nao_circulante": {
            "total": "total_ativo_nao_circulante",
            "outros": "outros_realizavel_a_longo_prazo",
            "specific": ["titulos_a_receber", "estoques", "adiantamentos",
                         "impostos_a_recuperar", "despesas_pagas_antecipadamente",
                         "conta_corrente_socios_control_colig"],
        },
        "passivo_circulante": {
            "total": "total_passivo_circulante",
            "outros": "outros_passivos_circulante",
            "specific": ["fornecedores", "financiamentos_com_instituicoes_de_credito",
                         "salarios_contribuicoes", "tributos", "adiantamentos",
                         "conta_corrente_socios_coligadas_controladas", "provisoes",
                         "outros_passivos_financeiros"],
        },
        "passivo_nao_circulante": {
            "total": "total_passivo_nao_circulante",
            "outros": "outros_passivos_nao_circulantes",
            "specific": ["fornecedores", "financiamentos_com_instituicoes_de_credito",
                         "salarios_contribuicoes", "tributos", "adiantamentos",
                         "conta_corrente_socios_coligadas_controladas", "provisoes"],
        },
    }

    # Groups with totals for gap detection
    _GROUP_TOTALS = {
        "ativo_circulante": "total_ativo_circulante",
        "ativo_nao_circulante": "total_ativo_nao_circulante",
        "ativo_permanente": "total_ativo_permanente",
        "passivo_circulante": "total_passivo_circulante",
        "passivo_nao_circulante": "total_passivo_nao_circulante",
        "patrimonio_liquido": "total_patrimonio_liquido",
    }

    def _postprocess_gap_recovery(self, result, text):
        """Recupera valores não extraídos quando há gap entre total e soma.

        Se total_grupo - soma_campos > threshold, busca no texto do documento
        linhas que correspondem a aliases do depara apontando para OUTRO grupo.
        Extrai o valor numérico e move para o campo correto."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        if not self._alias_to_path or not text:
            return result

        import re

        postprocessed = result.setdefault("_postprocessed", [])

        for group_name, total_field in self._GROUP_TOTALS.items():
            grp = result.get(group_name)
            if not isinstance(grp, dict):
                continue
            total_val = grp.get(total_field) or 0
            if abs(total_val) < 1:
                continue
            soma = sum(v for k, v in grp.items()
                       if k != total_field and isinstance(v, (int, float)))
            gap = total_val - soma
            # Only act if gap is significant (>1% of total)
            if abs(gap) < max(1.0, abs(total_val) * 0.01):
                continue

            # Search text for aliases that should go to a DIFFERENT group
            for alias_lower, target_paths in self._alias_to_path.items():
                # Only consider aliases that map to a different group
                targets_other_group = [
                    tp for tp in target_paths
                    if tp.split(".")[0] != group_name
                ]
                if not targets_other_group:
                    continue

                # Search for the alias in the text (case-insensitive)
                # Look for: alias followed by a number on the same or next line
                pattern = re.compile(
                    re.escape(alias_lower).replace(r"\ ", r"\s+") +
                    r"[\s\S]{0,50}?" +
                    r"([\-\(]?\s*[\d.,]+(?:\.\d{2,3})?\s*\)?)",
                    re.IGNORECASE
                )
                match = pattern.search(text)
                if not match:
                    continue

                # Parse the value
                raw_val = match.group(1).strip()
                # Handle Brazilian format: 1.234.567,89 or (1.234.567,89)
                is_negative = raw_val.startswith("(") or raw_val.startswith("-")
                raw_val = raw_val.strip("()-").strip()
                # Detect format: if has comma followed by 2 digits at end → BR format
                if re.search(r",\d{2}$", raw_val):
                    raw_val = raw_val.replace(".", "").replace(",", ".")
                try:
                    value = float(raw_val)
                except ValueError:
                    continue
                if is_negative:
                    value = -value

                # Only accept if value matches the gap closely (within 5%)
                if abs(value) < 1:
                    continue
                tolerance = max(1.0, abs(gap) * 0.05)
                if abs(abs(value) - abs(gap)) > tolerance:
                    continue  # Value doesn't explain the gap

                # Move to the correct field — prefer non-circulante over circulante
                # (items missed from PL are typically long-term)
                target = targets_other_group[0]
                for tp in targets_other_group:
                    if "nao_circulante" in tp:
                        target = tp
                        break
                target_parts = target.split(".")
                target_group = target_parts[0]
                target_field = target_parts[1] if len(target_parts) > 1 else target_parts[0]

                target_grp = result.get(target_group)
                if not isinstance(target_grp, dict):
                    continue

                existing = target_grp.get(target_field, 0) or 0
                target_grp[target_field] = round(existing + value, 2)

                postprocessed.append({
                    "campo": f"{target_group}.{target_field}",
                    "original": existing,
                    "corrigido": round(existing + value, 2),
                    "motivo": f"Gap recovery: '{alias_lower}' ({value}) encontrado no texto, movido de {group_name} conforme de-para",
                })

                # Reduce the gap
                gap -= value
                if abs(gap) < max(1.0, abs(total_val) * 0.01):
                    break

        return result

    def _postprocess_afac_in_pl(self, result):
        """AFAC em Patrimônio Líquido é erro de classificação da empresa.
        Move para passivo_nao_circulante.cc_socios. Em outros campos, não age.
        Atualiza total_patrimonio_liquido e total_passivo_nao_circulante para
        manter consistência interna (sem isso, _postprocess_outros — que roda
        depois — back-calcularia outros_passivos_nao_circulantes anulando o efeito).
        passivo_total (PC+PNC+PL, modelo brasileiro) não muda."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        fontes = result.get("fontes", {}) or {}
        AFAC_PATTERNS = [
            'afac',
            'adiantamento para futuro aumento',
            'adto futuro aumento',
            'adiantamento p futuro aumento',
            'adiantamento para aumento de capital',
        ]
        target_path = "passivo_nao_circulante.conta_corrente_socios_coligadas_controladas"
        target_grp, target_field = target_path.split(".")

        for campo, fonte_text in list(fontes.items()):
            if not isinstance(fonte_text, str):
                continue
            if not campo.startswith("patrimonio_liquido."):
                continue
            if not any(p in fonte_text.lower() for p in AFAC_PATTERNS):
                continue
            parts = campo.split(".")
            obj = result
            for p in parts[:-1]:
                obj = obj.get(p, {}) if isinstance(obj, dict) else {}
            if not isinstance(obj, dict):
                continue
            value = obj.get(parts[-1], 0) or 0
            if abs(value) < 0.01:
                continue
            obj[parts[-1]] = 0
            target_obj = result.setdefault(target_grp, {})
            existing = target_obj.get(target_field, 0) or 0
            target_obj[target_field] = round(existing + value, 2)

            # Atualizar totais para manter consistência (PL diminui, PNC aumenta)
            pl_obj = result.setdefault("patrimonio_liquido", {})
            pl_total_before = pl_obj.get("total_patrimonio_liquido", 0) or 0
            pl_obj["total_patrimonio_liquido"] = round(pl_total_before - value, 2)

            pnc_total_before = target_obj.get("total_passivo_nao_circulante", 0) or 0
            target_obj["total_passivo_nao_circulante"] = round(pnc_total_before + value, 2)

            if target_path in result.get("fontes", {}):
                result["fontes"][target_path] = f"{result['fontes'][target_path]} + {fonte_text}"
            else:
                result["fontes"][target_path] = fonte_text
            result["fontes"].pop(campo, None)

            # Instrumentação: telemetria para monitorar uso do postproc vs LLM
            tipo_ent = result.get("tipo_entidade", "")
            periodo = (result.get("identificacao") or {}).get("periodo", "")
            print(
                f"[AFAC-postproc] entidade={tipo_ent!r} periodo={periodo!r} "
                f"campo_origem={campo!r} valor={value} fonte={fonte_text!r}"
            )

            postprocessed = result.setdefault("_postprocessed", [])
            postprocessed.append({
                "campo": campo, "original": value, "corrigido": 0,
                "motivo": f"AFAC em PL detectado ('{fonte_text}'), reclassificado para PNC.cc_socios",
            })
            postprocessed.append({
                "campo": target_path,
                "original": existing, "corrigido": target_obj[target_field],
                "motivo": f"AFAC recebido de {campo} (Regra Absoluta nº 1)",
            })
            postprocessed.append({
                "campo": "patrimonio_liquido.total_patrimonio_liquido",
                "original": pl_total_before, "corrigido": pl_obj["total_patrimonio_liquido"],
                "motivo": f"Total PL ajustado: -{value} (saida de AFAC)",
            })
            postprocessed.append({
                "campo": "passivo_nao_circulante.total_passivo_nao_circulante",
                "original": pnc_total_before, "corrigido": target_obj["total_passivo_nao_circulante"],
                "motivo": f"Total PNC ajustado: +{value} (entrada de AFAC)",
            })
        return result

    def _postprocess_reclassify(self, result):
        """Reclassifica campos cujo 'fontes' indica que o LLM mapeou para o campo errado.

        Para cada campo com fonte, verifica se o texto da fonte corresponde a um alias
        no depara que aponta para um campo DIFERENTE. Se sim, move o valor para o campo
        correto. Isso corrige casos em que o LLM ignora o depara (ex: AFAC no PL em vez
        de PNC.cc_socios, ou 'Ajustes as Normas Internacionais' em reservas_de_reavaliacao
        em vez de lucros_acumulados)."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        if not self._alias_to_path:
            return result

        fontes = result.get("fontes", {})
        if not fontes:
            return result

        postprocessed = result.setdefault("_postprocessed", [])
        moves = []  # (from_path, to_path, value, fonte_text)

        for campo, fonte_text in fontes.items():
            if not fonte_text or not isinstance(fonte_text, str):
                continue
            # Get current value of the field
            parts = campo.split(".")
            obj = result
            for p in parts[:-1]:
                obj = obj.get(p, {}) if isinstance(obj, dict) else {}
            current_val = obj.get(parts[-1]) if isinstance(obj, dict) else None
            if not current_val or (isinstance(current_val, (int, float)) and abs(current_val) < 0.01):
                continue

            # Check each item in the fonte (split by " + ")
            items = [s.strip() for s in fonte_text.split(" + ")]
            for item in items:
                # Strip trailing numbers/values to get just the name
                item_name = item.strip()
                key = item_name.lower()
                # Look up in depara reverse index
                target_paths = self._alias_to_path.get(key, [])
                if not target_paths:
                    continue
                # Only reclassify if: (a) single-item fonte, (b) alias is
                # UNAMBIGUOUS (points to exactly one field in depara, excluding
                # CP/LP variants of the same field name), (c) that field != current
                if len(items) != 1:
                    continue
                # Filter out CP/LP variants — keep only unique field names
                unique_fields = set()
                for tp in target_paths:
                    unique_fields.add(tp.split(".")[-1])
                if campo.split(".")[-1] in unique_fields:
                    # Current field name is among targets — alias is ambiguous
                    continue
                # Pick the target that is NOT the current campo
                target = next((tp for tp in target_paths if tp != campo), None)
                if target:
                    moves.append((campo, target, current_val, item_name))

        # Apply moves
        for from_path, to_path, value, fonte_text in moves:
            # Remove from source
            from_parts = from_path.split(".")
            from_obj = result
            for p in from_parts[:-1]:
                from_obj = from_obj.get(p, {})
            if isinstance(from_obj, dict):
                old_val = from_obj.get(from_parts[-1], 0)
                from_obj[from_parts[-1]] = 0

            # Add to target
            to_parts = to_path.split(".")
            to_obj = result
            for p in to_parts[:-1]:
                if p not in to_obj:
                    to_obj[p] = {}
                to_obj = to_obj[p]
            if isinstance(to_obj, dict):
                existing = to_obj.get(to_parts[-1], 0) or 0
                to_obj[to_parts[-1]] = round(existing + value, 2)

            postprocessed.append({
                "campo": from_path,
                "original": old_val,
                "corrigido": 0,
                "motivo": f"Reclassificado para {to_path} conforme de-para (fonte: '{fonte_text}')",
            })
            postprocessed.append({
                "campo": to_path,
                "original": existing,
                "corrigido": round(existing + value, 2),
                "motivo": f"Recebido de {from_path} conforme de-para (fonte: '{fonte_text}')",
            })

        return result

    def _postprocess_anti_duplicacao(self, result):
        """Remove dupla contagem de campos marcados como detalhe_comum no depara.

        Se a soma dos sub-itens de um grupo excede o total extraído (outros_*
        daria negativo), itera nos campos detalhe_comum do grupo, zerando o de
        maior valor primeiro, até que soma ≤ total. Depois _postprocess_outros
        recalcula outros_* como resíduo."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        for group_name, cfg in self._GROUPS_WITH_OUTROS.items():
            grp = result.get(group_name)
            if not isinstance(grp, dict):
                continue
            total_val = grp.get(cfg["total"]) or 0
            if total_val <= 0:
                continue
            # Soma dos específicos (sem outros_*)
            specific_sum = sum(grp.get(f) or 0 for f in cfg["specific"])
            # Se específicos já excedem o total, outros_* seria negativo → dupla contagem
            if specific_sum <= total_val:
                continue
            # Get detalhe_comum fields for this group, sorted by value descending
            suspects = self._detalhe_comum.get(group_name, [])
            if not suspects:
                continue
            suspects_sorted = sorted(suspects, key=lambda f: abs(grp.get(f) or 0), reverse=True)
            for field in suspects_sorted:
                val = grp.get(field) or 0
                if abs(val) < 1:
                    continue
                postprocessed = result.setdefault("_postprocessed", [])
                postprocessed.append({
                    "campo": f"{group_name}.{field}",
                    "original": val,
                    "corrigido": 0,
                    "motivo": f"Provável dupla contagem com {cfg['specific']}: zerado (detalhe_comum)",
                })
                grp[field] = 0
                # Recompute and check
                specific_sum = sum(grp.get(f) or 0 for f in cfg["specific"])
                if specific_sum <= total_val:
                    break
        return result

    @classmethod
    def _postprocess_outros(cls, result):
        """Recalcula campos outros_* como resíduo (total - soma dos específicos)
        quando a soma dos subitens não bate com o total extraído do documento.

        Segurança: só corrige se o resíduo computado for ≥ 0 e se a diferença
        atual for significativa (> 1 unidade) — evita ajustes por arredondamento.
        O total_* e os campos específicos são extraídos diretamente do documento
        (Regra 11); outros_* é o campo mais propenso a erros aritméticos do LLM."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        for group_name, cfg in cls._GROUPS_WITH_OUTROS.items():
            grp = result.get(group_name)
            if not isinstance(grp, dict):
                continue
            total_val = grp.get(cfg["total"]) or 0
            if total_val <= 0:
                continue
            specific_sum = sum(grp.get(f) or 0 for f in cfg["specific"])
            current_outros = grp.get(cfg["outros"]) or 0
            computed_outros = total_val - specific_sum
            current_total = specific_sum + current_outros
            # Só corrige se: (a) diferença relevante (>1) E (b) resíduo não-negativo
            if abs(current_total - total_val) > 1 and computed_outros >= 0:
                postprocessed = result.setdefault("_postprocessed", [])
                postprocessed.append({
                    "campo": f"{group_name}.{cfg['outros']}",
                    "original": current_outros,
                    "corrigido": round(computed_outros, 2),
                    "motivo": f"Recalculado como resíduo: {cfg['total']} - soma específicos",
                })
                grp[cfg["outros"]] = round(computed_outros, 2)
        return result

    # Campos que compõem total_despesas_operacionais (per Regra 13)
    _DRE_DESP_OP_COMPONENTES = [
        "despesas_com_vendas",
        "provisao_para_devedores_duvidosos",
        "outras_receitas_despesas_operacionais",
        "despesas_administrativas",
        "despesas_tributarias",
        "despesas_gerais",
        "depreciacao",
        "amortizacao",
    ]

    @classmethod
    def _postprocess_cascata_dre(cls, result):
        """Corrige inconsistências aritméticas na cascata da DRE:

        1. total_despesas_operacionais = soma dos componentes (Regra 13)
           — LLMs frequentemente erram a soma de 5+ itens.
        2. Detecta sinal invertido de provisao_imposto_de_renda comparando
           com a cascata: se LL = LAIR - provisao - csll não bate mas
           LL = LAIR + provisao - csll bate, inverte o sinal."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        dre = result.get("dre")
        if not isinstance(dre, dict):
            return result

        # (1) Anti-duplicação: se outras_receitas_despesas_operacionais ≈
        # |despesa_nao_operacional − receita_nao_operacional|, é duplicação do
        # mesmo bloco não-operacional (LLM extraiu item individual + agregado).
        # Zera o campo operacional nesse caso.
        outras_op = dre.get("outras_receitas_despesas_operacionais") or 0
        desp_no = dre.get("despesa_nao_operacional") or 0
        rec_no = dre.get("receita_nao_operacional") or 0
        net_no = abs(desp_no - rec_no)
        if abs(outras_op) > 1 and net_no > 1:
            tolerance = max(1.0, net_no * 0.01)
            if abs(abs(outras_op) - net_no) < tolerance:
                dre["outras_receitas_despesas_operacionais"] = 0.0

        # (2) total_despesas_operacionais = soma dos componentes
        postprocessed = result.get("_postprocessed", [])
        components_sum = sum(dre.get(f) or 0 for f in cls._DRE_DESP_OP_COMPONENTES)
        current_total = dre.get("total_despesas_operacionais") or 0
        if abs(current_total - components_sum) > 1:
            postprocessed.append({
                "campo": "dre.total_despesas_operacionais",
                "original": current_total,
                "corrigido": round(components_sum, 2),
                "motivo": "Recalculado: soma dos componentes de despesas operacionais",
            })
            dre["total_despesas_operacionais"] = round(components_sum, 2)

        # (2a) Corrige totais cujos componentes estão zerados — LLM mapeou
        # o item direto no total em vez do componente (de-para ignorado).
        # Se total > 0 e TODOS os componentes = 0, move total → componente primário.
        # IMPORTANTE: roda ANTES da cascata para que despesas_financeiras,
        # total_receitas_financeiras etc. estejam corretos.
        _TOTAL_TO_PRIMARY = {
            "despesas_financeiras": {
                "primary": "encargos_financeiros",
                "components": ["encargos_financeiros", "descontos_concedidos", "variacao_cambial_nao_paga"],
            },
            "total_receitas_financeiras": {
                "primary": "receitas_financeiras",
                "components": ["receitas_financeiras", "variacao_cambial_nao_recebida"],
            },
            "receita_operacional_bruta": {
                "primary": "receita_venda_produto_mercadoria",
                "components": ["receita_venda_produto_mercadoria", "receita_servicos_arrendamento"],
            },
            "total_deducoes": {
                "primary": "impostos_incidentes_sobre_vendas",
                "components": ["vendas_anuladas", "abatimentos", "impostos_incidentes_sobre_vendas"],
            },
            "total_custo": {
                "primary": "custo_servicos_produtos_mercadorias_vendidas",
                "components": ["custo_servicos_produtos_mercadorias_vendidas", "superveniencias_ativas"],
            },
        }
        for total_field, cfg in _TOTAL_TO_PRIMARY.items():
            total_val = dre.get(total_field) or 0
            if abs(total_val) < 1:
                continue
            comp_sum = sum(abs(dre.get(c) or 0) for c in cfg["components"])
            if comp_sum < 1:
                # All components are zero but total has value → push down to primary
                postprocessed.append({
                    "campo": f"dre.{cfg['primary']}",
                    "original": 0,
                    "corrigido": total_val,
                    "motivo": f"Movido de dre.{total_field} (componentes zerados)",
                })
                dre[cfg["primary"]] = total_val
            # Recalculate total from components
            new_total = sum(dre.get(c) or 0 for c in cfg["components"])
            if abs(total_val - new_total) > 0.01:
                postprocessed.append({
                    "campo": f"dre.{total_field}",
                    "original": total_val,
                    "corrigido": round(new_total, 2),
                    "motivo": "Recalculado: soma dos componentes",
                })
            dre[total_field] = round(new_total, 2)

        # (2b) Recalcula campos-cascata da DRE — soma simples respeitando
        # sinais do documento (Regra 22). Roda DEPOIS de _TOTAL_TO_PRIMARY.
        # Fase 1: cascata até LAIR
        _CASCADE_PHASE1 = [
            ("receita_operacional_liquida", ["receita_operacional_bruta", "total_deducoes", "incentivos_a_exportacoes"]),
            ("lucro_bruto", ["receita_operacional_liquida", "total_custo"]),
            ("lucro_operacional", ["lucro_bruto", "total_despesas_operacionais"]),
            ("lucro_financeiro", ["lucro_operacional", "despesas_financeiras", "total_receitas_financeiras"]),
            ("lucro_antes_imposto_de_renda", ["lucro_financeiro", "resultado_de_equivalencia_patrimonial", "receita_nao_operacional", "despesa_nao_operacional", "saldo_correcao_monetaria", "resultado_alienacao_ativos"]),
        ]

        def _recalc(field, components):
            calc = sum(dre.get(f) or 0 for f in components)
            has_inputs = any(abs(dre.get(f) or 0) > 0 for f in components)
            old_val = dre.get(field) or 0
            if has_inputs and abs(old_val - calc) > 0.01:
                desc = " + ".join(components)
                postprocessed.append({
                    "campo": f"dre.{field}",
                    "original": old_val,
                    "corrigido": round(calc, 2),
                    "motivo": f"Recalculado: {desc}",
                })
            if has_inputs:
                dre[field] = round(calc, 2)

        for field, components in _CASCADE_PHASE1:
            _recalc(field, components)

        # (3) Checar sinal de provisao_imposto_de_renda via cascata (soma simples)
        # Roda ENTRE fase 1 e fase 2 para que LAIR esteja correto e
        # lucro_antes_participacoes use o IRPJ com sinal corrigido.
        lair = dre.get("lucro_antes_imposto_de_renda") or 0
        provisao = dre.get("provisao_imposto_de_renda") or 0
        csll = dre.get("csll") or 0
        ll_extraido = dre.get("lucro_liquido") or 0
        part_grat = dre.get("participacoes_gratificacoes_estatutarias") or 0
        part_min = dre.get("participacao_minoritarios") or 0

        if abs(ll_extraido) > 0 and abs(provisao) > 1:
            expected_default = lair + provisao + csll + part_grat + part_min
            expected_flipped = lair + (-provisao) + csll + part_grat + part_min
            tolerance = max(1.0, abs(ll_extraido) * 0.01)
            if (abs(expected_default - ll_extraido) > tolerance
                and abs(expected_flipped - ll_extraido) < tolerance):
                postprocessed.append({
                    "campo": "dre.provisao_imposto_de_renda",
                    "original": provisao,
                    "corrigido": round(-provisao, 2),
                    "motivo": "Sinal invertido para bater com cascata LL (soma simples)",
                })
                dre["provisao_imposto_de_renda"] = round(-provisao, 2)

        # Fase 2: cascata pós-IRPJ até lucro_liquido
        _CASCADE_PHASE2 = [
            ("lucro_antes_participacoes", ["lucro_antes_imposto_de_renda", "provisao_imposto_de_renda", "csll"]),
            ("lucro_antes_participacao_minoritaria", ["lucro_antes_participacoes", "participacoes_gratificacoes_estatutarias"]),
            ("lucro_liquido", ["lucro_antes_participacao_minoritaria", "participacao_minoritarios"]),
        ]
        for field, components in _CASCADE_PHASE2:
            _recalc(field, components)

        if postprocessed:
            result["_postprocessed"] = postprocessed

        return result

    @classmethod
    def _postprocess_validate_fontes(cls, result):
        """Valida que fontes estruturadas (array) somam algebricamente para o
        valor do campo correspondente. Divergências viram avisos em _postprocessed.
        Não altera valores — observação pura para o painel Pontos de Atenção."""
        if not isinstance(result, dict) or result.get("error"):
            return result
        fontes = result.get("fontes")
        if not isinstance(fontes, dict):
            return result

        for campo_path, fonte in fontes.items():
            if not isinstance(fonte, list):
                continue  # string ou outro tipo — sem validar

            try:
                soma = sum(
                    (item.get("valor", 0) or 0) * (-1 if item.get("operacao") == "-" else 1)
                    for item in fonte
                    if isinstance(item, dict)
                )
            except (TypeError, ValueError):
                continue

            parts = campo_path.split(".")
            obj = result
            for p in parts[:-1]:
                obj = obj.get(p, {}) if isinstance(obj, dict) else {}
            if not isinstance(obj, dict):
                continue
            campo_valor = obj.get(parts[-1])
            if not isinstance(campo_valor, (int, float)):
                continue

            tolerance = max(1.0, abs(campo_valor) * 0.01)
            if abs(soma - campo_valor) <= tolerance:
                continue

            postprocessed = result.setdefault("_postprocessed", [])
            postprocessed.append({
                "tipo": "aviso_fonte_inconsistente",
                "campo": campo_path,
                "valor_campo": round(campo_valor, 2),
                "soma_fontes": round(soma, 2),
                "diferenca": round(campo_valor - soma, 2),
                "motivo": (
                    "A soma algébrica dos itens registrados em fontes não bate com "
                    "o valor extraído do campo. Possível erro de registro do LLM "
                    "(operador trocado, item esquecido, ou valor individual errado)."
                ),
            })

        return result

    @staticmethod
    def _recover_truncated_json(raw: str):
        """Try to recover complete JSON records from a truncated response.
        If the model output was cut off mid-JSON, find the last complete
        top-level object in the array and return what we can parse."""
        if not raw or raw[0] != "[":
            return None
        # Strategy: find each complete top-level {} in the array
        # by scanning for }, then try json.loads on the array up to that point
        last_good = None
        depth = 0
        in_str = False
        escape_next = False
        for i, ch in enumerate(raw):
            if escape_next:
                escape_next = False
                continue
            if ch == "\\":
                if in_str:
                    escape_next = True
                continue
            if ch == '"' and not escape_next:
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    # Complete top-level object found
                    last_good = i
        if last_good is None:
            return None
        # Try to parse array up to last complete object + closing bracket
        candidate = raw[:last_good + 1].rstrip().rstrip(",") + "\n]"
        try:
            parsed = json.loads(candidate)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            return None

    def predict(self, context, model_input):
        import pandas as pd

        client = self._get_client()

        if isinstance(model_input, pd.DataFrame):
            texts = model_input.iloc[:, 0].tolist()
        elif isinstance(model_input, dict):
            val = model_input.get("text", model_input.get("inputs", ""))
            texts = [val] if isinstance(val, str) else val
        elif isinstance(model_input, list):
            texts = [m.get("content", "") if isinstance(m, dict) else str(m) for m in model_input]
        else:
            texts = [str(model_input)]

        results = []
        for text in texts:
            response = client.chat.completions.create(
                model=os.environ.get("OCR_MODEL", "databricks-claude-sonnet-4"),
                messages=[
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user",   "content": self._user_prompt(text)},
                ],
                temperature=0,
                max_tokens=64000,
            )
            extract_usage = {
                "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
                "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            }
            raw = response.choices[0].message.content.strip()
            finish_reason = getattr(response.choices[0], "finish_reason", None)

            # Always search for a fenced ```json block first (model may prepend reasoning text)
            import re as _re
            _fenced = _re.search(r"```json\s*(.*?)\s*```", raw, _re.DOTALL)
            if _fenced:
                raw = _fenced.group(1).strip()
            elif _re.search(r"```json", raw):
                # Incomplete fenced block (truncated output — no closing ```)
                _fenced_open = _re.search(r"```json\s*(.*)", raw, _re.DOTALL)
                if _fenced_open:
                    raw = _fenced_open.group(1).strip()
            elif raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.split("```")[0].strip()
            elif not (raw.startswith("[") or raw.startswith("{")):
                for start in ["[{", "[ {", "[\n{", "[\r\n{", "[  {"]:
                    idx = raw.find(start)
                    if idx >= 0:
                        raw = raw[idx:]
                        break
                else:
                    idx = raw.find("[")
                    if idx >= 0:
                        raw = raw[idx:]
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # Output may have been truncated — try to recover complete records
                parsed = self._recover_truncated_json(raw)

            if parsed is not None:
                if isinstance(parsed, dict):
                    parsed = [parsed]
                # Pós-processamento: corrige inconsistências aritméticas do LLM.
                # (a) outros_* recalculado como resíduo do grupo (AC/ANC/PC/PNC)
                # (b) DRE: total_despesas_operacionais = soma componentes; sinal IRPJ
                parsed = [self._postprocess_afac_in_pl(r) for r in parsed]
                parsed = [self._postprocess_reclassify(r) for r in parsed]
                parsed = [self._postprocess_anti_duplicacao(r) for r in parsed]
                parsed = [self._postprocess_outros(r) for r in parsed]
                parsed = [self._postprocess_cascata_dre(r) for r in parsed]
                parsed = [self._postprocess_validate_fontes(r) for r in parsed]
                usage_summary = {
                    "extract_prompt_tokens": extract_usage["prompt_tokens"],
                    "extract_completion_tokens": extract_usage["completion_tokens"],
                    "total_prompt_tokens": extract_usage["prompt_tokens"],
                    "total_completion_tokens": extract_usage["completion_tokens"],
                    "total_tokens": extract_usage["prompt_tokens"] + extract_usage["completion_tokens"],
                }
                for r in parsed:
                    if not r.get("error"):
                        r["_assessment"] = []
                        r["_usage"] = usage_summary
                results.append(parsed)
            else:
                results.append([{"error": "parse_failed",
                                 "raw": raw[:2000],
                                 "finish_reason": str(finish_reason),
                                 "completion_tokens": extract_usage["completion_tokens"]}])

        return results if len(results) > 1 else results[0]


# Necessário para code-based logging
mlflow.models.set_model(TechFinExtractorAgent())
