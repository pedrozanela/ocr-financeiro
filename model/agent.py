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
    "* Para cada elemento, preencha `tipo_entidade` com CONSOLIDADO, CONTROLADORA ou INDIVIDUAL, "
    "conforme o cabeçalho da coluna correspondente no documento.\n"
    "* Substitua qualquer valor null, vazio ou não informado por zero.\n"
    "* Formate todos os números para exibir exatamente 2 casas decimais, usando ponto como separador, "
    "mesmo que o valor seja inteiro ou zero (ex: 834988.00, 0.00, 15.50).\n"
    "* Preencha o objeto `fontes` no JSON de saída: para cada campo com valor diferente de zero, indique "
    "a(s) linha(s) do documento que originaram o valor. Use o caminho do campo como chave "
    "(ex: 'ativo_circulante.impostos_a_recuperar') e como valor uma descrição CURTA (máx. 80 caracteres): "
    "apenas o nome da conta principal e o valor. Se houver soma de múltiplas linhas, liste só os nomes "
    "sem os valores individuais (ex: 'Caixa + Bancos CC + Bancos investimento'). "
    "NÃO inclua cálculos detalhados, códigos contábeis ou valores parciais nas fontes.\n\n"
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

# Template do system prompt — preenchido em runtime com artifacts separados
SYSTEM_PROMPT = """\
Você é um especialista em análise de demonstrações financeiras brasileiras.
Sua tarefa é extrair informações estruturadas de documentos financeiros (Balanço Patrimonial e DRE).

{depara}

{regras}

## INSTRUÇÕES DE EXTRAÇÃO

{instructions}
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
        "Se o nome de uma linha do documento corresponder (exato ou similar) a um dos aliases abaixo,",
        "mapeie para o campo indicado. Quando houver ambiguidade, use o contexto da seção do balanço.",
        "",
    ]
    for section_key, field_entries in sections.items():
        label = _SECTION_LABELS.get(section_key, section_key)
        lines.append(f"### {label}")
        lines.append("")
        for path, conceito, aliases in field_entries:
            aliases_str = ", ".join(aliases) if isinstance(aliases, list) else aliases
            lines.append(f"**{path}** — {conceito}")
            lines.append(f"→ {aliases_str}")
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
        else:
            self.depara_section = ""

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
            depara=self.depara_section,
            regras=self.regras_section,
            instructions=INSTRUCTIONS,
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
