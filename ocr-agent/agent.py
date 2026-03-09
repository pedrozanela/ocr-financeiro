"""
TechFin OCR v4 - Information Extraction Agent
Extrai dados financeiros estruturados de documentos (Balanço Patrimonial + DRE).

Code-based MLflow model: https://mlflow.org/docs/latest/models.html#models-from-code
"""
import json
import os
import mlflow
from mlflow.pyfunc import PythonModel

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
    "* Preencha o objeto `fontes` no JSON de saída: para cada campo extraído, indique qual texto exato do PDF "
    "originou o valor. Use o caminho do campo como chave (ex: 'ativo_circulante.impostos_a_recuperar') "
    "e como valor descreva brevemente: o(s) nome(s) da(s) linha(s) do documento, os valores individuais "
    "e a operação realizada (ex: soma, leitura direta). "
    "Exemplo: 'Impostos a recuperar (2.411) + IRPJ e CSLL a compensar (4.596) = 7.007 (escala: milhares)'. "
    "Se o valor foi lido diretamente de uma única linha, indique apenas o nome da linha e o valor. "
    "Inclua fontes apenas para campos com valor diferente de zero."
)

SYSTEM_PROMPT = """Você é um especialista em análise de demonstrações financeiras brasileiras.
Sua tarefa é extrair informações estruturadas de documentos financeiros (Balanço Patrimonial e DRE).

Instruções adicionais:
{instructions}

Retorne SOMENTE um JSON array válido seguindo exatamente o schema fornecido. Sem texto adicional."""


class TechFinExtractorAgent(PythonModel):

    def load_context(self, context):
        from openai import OpenAI
        import mlflow

        # Pegar token via Databricks SDK (funciona tanto local quanto no serving endpoint)
        # O DATABRICKS_TOKEN é injetado via environment_vars do endpoint config
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            token = w.config.token
            host = w.config.host
        except Exception:
            token = os.environ.get("DATABRICKS_TOKEN", "token")
            host = os.environ.get("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")

        if not host.startswith("http"):
            host = f"https://{host}"

        self.client = OpenAI(
            api_key=token,
            base_url=f"{host.rstrip('/')}/serving-endpoints",
        )
        with open(context.artifacts["output_schema"]) as f:
            self.output_schema = json.load(f)

    def _build_user_prompt(self, text: str) -> str:
        schema_str = json.dumps(self.output_schema, ensure_ascii=False, indent=2)
        return (
            f"Extraia as informações financeiras do seguinte documento e retorne um JSON "
            f"seguindo exatamente este schema:\n\n{schema_str}\n\n"
            f"DOCUMENTO:\n{text}"
        )

    def predict(self, context, model_input):
        import pandas as pd

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
            response = self.client.chat.completions.create(
                model="databricks-claude-3-7-sonnet",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT.format(instructions=INSTRUCTIONS)},
                    {"role": "user", "content": self._build_user_prompt(text)},
                ],
                temperature=0,
                max_tokens=16000,
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            try:
                parsed = json.loads(raw)
                # Model returns an array per document; wrap in list if dict (fallback)
                if isinstance(parsed, dict):
                    parsed = [parsed]
                results.append(parsed)
            except json.JSONDecodeError:
                results.append([{"error": "parse_failed", "raw": raw[:500]}])

        return results if len(results) > 1 else results[0]


# Necessário para code-based logging
mlflow.models.set_model(TechFinExtractorAgent())
