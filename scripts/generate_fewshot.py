"""
Gera few_shot_examples.json a partir das correções confirmadas no Delta.
Executar: cd techfin && conda run -n base python scripts/generate_fewshot.py [--all]
"""
import json
import os
import subprocess
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    DATABRICKS_PROFILE as PROFILE,
    WAREHOUSE_ID,
    CORRECTIONS_TABLE,
    RESULTS_TABLE,
)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAX_EXAMPLES = 20
OUTPUT_FILE = os.path.join(_ROOT, "model", "few_shot_examples.json")


def run_sql(statement: str) -> list[dict]:
    """Executa SQL via Databricks Statement Execution API."""
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s",
        "format": "JSON_ARRAY",
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--json", payload, "--profile", PROFILE],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"SQL failed: {result.stderr}")
    resp = json.loads(result.stdout)
    status = resp.get("status", {})
    if status.get("state") != "SUCCEEDED":
        raise RuntimeError(f"SQL error: {status.get('error', {}).get('message', resp)}")
    columns = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    rows = resp.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in rows]


def categorize_error(campo: str, valor_extraido: str, valor_correto: str, comentario: str) -> str:
    """Classifica o tipo de erro para agrupamento."""
    try:
        v_ext = float(valor_extraido)
        v_cor = float(valor_correto)
        if v_cor == 0 and v_ext != 0:
            return "valor_deveria_ser_zero"
        if v_ext != 0 and v_cor != 0:
            ratio = v_ext / v_cor
            if abs(ratio - 1000) < 10 or abs(ratio - 0.001) < 0.0001:
                return "escala_errada"
            if abs(v_ext + v_cor) < 0.01:
                return "sinal_invertido"
    except (ValueError, ZeroDivisionError, TypeError):
        pass
    # Padrões comuns nos comentários
    if comentario:
        lower = comentario.lower()
        if "somar" in lower or "somado" in lower or "faltou somar" in lower:
            return "faltou_somar_subconta"
        if "subtrair" in lower or "subtraiu" in lower:
            return "faltou_subtrair"
        if "acumulado" in lower or "trimestre" in lower:
            return "periodo_errado"
    return "classificacao_incorreta"


def get_fonte(extracted_json_str: str, campo: str) -> str:
    """Extrai a fonte do campo a partir do extracted_json."""
    if not extracted_json_str:
        return ""
    try:
        data = json.loads(extracted_json_str)
        fontes = data.get("fontes", {})
        if campo in fontes:
            return fontes[campo]
        # Tenta caminho parcial (último segmento)
        last = campo.split(".")[-1]
        for k, v in fontes.items():
            if k.endswith(last):
                return v
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return ""


def main():
    use_all = "--all" in sys.argv

    # 1. Busca correções com contexto
    # Sempre exclui correções resolvidas (modelo já aprendeu)
    status_filter = "AND COALESCE(c.status, 'pendente') != 'resolvido'" if use_all else "AND c.status = 'confirmado'"
    sql = f"""
    SELECT
        c.campo,
        c.valor_extraido,
        c.valor_correto,
        c.comentario,
        c.document_name,
        c.tipo_entidade,
        c.periodo,
        r.extracted_json
    FROM {CORRECTIONS_TABLE} c
    LEFT JOIN {RESULTS_TABLE} r
        ON c.document_name = r.document_name
        AND COALESCE(c.tipo_entidade, '') = COALESCE(r.tipo_entidade, '')
        AND COALESCE(c.periodo, '') = COALESCE(r.periodo, '')
    WHERE 1=1 {status_filter}
    ORDER BY c.campo, c.document_name
    """

    print("Buscando correções...")
    rows = run_sql(sql)
    print(f"  {len(rows)} correções encontradas")

    if not rows:
        print("Nenhuma correção encontrada. Salvando arquivo vazio.")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    # 2. Agrupa por (campo, categoria_erro)
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for row in rows:
        cat = categorize_error(
            row["campo"],
            row["valor_extraido"] or "0",
            row["valor_correto"] or "0",
            row["comentario"] or "",
        )
        key = (row["campo"], cat)
        fonte = get_fonte(row.get("extracted_json"), row["campo"])
        groups[key].append({
            "document_name": row["document_name"],
            "tipo_entidade": row["tipo_entidade"],
            "periodo": row["periodo"],
            "valor_extraido": row["valor_extraido"],
            "valor_correto": row["valor_correto"],
            "comentario": row["comentario"] or "",
            "fonte": fonte,
        })

    # 3. Seleciona os exemplos mais representativos
    # Ordena por frequência (mais corrigidos primeiro)
    sorted_groups = sorted(groups.items(), key=lambda x: -len(x[1]))

    # Garante diversidade: primeiro pega o top de cada campo único
    seen_campos = set()
    selected = []
    remaining = []

    for (campo, cat), items in sorted_groups:
        if campo not in seen_campos and len(selected) < MAX_EXAMPLES:
            seen_campos.add(campo)
            selected.append(((campo, cat), items))
        else:
            remaining.append(((campo, cat), items))

    # Preenche slots restantes
    for entry in remaining:
        if len(selected) >= MAX_EXAMPLES:
            break
        selected.append(entry)

    # 4. Formata exemplos
    examples = []
    for (campo, cat), items in selected:
        # Usa o item mais representativo (com comentário e fonte, se possível)
        best = None
        for item in items:
            if item["comentario"] and item["fonte"]:
                best = item
                break
        if not best:
            for item in items:
                if item["comentario"]:
                    best = item
                    break
        if not best:
            best = items[0]

        # Monta explicação
        explicacao = best["comentario"]
        if not explicacao:
            if cat == "valor_deveria_ser_zero":
                explicacao = f"O valor de {campo.split('.')[-1]} deve ser zero; o montante deve ser reclassificado para outro campo."
            elif cat == "faltou_somar_subconta":
                explicacao = f"Faltou somar subcontas ao campo {campo}."
            elif cat == "escala_errada":
                explicacao = f"Erro de escala (milhares vs unidades) no campo {campo}."
            elif cat == "sinal_invertido":
                explicacao = f"Sinal invertido no campo {campo}."
            elif cat == "periodo_errado":
                explicacao = f"Usar valor acumulado do período, não o trimestral."
            else:
                explicacao = f"Classificação incorreta no campo {campo}."

        example = {
            "campo": campo,
            "categoria": cat,
            "frequencia": len(items),
            "valor_errado": best["valor_extraido"],
            "valor_correto": best["valor_correto"],
            "explicacao": explicacao[:200],  # Limita tamanho
        }
        if best["fonte"]:
            example["fonte_doc"] = best["fonte"][:200]
        if best["document_name"]:
            example["documento_exemplo"] = best["document_name"]

        examples.append(example)

    # 5. Salva
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(examples, f, ensure_ascii=False, indent=2)

    print(f"\n{len(examples)} exemplos gerados → {OUTPUT_FILE}")
    for ex in examples:
        print(f"  [{ex['frequencia']}x] {ex['campo']} ({ex['categoria']})")


if __name__ == "__main__":
    main()
