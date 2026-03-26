"""
Pipeline completo: gera few-shot examples a partir das correções e deploya nova versão.
Executar: cd techfin && conda run -n base python scripts/update_fewshot_and_deploy.py [--all]
"""
import subprocess
import sys
import os

_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_DIR)


def run(cmd: list[str], desc: str):
    print(f"\n{'='*60}")
    print(f"  {desc}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=_ROOT)
    if result.returncode != 0:
        print(f"\n✗ Falhou: {desc}")
        sys.exit(1)
    print(f"✓ {desc}")


def main():
    extra_args = [a for a in sys.argv[1:] if a.startswith("--")]

    # 1. Gera few_shot_examples.json
    run(
        [sys.executable, os.path.join(_DIR, "generate_fewshot.py")] + extra_args,
        "Gerando few-shot examples a partir das correções",
    )

    # 2. Verifica se gerou o arquivo
    fewshot = os.path.join(_ROOT, "model", "few_shot_examples.json")
    if os.path.exists(fewshot):
        import json
        with open(fewshot) as f:
            examples = json.load(f)
        print(f"\n  {len(examples)} exemplos gerados")
    else:
        print("\n  Nenhum exemplo gerado (sem correções)")

    # 3. Loga nova versão e atualiza endpoint
    run(
        [sys.executable, os.path.join(_DIR, "log_new_version.py")],
        "Logando nova versão do modelo e atualizando endpoint",
    )

    print("\n" + "="*60)
    print("  Pipeline completo!")
    print("="*60)


if __name__ == "__main__":
    main()
