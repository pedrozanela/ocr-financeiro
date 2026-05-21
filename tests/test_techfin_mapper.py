"""Testes do mapper Techfin. Roda standalone: pytest tests/test_techfin_mapper.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.integrations.techfin.mapper import (
    map_extracted_to_techfin,
    _z, _clean_cnpj, _tipo_demonstrativo_to_int, _tipo_documento_to_int, _numero_meses,
)


def test_z_handles_none():
    assert _z(None) == 0.0
    assert _z("") == 0.0
    assert _z(0) == 0.0
    assert _z(123.45) == 123.45
    assert _z("123.45") == 123.45
    assert _z("invalid") == 0.0


def test_clean_cnpj_strips_mask():
    assert _clean_cnpj("12.345.678/0001-90") == "12345678000190"
    assert _clean_cnpj("12345678000190") == "12345678000190"
    assert _clean_cnpj("") == ""
    assert _clean_cnpj(None) == ""
    assert _clean_cnpj("  12.345.678/0001-90  ") == "12345678000190"


def test_tipo_demonstrativo_int_passthrough():
    assert _tipo_demonstrativo_to_int(1) == 1
    assert _tipo_demonstrativo_to_int(4) == 4


def test_tipo_demonstrativo_string_to_int():
    assert _tipo_demonstrativo_to_int("ANUAL") == 1
    assert _tipo_demonstrativo_to_int("anual") == 1
    assert _tipo_demonstrativo_to_int(" Anual ") == 1
    assert _tipo_demonstrativo_to_int("SEMESTRAL") == 2
    assert _tipo_demonstrativo_to_int("TRIMESTRAL") == 3
    assert _tipo_demonstrativo_to_int("MENSAL") == 4
    assert _tipo_demonstrativo_to_int("DESCONHECIDO") == 1  # default


def test_tipo_demonstrativo_numeric_string():
    assert _tipo_demonstrativo_to_int("2") == 2


def test_tipo_documento_string_to_int():
    assert _tipo_documento_to_int("BALANCO") == 1
    assert _tipo_documento_to_int("BALANÇO") == 1
    assert _tipo_documento_to_int("BALANCETE") == 2
    assert _tipo_documento_to_int(1) == 1
    assert _tipo_documento_to_int(None) == 1


def test_numero_meses_explicit():
    assert _numero_meses({"numeroMeses": 6}, 2) == 6
    assert _numero_meses({"numero_meses": 3}, 3) == 3
    assert _numero_meses({"numeromeses": 12}, 1) == 12


def test_numero_meses_fallback_by_tipo():
    assert _numero_meses({}, 1) == 12  # anual
    assert _numero_meses({}, 2) == 6   # semestral
    assert _numero_meses({}, 3) == 3   # trimestral
    assert _numero_meses({}, 4) == 1   # mensal


def test_total_deducoes_computed():
    extracted = {
        "dre": {
            "vendas_anuladas": 100.0,
            "abatimentos": 50.0,
            "impostos_incidentes_sobre_vendas": 4199920.36,
        },
    }
    out = map_extracted_to_techfin(extracted, "INDIVIDUAL", "Test SA")
    assert out["dre"]["total_deducoes"] == 4200070.36


def test_total_despesas_operacionais_computed():
    extracted = {
        "dre": {
            "despesas_com_vendas": 1778268.89,
            "despesas_administrativas": 4471833.78,
            "despesas_gerais": 2018179.91,
        },
    }
    out = map_extracted_to_techfin(extracted, "INDIVIDUAL", "Test SA")
    assert out["dre"]["total_despesas_operacionais"] == 8268282.58


def test_payload_matches_postman_example():
    """Compara com o payload do Postman (AGROBECKER) para campos chave."""
    extracted = {
        "cnpj": "97.741.211/0001-17",
        "identificacao": {
            "periodo": "2023-12-31",
            "tipo_demonstrativo": 1,
            "moeda": "REAL",
            "escala_valores": "UNIDADE",
            "numeroMeses": 12,
            "tipo_documento": 1,
        },
        "ativo_circulante": {
            "disponibilidades": 397604.41,
            "titulos_a_receber": 2430196.92,
            "estoques": 3580147.5,
            "total_ativo_circulante": 6407948.83,
        },
        "ativo_total": 523182825.04,
        "passivo_total": 523182825.04,
        "dre": {
            "receita_operacional_bruta": 47308653.85,
            "impostos_incidentes_sobre_vendas": 4199920.36,
            "lucro_liquido": 5570856.69,
        },
    }
    out = map_extracted_to_techfin(extracted, "INDIVIDUAL", "AGROBECKER AGRICULTURA E PECUARIA LTDA")

    assert out["tipo_entidade"] == "INDIVIDUAL"
    assert out["razao_social"] == "AGROBECKER AGRICULTURA E PECUARIA LTDA"
    assert out["cnpj"] == "97741211000117"  # sem máscara
    assert out["identificacao"]["periodo"] == "2023-12-31"
    assert out["identificacao"]["tipo_demonstrativo"] == 1
    assert out["identificacao"]["numeroMeses"] == 12
    assert out["identificacao"]["tipo_documento"] == 1
    assert out["ativo_circulante"]["disponibilidades"] == 397604.41
    assert out["ativo_circulante"]["total_ativo_circulante"] == 6407948.83
    assert out["ativo_total"] == 523182825.04
    assert out["passivo_total"] == 523182825.04
    assert out["dre"]["receita_operacional_bruta"] == 47308653.85
    assert out["dre"]["impostos_incidentes_sobre_vendas"] == 4199920.36
    assert out["dre"]["total_deducoes"] == 4199920.36  # soma de vendas_anuladas+abatimentos+impostos
    assert out["dre"]["lucro_liquido"] == 5570856.69


def test_missing_fields_become_zero():
    """Campos ausentes no extracted_json viram 0.0 (não null)."""
    extracted = {"ativo_total": 100.0}
    out = map_extracted_to_techfin(extracted, "INDIVIDUAL", "Test")
    assert out["ativo_circulante"]["disponibilidades"] == 0.0
    assert out["passivo_circulante"]["fornecedores"] == 0.0
    assert out["patrimonio_liquido"]["capital_social"] == 0.0
    assert out["dre"]["receita_operacional_bruta"] == 0.0
    assert out["passivo_total"] == 0.0


def test_tipo_entidade_e_razao_social_da_coluna():
    """tipo_entidade e razao_social vêm dos argumentos, não do JSON."""
    extracted = {"identificacao": {"tipo_entidade": "DEVE_SER_IGNORADO"}}
    out = map_extracted_to_techfin(extracted, "CONSOLIDADO", "Empresa Consolidada SA")
    assert out["tipo_entidade"] == "CONSOLIDADO"
    assert out["razao_social"] == "Empresa Consolidada SA"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
