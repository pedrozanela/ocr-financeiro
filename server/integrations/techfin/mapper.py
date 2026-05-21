"""Mapper de extracted_json (formato interno) para payload da API Techfin (PARC).

Diferenças relativas ao formato interno do app:
- tipo_entidade e razao_social ficam FORA do JSON (estão nas colunas SQL); são passados explicitamente.
- CNPJ vai sem máscara (apenas dígitos).
- tipo_demonstrativo, tipo_documento e numeroMeses são integers.
- Totais derivados (total_deducoes, total_custo, total_despesas_operacionais, total_receitas_financeiras)
  são recalculados via soma simples dos componentes se não estiverem presentes.
- None/missing → 0.0 (a API rejeita null em campos numéricos).
"""
from typing import Optional


# Conversões para legado: se extracted_json vier com STRING (modelo antigo), converte para INT
TIPO_DEMONSTRATIVO_MAP = {
    "ANUAL": 1,
    "SEMESTRAL": 2,
    "TRIMESTRAL": 3,
    "MENSAL": 4,
}

TIPO_DOCUMENTO_MAP = {
    "BALANCO": 1,
    "BALANÇO": 1,
    "BALANCETE": 2,
}

# Fallback para numeroMeses quando ausente, derivado de tipo_demonstrativo
NUMERO_MESES_FALLBACK = {1: 12, 2: 6, 3: 3, 4: 1}


def _z(value) -> float:
    """None/'' → 0.0. A API espera 0.0 explícito em todos os campos numéricos."""
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _clean_cnpj(cnpj) -> str:
    """Remove máscara. A API espera apenas dígitos."""
    if not cnpj:
        return ""
    return "".join(c for c in str(cnpj) if c.isdigit())


def _tipo_demonstrativo_to_int(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return TIPO_DEMONSTRATIVO_MAP.get(value.strip().upper(), 1)
    return 1


def _tipo_documento_to_int(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return TIPO_DOCUMENTO_MAP.get(value.strip().upper(), 1)
    return 1


def _numero_meses(ident: dict, tipo_demo: int) -> int:
    """Pega numeroMeses do JSON; fallback baseado em tipo_demonstrativo."""
    v = ident.get("numeroMeses") or ident.get("numero_meses") or ident.get("numeromeses")
    if v is not None:
        try:
            n = int(v)
            if 1 <= n <= 12:
                return n
        except (ValueError, TypeError):
            pass
    return NUMERO_MESES_FALLBACK.get(tipo_demo, 12)


def map_extracted_to_techfin(
    extracted: dict,
    tipo_entidade: str,
    razao_social: str,
) -> dict:
    """Converte extracted_json + metadados para payload da API Techfin.

    Args:
        extracted: JSON revisado (sem fontes, sem _postprocessed).
        tipo_entidade: vem da coluna SQL (ex: 'INDIVIDUAL', 'CONSOLIDADO').
        razao_social: vem da coluna SQL.
    """
    ident = extracted.get("identificacao", {}) or {}
    ac = extracted.get("ativo_circulante", {}) or {}
    anc = extracted.get("ativo_nao_circulante", {}) or {}
    ap = extracted.get("ativo_permanente", {}) or {}
    pc = extracted.get("passivo_circulante", {}) or {}
    pnc = extracted.get("passivo_nao_circulante", {}) or {}
    pl = extracted.get("patrimonio_liquido", {}) or {}
    dre = extracted.get("dre", {}) or {}

    tipo_demo = _tipo_demonstrativo_to_int(ident.get("tipo_demonstrativo"))
    tipo_doc = _tipo_documento_to_int(ident.get("tipo_documento"))

    # Totais derivados — recalcular sempre via soma simples (sinais já vêm do documento, Regra 22).
    # Mesmo se o LLM extraiu, recalculamos para garantir consistência.
    total_deducoes = (
        _z(dre.get("vendas_anuladas"))
        + _z(dre.get("abatimentos"))
        + _z(dre.get("impostos_incidentes_sobre_vendas"))
    )
    total_custo = (
        _z(dre.get("custo_servicos_produtos_mercadorias_vendidas"))
        + _z(dre.get("superveniencias_ativas"))
    )
    total_despesas_operacionais = (
        _z(dre.get("despesas_com_vendas"))
        + _z(dre.get("provisao_para_devedores_duvidosos"))
        + _z(dre.get("outras_receitas_despesas_operacionais"))
        + _z(dre.get("despesas_administrativas"))
        + _z(dre.get("despesas_tributarias"))
        + _z(dre.get("despesas_gerais"))
        + _z(dre.get("depreciacao"))
        + _z(dre.get("amortizacao"))
    )
    total_receitas_financeiras = (
        _z(dre.get("receitas_financeiras"))
        + _z(dre.get("variacao_cambial_nao_recebida"))
    )
    despesas_financeiras_total = (
        _z(dre.get("encargos_financeiros"))
        + _z(dre.get("descontos_concedidos"))
        + _z(dre.get("variacao_cambial_nao_paga"))
    )
    # despesas_financeiras pode estar já calculado no JSON; usa soma se zero.
    despesas_financeiras = _z(dre.get("despesas_financeiras")) or despesas_financeiras_total

    return {
        "tipo_entidade": tipo_entidade or "INDIVIDUAL",
        "razao_social": razao_social or "",
        "cnpj": _clean_cnpj(extracted.get("cnpj") or ident.get("cnpj", "")),
        "identificacao": {
            "periodo": ident.get("periodo", ""),
            "tipo_demonstrativo": tipo_demo,
            "moeda": ident.get("moeda", "REAL"),
            "escala_valores": ident.get("escala_valores", "UNIDADE"),
            "numeroMeses": _numero_meses(ident, tipo_demo),
            "tipo_documento": tipo_doc,
        },
        "ativo_circulante": {
            "disponibilidades": _z(ac.get("disponibilidades")),
            "titulos_a_receber": _z(ac.get("titulos_a_receber")),
            "estoques": _z(ac.get("estoques")),
            "adiantamentos": _z(ac.get("adiantamentos")),
            "impostos_a_recuperar": _z(ac.get("impostos_a_recuperar")),
            "outros_ativos_circulantes": _z(ac.get("outros_ativos_circulantes")),
            "conta_corrente_socios_control_colig": _z(ac.get("conta_corrente_socios_control_colig")),
            "outros_ativos_financeiros": _z(ac.get("outros_ativos_financeiros")),
            "total_ativo_circulante": _z(ac.get("total_ativo_circulante")),
        },
        "ativo_nao_circulante": {
            "titulos_a_receber": _z(anc.get("titulos_a_receber")),
            "estoques": _z(anc.get("estoques")),
            "adiantamentos": _z(anc.get("adiantamentos")),
            "impostos_a_recuperar": _z(anc.get("impostos_a_recuperar")),
            "despesas_pagas_antecipadamente": _z(anc.get("despesas_pagas_antecipadamente")),
            "conta_corrente_socios_control_colig": _z(anc.get("conta_corrente_socios_control_colig")),
            "outros_realizavel_a_longo_prazo": _z(anc.get("outros_realizavel_a_longo_prazo")),
            "total_ativo_nao_circulante": _z(anc.get("total_ativo_nao_circulante")),
        },
        "ativo_permanente": {
            "investimentos": _z(ap.get("investimentos")),
            "imobilizado": _z(ap.get("imobilizado")),
            "intangivel_diferido": _z(ap.get("intangivel_diferido")),
            "total_ativo_permanente": _z(ap.get("total_ativo_permanente")),
        },
        "ativo_total": _z(extracted.get("ativo_total")),
        "passivo_circulante": {
            "fornecedores": _z(pc.get("fornecedores")),
            "financiamentos_com_instituicoes_de_credito": _z(pc.get("financiamentos_com_instituicoes_de_credito")),
            "salarios_contribuicoes": _z(pc.get("salarios_contribuicoes")),
            "tributos": _z(pc.get("tributos")),
            "adiantamentos": _z(pc.get("adiantamentos")),
            "conta_corrente_socios_coligadas_controladas": _z(pc.get("conta_corrente_socios_coligadas_controladas")),
            "outros_passivos_circulante": _z(pc.get("outros_passivos_circulante")),
            "provisoes": _z(pc.get("provisoes")),
            "outros_passivos_financeiros": _z(pc.get("outros_passivos_financeiros")),
            "total_passivo_circulante": _z(pc.get("total_passivo_circulante")),
        },
        "passivo_nao_circulante": {
            "fornecedores": _z(pnc.get("fornecedores")),
            "financiamentos_com_instituicoes_de_credito": _z(pnc.get("financiamentos_com_instituicoes_de_credito")),
            "salarios_contribuicoes": _z(pnc.get("salarios_contribuicoes")),
            "tributos": _z(pnc.get("tributos")),
            "adiantamentos": _z(pnc.get("adiantamentos")),
            "conta_corrente_socios_coligadas_controladas": _z(pnc.get("conta_corrente_socios_coligadas_controladas")),
            "outros_passivos_nao_circulantes": _z(pnc.get("outros_passivos_nao_circulantes")),
            "provisoes": _z(pnc.get("provisoes")),
            "total_passivo_nao_circulante": _z(pnc.get("total_passivo_nao_circulante")),
        },
        "patrimonio_liquido": {
            "capital_social": _z(pl.get("capital_social")),
            "reserva_de_capital": _z(pl.get("reserva_de_capital")),
            "reservas_de_lucro": _z(pl.get("reservas_de_lucro")),
            "reservas_de_reavaliacao": _z(pl.get("reservas_de_reavaliacao")),
            "outras_reservas": _z(pl.get("outras_reservas")),
            "lucros_ou_prejuizos_acumulados": _z(pl.get("lucros_ou_prejuizos_acumulados")),
            "acoes_em_tesouraria": _z(pl.get("acoes_em_tesouraria")),
            "total_patrimonio_liquido": _z(pl.get("total_patrimonio_liquido")),
        },
        "passivo_total": _z(extracted.get("passivo_total")),
        "dre": {
            "receita_venda_produto_mercadoria": _z(dre.get("receita_venda_produto_mercadoria")),
            "receita_servicos_arrendamento": _z(dre.get("receita_servicos_arrendamento")),
            "receita_operacional_bruta": _z(dre.get("receita_operacional_bruta")),
            "vendas_anuladas": _z(dre.get("vendas_anuladas")),
            "abatimentos": _z(dre.get("abatimentos")),
            "impostos_incidentes_sobre_vendas": _z(dre.get("impostos_incidentes_sobre_vendas")),
            "total_deducoes": total_deducoes,
            "incentivos_a_exportacoes": _z(dre.get("incentivos_a_exportacoes")),
            "receita_operacional_liquida": _z(dre.get("receita_operacional_liquida")),
            "custo_servicos_produtos_mercadorias_vendidas": _z(dre.get("custo_servicos_produtos_mercadorias_vendidas")),
            "superveniencias_ativas": _z(dre.get("superveniencias_ativas")),
            "total_custo": total_custo,
            "lucro_bruto": _z(dre.get("lucro_bruto")),
            "despesas_com_vendas": _z(dre.get("despesas_com_vendas")),
            "provisao_para_devedores_duvidosos": _z(dre.get("provisao_para_devedores_duvidosos")),
            "outras_receitas_despesas_operacionais": _z(dre.get("outras_receitas_despesas_operacionais")),
            "despesas_administrativas": _z(dre.get("despesas_administrativas")),
            "despesas_tributarias": _z(dre.get("despesas_tributarias")),
            "despesas_gerais": _z(dre.get("despesas_gerais")),
            "depreciacao": _z(dre.get("depreciacao")),
            "amortizacao": _z(dre.get("amortizacao")),
            "total_despesas_operacionais": total_despesas_operacionais,
            "lucro_operacional": _z(dre.get("lucro_operacional")),
            "encargos_financeiros": _z(dre.get("encargos_financeiros")),
            "descontos_concedidos": _z(dre.get("descontos_concedidos")),
            "variacao_cambial_nao_paga": _z(dre.get("variacao_cambial_nao_paga")),
            "despesas_financeiras": despesas_financeiras,
            "receitas_financeiras": _z(dre.get("receitas_financeiras")),
            "variacao_cambial_nao_recebida": _z(dre.get("variacao_cambial_nao_recebida")),
            "total_receitas_financeiras": total_receitas_financeiras,
            "lucro_financeiro": _z(dre.get("lucro_financeiro")),
            "resultado_de_equivalencia_patrimonial": _z(dre.get("resultado_de_equivalencia_patrimonial")),
            "receita_nao_operacional": _z(dre.get("receita_nao_operacional")),
            "despesa_nao_operacional": _z(dre.get("despesa_nao_operacional")),
            "saldo_correcao_monetaria": _z(dre.get("saldo_correcao_monetaria")),
            "resultado_alienacao_ativos": _z(dre.get("resultado_alienacao_ativos")),
            "lucro_antes_imposto_de_renda": _z(dre.get("lucro_antes_imposto_de_renda")),
            "provisao_imposto_de_renda": _z(dre.get("provisao_imposto_de_renda")),
            "csll": _z(dre.get("csll")),
            "lucro_antes_participacoes": _z(dre.get("lucro_antes_participacoes")),
            "participacoes_gratificacoes_estatutarias": _z(dre.get("participacoes_gratificacoes_estatutarias")),
            "lucro_antes_participacao_minoritaria": _z(dre.get("lucro_antes_participacao_minoritaria")),
            "participacao_minoritarios": _z(dre.get("participacao_minoritarios")),
            "lucro_liquido": _z(dre.get("lucro_liquido")),
        },
    }
