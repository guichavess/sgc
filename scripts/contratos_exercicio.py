"""
Escolha do exercicio SIAFE usado para atualizar um contrato.

O SIAFE mantem um registro do contrato POR EXERCICIO, e eles podem divergir.
No contrato 25018627 o registro de 2026 ficou "A Contratar" sem contratado
(planejamento: R$ 2,97 mi, Pregao) enquanto o de 2025 recebeu a contratacao
(LICITADO, DATEN, contrato 31/2026, R$ 1,33 mi). Os dois tinham a mesma
quantidade de campos preenchidos: o que distingue e ter contratado.

Politica:
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) prefere o registro com contratado identificado (codigoContratado)
  3) ambos ou nenhum com contratado -> ano corrente
  4) sem registro do ano corrente (erro na consulta) -> nao escolhe nada, para
     um erro transitorio nao sobrescrever o banco com dado do ano anterior

Modulo sem efeitos colaterais (atualizar_contratos.py autentica no import).
"""

# Valores que o SIAFE devolve no lugar de um campo vazio
_VALORES_VAZIOS = {"", "-"}


def exercicios_a_consultar(codigo, ano_corrente):
    """Exercicios a consultar, ano corrente primeiro.

    O codigo do contrato comeca com o ano de criacao (25018627 -> 2025).
    """
    prefixo = str(codigo or "")[:2]
    if len(prefixo) == 2 and prefixo.isdigit() and 2000 + int(prefixo) < ano_corrente:
        return [ano_corrente, ano_corrente - 1]
    return [ano_corrente]


def tem_contratado(data):
    """True se o registro tem contratado identificado (nao e so planejamento)."""
    codigo = data.get("codigoContratado")
    if codigo is None:
        return False
    return str(codigo).strip() not in _VALORES_VAZIOS


def escolher_registro(registros_por_ano, ano_corrente):
    """Retorna (ano, data) do registro escolhido ou (None, None)."""
    if ano_corrente not in registros_por_ano:
        return None, None
    ano = max(
        registros_por_ano,
        key=lambda a: (tem_contratado(registros_por_ano[a]), a == ano_corrente),
    )
    return ano, registros_por_ano[ano]
