"""
Escolha do exercicio SIAFE usado para atualizar um contrato.

O SIAFE mantem um registro do contrato POR EXERCICIO, e eles podem divergir:
o contrato 25018627 foi atualizado no exercicio 2025 (LICITADO, contratado
preenchido) enquanto o registro de 2026 continuou "A Contratar" e vazio.

Politica:
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) fica com o registro mais completo (campos preenchidos + fiscais + aditivos)
  3) empate -> ano corrente
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


def _preenchido(valor):
    if valor is None:
        return False
    if isinstance(valor, str):
        return valor.strip() not in _VALORES_VAZIOS
    return True


def pontuar_completude(data, colunas):
    """Quantidade de campos preenchidos + fiscais + aditivos."""
    campos = sum(1 for c in colunas if _preenchido(data.get(c)))
    fiscais = len(data.get("responsaveisContrato") or [])
    aditivos = len(data.get("aditivos") or [])
    return campos + fiscais + aditivos


def escolher_registro(registros_por_ano, colunas, ano_corrente):
    """Retorna (ano, data) do registro escolhido ou (None, None)."""
    if ano_corrente not in registros_por_ano:
        return None, None
    ano = max(
        registros_por_ano,
        key=lambda a: (pontuar_completude(registros_por_ano[a], colunas), a == ano_corrente),
    )
    return ano, registros_por_ano[ano]
