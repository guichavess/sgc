"""
Mescla dos registros de exercicios SIAFE usados para atualizar um contrato.

O SIAFE mantem um registro do contrato POR EXERCICIO, e eles podem divergir.
No contrato 25018627 o registro de 2026 ficou sem contratado (" - ") enquanto
o de 2025 recebeu a contratacao (DATEN, contrato 31/2026).

Politica (definida com o usuario em 2026-09-16):
  1) consulta o ano corrente e, se o contrato ja existia antes, o ano anterior
  2) base = ano corrente; campo vazio ("", "-", " - ", None...) e preenchido
     com o valor do ano anterior
  3) contratado (tipo, codigo, nome) e um bloco: sem codigo no ano corrente,
     os tres vem do ano anterior (o SIAFE devolve "PF" como tipo padrao)
  4) falha no ano corrente -> nao grava; falha transitoria no ano anterior ->
     nao grava (evita alternar entre dado mesclado e nao mesclado)

Modulo sem efeitos colaterais (atualizar_contratos.py autentica no import).
"""

# Textos que o SIAFE (ou digitacao) usa no lugar de um campo vazio
_TEXTOS_VAZIOS = {"null", "none", "n/a", "na", "nan"}

BLOCO_CONTRATADO = ("tipoContratado", "codigoContratado", "nomeContratado")
LISTAS = ("responsaveisContrato", "aditivos")


def exercicios_a_consultar(codigo, ano_corrente):
    """Exercicios a consultar, ano corrente primeiro.

    O codigo do contrato comeca com o ano de criacao (25018627 -> 2025).
    """
    prefixo = str(codigo or "")[:2]
    if len(prefixo) == 2 and prefixo.isdigit() and 2000 + int(prefixo) < ano_corrente:
        return [ano_corrente, ano_corrente - 1]
    return [ano_corrente]


def eh_vazio(valor):
    """None, lista/dict vazio, texto sem letras/numeros ("", "-", " - ") ou "null"/"N/A"."""
    if valor is None:
        return True
    if isinstance(valor, (list, dict)):
        return len(valor) == 0
    if isinstance(valor, str):
        texto = valor.strip()
        return not any(c.isalnum() for c in texto) or texto.lower() in _TEXTOS_VAZIOS
    return False


def falha_transitoria(status):
    """Erro de rede, timeout, 429 ou 5xx: vale tentar de novo na proxima execucao."""
    if status == "error":
        return True
    return isinstance(status, int) and (status == 429 or status >= 500)


def mesclar_registros(atual, anterior, colunas):
    """Retorna (registro mesclado, campos preenchidos com o ano anterior).

    Nao altera os dicionarios recebidos.
    """
    data = dict(atual)
    campos = []

    if eh_vazio(atual.get("codigoContratado")) and not eh_vazio(anterior.get("codigoContratado")):
        for c in BLOCO_CONTRATADO:
            if atual.get(c) != anterior.get(c):
                data[c] = anterior.get(c)
                campos.append(c)

    for c in list(colunas) + list(LISTAS):
        if c in BLOCO_CONTRATADO:
            continue  # só entra em bloco (acima), para não misturar empresas
        if eh_vazio(data.get(c)) and not eh_vazio(anterior.get(c)):
            data[c] = anterior.get(c)
            campos.append(c)

    return data, campos
