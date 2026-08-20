"""
Normalizacao de CPF.

Dois formatos circulam no sistema: apenas digitos ('99202743304') e formatado
('992.027.433-04'). O banco usa o formatado (DiariasControleServidor.cpf e
String(14), o tamanho exato do CPF com pontuacao) e a API pessoaSGA do Gestor
SEAD tambem exige o formatado. A entrada do usuario chega dos dois jeitos.
"""

TAMANHO_CPF = 11


def limpar_cpf(cpf):
    """Retorna somente os digitos do CPF. String vazia se a entrada for vazia."""
    if not cpf:
        return ''
    return ''.join(c for c in str(cpf) if c.isdigit())


def formatar_cpf(cpf):
    """
    Retorna o CPF no formato '999.999.999-99'.

    Aceita entrada com ou sem pontuacao. Retorna None se nao houver
    exatamente 11 digitos — o chamador decide o que fazer com CPF invalido.
    """
    digitos = limpar_cpf(cpf)
    if len(digitos) != TAMANHO_CPF:
        return None
    return f'{digitos[:3]}.{digitos[3:6]}.{digitos[6:9]}-{digitos[9:]}'


def variantes_cpf(cpf):
    """
    Retorna os formatos equivalentes de um CPF, para consultas que precisam
    casar com registros gravados de formas diferentes:

        ['992.027.433-04', '99202743304']

    Lista vazia se o CPF for invalido.
    """
    formatado = formatar_cpf(cpf)
    if not formatado:
        return []
    return [formatado, limpar_cpf(cpf)]
