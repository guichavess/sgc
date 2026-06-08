"""
Normalização do campo `competencia` para o formato canônico MM/YYYY.

A API SIAFE retorna o campo em formatos distintos por endpoint:
  - PD:          'MM/YYYY' direto (ex.: '12/2024')
  - OB:          descritivo (ex.: '12 - Dezembro/2024', '13 - Encerramento - 13/2024')
  - Empenho/NL:  na maioria das vezes não vem direto — extraído de classificadores

Esta função aceita qualquer das formas e retorna sempre `MM/YYYY` com mês com
2 dígitos, ou `None` se não der pra reconhecer.

Exemplos:
  '12/2024'                       → '12/2024'
  '1 - Janeiro/2025'              → '01/2025'
  '12 - Dezembro/2024'            → '12/2024'
  '13 - Encerramento - 13/2024'   → '13/2024'
  '', None, 'nan'                 → None
"""
import re

# Mês no início da string (tolera prefixo numérico antes de qualquer separador)
_PATTERN_MES = re.compile(r'^(\d{1,2})\b')
# Ano no final da string, precedido de '/'
_PATTERN_ANO = re.compile(r'/(\d{4})\s*$')


def normalizar_competencia(valor):
    """Converte qualquer formato suportado da API SIAFE para 'MM/YYYY' (ou None)."""
    if valor is None:
        return None

    s = str(valor).strip()
    if not s or s.lower() in ('nan', 'none', 'nat'):
        return None

    m_mes = _PATTERN_MES.match(s)
    m_ano = _PATTERN_ANO.search(s)
    if not m_mes or not m_ano:
        return None

    return f'{m_mes.group(1).zfill(2)}/{m_ano.group(1)}'
