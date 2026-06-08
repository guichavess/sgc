"""
Helpers para normalizar campos retornados pelo SIAFE.
"""
import math


def normalizar_codigo_fonte(valor):
    """Retorna codigo de fonte sem pontuacao, preservando todos os digitos."""
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None

    texto = str(valor).strip()
    if not texto or texto.lower() in {'nan', 'none', 'nat'}:
        return None

    digitos = ''.join(ch for ch in texto if ch.isdigit())
    return digitos or None
