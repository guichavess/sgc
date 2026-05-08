"""
Helpers para corrigir textos com mojibake comum no Windows.
"""


_CP850_MARKERS = ("├", "┬", "┤", "┐", "┴", "└", "┘", "┼")


def corrigir_mojibake_cp850(value):
    """Corrige UTF-8 que foi interpretado como CP850, preservando textos normais."""
    if value is None or not isinstance(value, str):
        return value
    if not any(marker in value for marker in _CP850_MARKERS):
        return value
    try:
        corrigido = value.encode("cp850").decode("utf-8")
    except UnicodeError:
        return value
    return corrigido
