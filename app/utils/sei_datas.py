"""
Datas de documentos SEI.

O SEI devolve a data dos documentos como string ("dd/mm/yyyy", às vezes com
hora, ou ISO). A data de início de um processo é a do 1º documento — a menor
data entre todos os documentos, independente da ordem retornada pela API.
"""
from datetime import datetime

_FORMATOS = ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d')


def parse_data_documento_sei(valor):
    """Converte a data de um documento SEI em datetime (None se inválida)."""
    if not valor:
        return None
    texto = str(valor).strip().strip('\'"').strip()
    for fmt in _FORMATOS:
        try:
            return datetime.strptime(texto, fmt)
        except ValueError:
            continue
    return None


def data_primeiro_documento_sei(documentos):
    """Menor data entre os documentos da API SEI (campo Data ou DataGeracao)."""
    datas = [
        parse_data_documento_sei(doc.get('Data') or doc.get('DataGeracao'))
        for doc in (documentos or [])
    ]
    return min((d for d in datas if d), default=None)
