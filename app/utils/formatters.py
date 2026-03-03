"""
Funções de formatação para uso no backend e templates.
"""
from datetime import datetime, timedelta
from typing import Optional, Union


def formatar_valor_monetario(valor: Union[float, int, None]) -> str:
    """
    Formata um valor numérico como moeda brasileira.

    Args:
        valor: Valor numérico

    Returns:
        String formatada (ex: "R$ 1.234,56")
    """
    if valor is None:
        return "R$ 0,00"

    try:
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "R$ 0,00"


def formatar_data(data: Optional[datetime], formato: str = "%d/%m/%Y") -> str:
    """
    Formata uma data para exibição.

    Args:
        data: Objeto datetime
        formato: Formato de saída

    Returns:
        String formatada ou "--" se data for None
    """
    if data is None:
        return "--"

    try:
        return data.strftime(formato)
    except (ValueError, AttributeError):
        return "--"


def formatar_data_hora(data: Optional[datetime]) -> str:
    """
    Formata data e hora para exibição.

    Args:
        data: Objeto datetime

    Returns:
        String formatada (ex: "01/01/2025 14:30")
    """
    return formatar_data(data, "%d/%m/%Y %H:%M")


def formatar_diferenca_tempo(
    data_fim: Optional[datetime],
    data_inicio: Optional[datetime]
) -> str:
    """
    Calcula e formata a diferença entre duas datas.

    Args:
        data_fim: Data final
        data_inicio: Data inicial

    Returns:
        String descritiva (ex: "5 dias", "2 horas", "30 min")
    """
    if not data_fim or not data_inicio:
        return "--"

    try:
        diferenca = data_fim - data_inicio

        dias = diferenca.days
        segundos_restantes = diferenca.seconds
        horas = segundos_restantes // 3600
        minutos = (segundos_restantes % 3600) // 60

        if dias > 0:
            if dias == 1:
                return "1 dia"
            return f"{dias} dias"
        elif horas > 0:
            if horas == 1:
                return "1 hora"
            return f"{horas} horas"
        elif minutos > 0:
            return f"{minutos} min"
        else:
            return "recentemente"

    except (ValueError, TypeError):
        return "--"


def formatar_competencia(mes: int, ano: int) -> str:
    """
    Formata competência (mês/ano) para exibição.

    Args:
        mes: Número do mês (1-12)
        ano: Ano

    Returns:
        String formatada (ex: "Janeiro/2025")
    """
    meses = [
        "Janeiro", "Fevereiro", "Março", "Abril",
        "Maio", "Junho", "Julho", "Agosto",
        "Setembro", "Outubro", "Novembro", "Dezembro"
    ]

    try:
        if 1 <= mes <= 12:
            return f"{meses[mes - 1]}/{ano}"
        return f"{mes:02d}/{ano}"
    except (ValueError, IndexError):
        return f"{mes}/{ano}"


def truncar_texto(texto: Optional[str], tamanho_max: int = 50) -> str:
    """
    Trunca texto adicionando reticências se necessário.

    Args:
        texto: Texto a truncar
        tamanho_max: Tamanho máximo

    Returns:
        Texto truncado ou original se menor que o máximo
    """
    if not texto:
        return ""

    if len(texto) <= tamanho_max:
        return texto

    return texto[:tamanho_max - 3] + "..."
