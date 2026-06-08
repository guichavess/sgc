"""
Backfill de empenho.competencia por encadeamento reverso SIAFE.

As fases posteriores referenciam a Nota de Empenho (NE), permitindo recuperar a
competencia quando ela esta ausente no empenho:

    Liquidacao.codigoEmpenhoVinculado -> Empenho.codigo
    PD.codigoNE                       -> Empenho.codigo
    OB.codigoNE                       -> Empenho.codigo

Politica para varias competencias candidatas:
    1. moda, ou seja, competencia mais frequente;
    2. empate resolvido pela competencia mais antiga.

A busca e em cascata: NL, depois PD, depois OB. O primeiro nivel com match
resolve a NE e os niveis seguintes nao sobrescrevem a escolha.
"""
from collections import Counter, defaultdict

from app.models.liquidacao import Liquidacao
from app.models.ob import OB
from app.models.pd import PD
from app.utils.competencia import normalizar_competencia


def escolher_competencia(candidatos):
    """Escolhe a competencia canonica entre candidatos no formato MM/YYYY."""
    validos = []
    for candidato in candidatos:
        comp = normalizar_competencia(candidato)
        if comp:
            validos.append(comp)

    if not validos:
        return None

    contagem = Counter(validos)
    frequencia_maxima = max(contagem.values())
    modas = [
        comp
        for comp, frequencia in contagem.items()
        if frequencia == frequencia_maxima
    ]
    if len(modas) == 1:
        return modas[0]

    def chave_competencia(comp):
        mes, ano = comp.split('/')
        return (int(ano), int(mes))

    return min(modas, key=chave_competencia)


def _candidatos_por_ne(session, model, coluna_ne, codigos_ne):
    """Retorna {codigo_ne: [competencia, ...]} para uma fase posterior."""
    if not codigos_ne:
        return {}

    rows = (
        session.query(coluna_ne, model.competencia)
        .filter(
            coluna_ne.in_(codigos_ne),
            model.competencia.isnot(None),
            model.competencia != '',
        )
        .all()
    )

    agrupado = defaultdict(list)
    for codigo_ne, competencia in rows:
        agrupado[codigo_ne].append(competencia)
    return agrupado


def mapear_competencias_empenho(session, codigos_ne):
    """Mapeia NEs para {'competencia': 'MM/YYYY', 'origem': 'NL'|'PD'|'OB'}."""
    codigos = [
        str(codigo).strip()
        for codigo in codigos_ne
        if codigo is not None and str(codigo).strip()
    ]
    if not codigos:
        return {}

    resultado = {}
    pendentes = set(codigos)
    niveis = [
        ('NL', Liquidacao, Liquidacao.codigoEmpenhoVinculado),
        ('PD', PD, PD.codigoNE),
        ('OB', OB, OB.codigoNE),
    ]

    for origem, model, coluna_ne in niveis:
        if not pendentes:
            break

        resolvidos_neste_nivel = set()
        agrupado = _candidatos_por_ne(session, model, coluna_ne, list(pendentes))
        for codigo_ne, competencias in agrupado.items():
            escolhida = escolher_competencia(competencias)
            if escolhida:
                resultado[codigo_ne] = {
                    'competencia': escolhida,
                    'origem': origem,
                }
                resolvidos_neste_nivel.add(codigo_ne)

        pendentes -= resolvidos_neste_nivel

    return resultado
