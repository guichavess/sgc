"""Testes da regra "CGFR aceita 3639 OU 3771 para conclusão da etapa"."""
from app.cgfr.models import CgfrMovimentacao, CgfrProcessoEnviado
from app.cgfr.services.processo_service import (
    _enrich_etapa_atual,
    obter_timeline_acompanhamento,
)
from app.constants import SerieDocumentoCGFR


def _mov(protocolo, id_serie, numero='0001', data='10/05/2026', link='https://sei/x', nome=None):
    return CgfrMovimentacao(
        id_documento=f'{protocolo}-{id_serie}-{numero}',
        protocolo_procedimento=protocolo,
        id_procedimento='PROC1',
        procedimento_formatado=protocolo,
        documento_formatado=f'DOC-{numero}',
        link_acesso=link,
        descricao='',
        data=data,
        numero=numero,
        id_serie=int(id_serie),
        serie_nome=nome or f'SERIE-{id_serie}',
        serie_aplicabilidade='I',
        unidade_id='U1',
        unidade_sigla='SEAD',
        unidade_descricao='SEAD',
        obs='',
    )


def test_constante_serie_3771_disponivel():
    """Existe uma constante nova para o idSerie 3771 usado como doc extra CGFR."""
    assert SerieDocumentoCGFR.CGFR_DESPACHO_EXTRA == '3771'


def test_timeline_etapa_cgfr_concluida_via_doc_3771(db_session):
    """Basta o doc 3771 para a etapa CGFR ser marcada como concluída."""
    protocolo = '00002.000771/2026-77'
    processo = CgfrProcessoEnviado(
        processo_formatado=protocolo,
        tramitado_sead_cgfr='01/05/2026',
        # data_recebido_cgfr propositalmente vazia
    )
    db_session.add(processo)
    db_session.add(_mov(protocolo, '3771', nome='SEFAZ: CGFR - Extra'))
    db_session.commit()

    dados = obter_timeline_acompanhamento(protocolo)
    assert dados is not None

    etapa_cgfr = dados['timeline_data'][1]
    assert etapa_cgfr['nome'] == 'CGFR'
    assert etapa_cgfr['concluida'] is True, (
        'Etapa CGFR deveria estar concluída porque o doc 3771 já foi juntado'
    )
    assert etapa_cgfr['data'], 'Etapa CGFR deveria ter uma data (do doc 3771)'


def test_timeline_expoe_mov_cgfr_extra(db_session):
    """A rota de acompanhamento recebe mov_cgfr_extra quando o doc 3771 existe."""
    protocolo = '00002.000772/2026-77'
    processo = CgfrProcessoEnviado(
        processo_formatado=protocolo,
        tramitado_sead_cgfr='01/05/2026',
        data_recebido_cgfr='03/05/2026',
    )
    db_session.add(processo)
    db_session.add(
        _mov(
            protocolo,
            '3771',
            numero='EXTRA-001',
            link='https://sei/extra',
            nome='SEFAZ: CGFR - Documento Extra',
        )
    )
    db_session.commit()

    dados = obter_timeline_acompanhamento(protocolo)
    assert dados is not None
    assert 'mov_cgfr_extra' in dados
    extra = dados['mov_cgfr_extra']
    assert extra is not None
    assert extra['link_acesso'] == 'https://sei/extra'
    assert extra['nome'] == 'SEFAZ: CGFR - Documento Extra'


def test_timeline_sem_doc_extra_nao_quebra(db_session):
    """Processo sem doc 3771 continua funcionando: mov_cgfr_extra é None."""
    protocolo = '00002.000773/2026-77'
    processo = CgfrProcessoEnviado(
        processo_formatado=protocolo,
        tramitado_sead_cgfr='01/05/2026',
        data_recebido_cgfr='03/05/2026',
    )
    db_session.add(processo)
    db_session.commit()

    dados = obter_timeline_acompanhamento(protocolo)
    assert dados is not None
    assert dados.get('mov_cgfr_extra') is None


def test_enrich_etapa_atual_considera_3771_como_cgfr(db_session):
    """Na listagem, doc 3771 avança o processo para etapa 'CGFR' (mesmo nível do 3639)."""
    protocolo = '00002.000774/2026-77'
    processo = CgfrProcessoEnviado(
        processo_formatado=protocolo,
        tramitado_sead_cgfr='01/05/2026',
    )
    db_session.add(processo)
    db_session.add(_mov(protocolo, '3771'))
    db_session.commit()

    records = [{
        'processo_formatado': protocolo,
        'tramitado_sead_cgfr': '01/05/2026',
    }]
    _enrich_etapa_atual(records)

    assert records[0]['etapa_atual'] == 'CGFR'
    assert records[0]['etapa_atual_num'] == 2
