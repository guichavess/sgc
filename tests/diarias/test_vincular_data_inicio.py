"""
Data de início do processo ao importar/sincronizar processos SEI em Diárias.

Mesmo bug do módulo de Pagamentos: itinerários criados a partir de um processo
SEI existente recebiam a data da importação em `data_solicitacao`, em vez da
data do 1º documento do processo.
"""
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

PROTOCOLO = '00002.004241/2026-55'


def _proc():
    return {
        'sucesso': True,
        'protocolo_formatado': PROTOCOLO,
        'id_procedimento': 'PROC123',
        'link_acesso': 'https://sei/pi/proc',
        'especificacao': 'DIÁRIAS TESTE',
        'erro': None,
    }


def _docs():
    return {
        'sucesso': True,
        'documentos': [
            {
                'IdDocumento': 'DOC_REQ',
                'DocumentoFormatado': '0020000002',
                'Data': '12/03/2026',
                'Serie': {'IdSerie': '2986', 'Nome': 'SEAD_MEMORANDO_SGA'},
            },
            {
                'IdDocumento': 'DOC_MEMO',
                'DocumentoFormatado': '0020000001',
                'Data': '03/03/2026',
                'Serie': {'IdSerie': '9999', 'Nome': 'Despacho'},
            },
        ],
        'erro': None,
    }


def test_importar_processo_usa_data_do_primeiro_documento(app, db_session):
    from app.models.diaria import DiariasItinerario
    from app.services.vincular_processo_diaria import importar_processo_sei_como_novo

    with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin', return_value='TOKEN'), \
         patch('app.services.vincular_processo_diaria.consultar_procedimento_sei', return_value=_proc()), \
         patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei', return_value=_docs()), \
         patch('app.services.vincular_processo_diaria.baixar_documento_sei', return_value=None):
        resultado = importar_processo_sei_como_novo(PROTOCOLO, etapa_id=1, usuario_id=1)

    assert resultado['sucesso'] is True
    it = db_session.get(DiariasItinerario, resultado['itinerario_id'])
    assert it.data_solicitacao == date(2026, 3, 3)


def test_importar_processo_sem_documentos_datados_mantem_hoje(app, db_session):
    from app.models.diaria import DiariasItinerario
    from app.services.vincular_processo_diaria import importar_processo_sei_como_novo

    with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin', return_value='TOKEN'), \
         patch('app.services.vincular_processo_diaria.consultar_procedimento_sei', return_value=_proc()), \
         patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei',
               return_value={'sucesso': True, 'documentos': [], 'erro': None}):
        resultado = importar_processo_sei_como_novo(PROTOCOLO, etapa_id=1, usuario_id=1)

    it = db_session.get(DiariasItinerario, resultado['itinerario_id'])
    data = it.data_solicitacao.date() if isinstance(it.data_solicitacao, datetime) else it.data_solicitacao
    assert data == date.today()


def test_vincular_a_solicitacao_existente_nao_altera_data_solicitacao(app, db_session):
    """Solicitação criada no sistema já tem data real — vincular não a sobrescreve."""
    from app.models.diaria import DiariasItinerario
    from app.services.vincular_processo_diaria import vincular_processo_sei

    it = DiariasItinerario(
        usuario_gerador='usuario.teste', tipo_solicitacao_id=1, tipo_itinerario=1,
        status_id=1, data_solicitacao=date(2026, 1, 20),
        data_viagem=datetime(2026, 5, 1), data_retorno=datetime(2026, 5, 3),
        qtd_diarias_solicitadas=Decimal('2.5'),
    )
    db_session.add(it)
    db_session.flush()

    with patch('app.services.vincular_processo_diaria.gerar_token_sei_admin', return_value='TOKEN'), \
         patch('app.services.vincular_processo_diaria.consultar_procedimento_sei', return_value=_proc()), \
         patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei', return_value=_docs()):
        resultado = vincular_processo_sei(it.id, PROTOCOLO, 1, 1)

    assert resultado['sucesso'] is True
    assert resultado['data_primeiro_documento'] == datetime(2026, 3, 3)
    db_session.refresh(it)
    assert it.data_solicitacao == date(2026, 1, 20)


def test_sincronizar_bloco_usa_data_do_primeiro_documento(app, db_session):
    from app.models.diaria import DiariasItinerario
    from app.services.vincular_processo_diaria import sincronizar_processos_bloco_diarias

    with patch('app.services.vincular_processo_diaria.consultar_bloco_diarias',
               return_value={'sucesso': True, 'protocolos': [{'ProtocoloFormatado': PROTOCOLO}], 'erro': None}), \
         patch('app.services.vincular_processo_diaria.consultar_procedimento_sei', return_value=_proc()), \
         patch('app.services.vincular_processo_diaria.listar_documentos_procedimento_sei', return_value=_docs()):
        resultado = sincronizar_processos_bloco_diarias(
            token='TOKEN', usuario_id=1, usuario_gerador='admin_sync',
        )

    assert resultado['criados'] == 1
    it = db_session.query(DiariasItinerario).filter_by(sei_protocolo=PROTOCOLO).one()
    assert it.data_solicitacao == date(2026, 3, 3)
