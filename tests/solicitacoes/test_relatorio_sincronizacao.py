"""
Testes do relatório da Sincronização Unificada (Pagamentos).

Cobre:
- classificar_alerta: traduz mensagens técnicas (504, 422, timeout...) em
  explicação + orientação, extraindo o protocolo
- montar_relatorio_sincronizacao: resumo executivo, status geral, agrupamento
- rota POST /solicitacoes/api/relatorio-sincronizacao (200, 401, 400)
- dashboard: modal ampliado com timeline das fases e botão de relatório
"""
import json
import uuid

import pytest


PROTOCOLOS_504 = [
    '00002.010665/2024-97', '00002.000928/2025-31', '00002.001028/2025-19',
    '00002.001210/2025-61', '00002.003718/2025-02', '00002.003749/2025-55',
    '00002.003104/2025-12', '00002.001204/2025-12',
]
MOVIMENTADOS = ['00002.008489/2026-95', '00002.008491/2026-64', '00002.008497/2026-31']


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    uid = uuid.uuid4().hex[:10]
    usuario = Usuario(
        id_usuario_sei=f'rel_admin_{uid}',
        nome='ADMIN RELATORIO',
        sigla_login=f'rel_admin_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


def _payload_caso_real():
    """Reproduz a execução relatada: 107 processos, 8× 504, 3 movimentados."""
    return {
        'iniciado_em': '2026-09-15T10:00:00',
        'finalizado_em': '2026-09-15T10:12:30',
        'fases': [
            {
                'numero': 1, 'status': 'alerta',
                'inicio': '2026-09-15T10:00:00', 'fim': '2026-09-15T10:09:00',
                'msg_final': 'Download concluído! 99/107 protocolos atualizados.',
            },
            {
                'numero': 2, 'status': 'sucesso',
                'inicio': '2026-09-15T10:09:01', 'fim': '2026-09-15T10:11:00',
                'msg_final': 'Concluído! 3 processos avançaram de etapa.',
                'movimentados': MOVIMENTADOS,
            },
            {
                'numero': 3, 'status': 'sucesso',
                'inicio': '2026-09-15T10:11:01', 'fim': '2026-09-15T10:12:30',
                'msg_final': 'Concluído! 240 saldos atualizados.',
            },
        ],
        'alertas': [
            {'fase': 'Download SEI', 'msg': f'[ALERTA] Erro API {p}: 504'}
            for p in PROTOCOLOS_504
        ],
        'protocolos_422': [],
    }


# ──────────────────────────────────────────────────────────────────────────────
# classificar_alerta
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('msg, tipo', [
    ('[ALERTA] Erro API 00002.010665/2024-97: 504', 'tempo_limite_sei'),
    ('[ALERTA] Erro API 00002.010665/2024-97: 503', 'sei_indisponivel'),
    ('[ALERTA] Erro API 00002.010665/2024-97: 502', 'sei_indisponivel'),
    ('[422] Processo inexistente: 00002.010665/2024-97', 'processo_inexistente'),
    ('[ALERTA] Erro 00002.010665/2024-97: ReadTimeout', 'falha_rede'),
    ('[ALERTA] Erro 00002.010665/2024-97: ConnectionError', 'falha_rede'),
    ('[ALERTA] Erro API 00002.010665/2024-97: 401', 'acesso_negado'),
    ('[ALERTA] Saldo C123 01/2026: SIAFE indisponível', 'saldo_siafe'),
    ('Algo inesperado aconteceu', 'nao_classificado'),
])
def test_classificar_alerta_tipos(msg, tipo):
    from app.services.sincronizacao_pagamentos_service import classificar_alerta

    resultado = classificar_alerta(msg)

    assert resultado['tipo'] == tipo
    assert resultado['titulo']
    assert resultado['explicacao']
    assert resultado['orientacao']
    assert resultado['mensagem_tecnica'] == msg


def test_classificar_alerta_extrai_protocolo():
    from app.services.sincronizacao_pagamentos_service import classificar_alerta

    resultado = classificar_alerta('[ALERTA] Erro API 00002.003104/2025-12: 504')
    assert resultado['protocolo'] == '00002.003104/2025-12'

    assert classificar_alerta('[ALERTA] Saldo C1 01/2026: erro')['protocolo'] is None


def test_classificar_alerta_504_explica_processo_grande():
    from app.services.sincronizacao_pagamentos_service import classificar_alerta

    texto = classificar_alerta('[ALERTA] Erro API 00002.003104/2025-12: 504')['explicacao'].lower()
    assert 'muitos documentos' in texto


def test_classificar_alerta_tolera_none():
    from app.services.sincronizacao_pagamentos_service import classificar_alerta

    assert classificar_alerta(None)['tipo'] == 'nao_classificado'


# ──────────────────────────────────────────────────────────────────────────────
# montar_relatorio_sincronizacao
# ──────────────────────────────────────────────────────────────────────────────

def test_relatorio_caso_real_resumo_e_status(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    with app.app_context():
        rel = montar_relatorio_sincronizacao(_payload_caso_real())

    assert rel['status'] == 'alerta'
    assert rel['status_rotulo'] == 'Concluída com alertas'

    numeros = rel['numeros']
    assert numeros['processos_verificados'] == 107
    assert numeros['processos_atualizados'] == 99
    assert numeros['processos_nao_consultados'] == 8
    assert numeros['etapas_avancadas'] == 3
    assert numeros['saldos_atualizados'] == 240
    assert numeros['total_pendencias'] == 8

    assert '107' in rel['resumo'] and '99' in rel['resumo'] and '8' in rel['resumo']
    assert rel['duracao_total'] == '12 min 30 s'


def test_relatorio_agrupa_pendencias_por_tipo(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    with app.app_context():
        rel = montar_relatorio_sincronizacao(_payload_caso_real())

    assert len(rel['grupos']) == 1
    grupo = rel['grupos'][0]
    assert grupo['tipo'] == 'tempo_limite_sei'
    assert sorted(i['protocolo'] for i in grupo['itens']) == sorted(PROTOCOLOS_504)


def test_relatorio_fases_com_duracao_e_nome(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    with app.app_context():
        rel = montar_relatorio_sincronizacao(_payload_caso_real())

    fases = rel['fases']
    assert [f['numero'] for f in fases] == [1, 2, 3]
    assert fases[0]['nome'] == 'Download de documentos SEI'
    assert fases[0]['duracao'] == '9 min 0 s'
    assert fases[0]['status'] == 'alerta'
    assert fases[1]['movimentados'] == MOVIMENTADOS
    assert all(f['descricao'] for f in fases)


def test_relatorio_sem_alertas_e_sucesso(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    payload = _payload_caso_real()
    payload['alertas'] = []
    payload['fases'][0]['status'] = 'sucesso'
    payload['fases'][0]['msg_final'] = 'Download concluído! 107/107 protocolos atualizados.'

    with app.app_context():
        rel = montar_relatorio_sincronizacao(payload)

    assert rel['status'] == 'sucesso'
    assert rel['status_rotulo'] == 'Concluída com sucesso'
    assert rel['grupos'] == []
    assert rel['numeros']['total_pendencias'] == 0


def test_relatorio_fase_com_erro_e_interrompida(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    payload = _payload_caso_real()
    payload['fases'][2]['status'] = 'erro'

    with app.app_context():
        rel = montar_relatorio_sincronizacao(payload)

    assert rel['status'] == 'erro'
    assert rel['status_rotulo'] == 'Interrompida'


def test_relatorio_fase2_interrompida_conta_movimentados(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    payload = _payload_caso_real()
    payload['fases'][1].update(status='erro', msg_final='Conexão perdida na Fase 2.')
    payload['fases'][1]['movimentados'] = MOVIMENTADOS[:2]

    with app.app_context():
        rel = montar_relatorio_sincronizacao(payload)

    assert rel['numeros']['etapas_avancadas'] == 2


def test_relatorio_inclui_422_como_pendencia(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    payload = _payload_caso_real()
    payload['alertas'] = []
    payload['protocolos_422'] = [{'protocolo': '00002.000001/2025-01', 'link_sei': 'http://sei/x'}]

    with app.app_context():
        rel = montar_relatorio_sincronizacao(payload)

    grupo = rel['grupos'][0]
    assert grupo['tipo'] == 'processo_inexistente'
    assert grupo['itens'][0]['link_sei'] == 'http://sei/x'


def test_relatorio_payload_vazio_nao_quebra(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    with app.app_context():
        rel = montar_relatorio_sincronizacao({})

    assert rel['status'] in ('sucesso', 'erro')
    assert [f['numero'] for f in rel['fases']] == [1, 2, 3]
    assert rel['grupos'] == []


def test_relatorio_ignora_itens_malformados(app):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao

    payload = {
        'fases': 'lixo',
        'alertas': ['texto solto', None, {'msg': '[ALERTA] Erro API 00002.010665/2024-97: 504'}],
        'protocolos_422': [None, 'x'],
    }
    with app.app_context():
        rel = montar_relatorio_sincronizacao(payload)

    assert rel['numeros']['total_pendencias'] == 1


def test_relatorio_enriquece_com_contrato_e_link(app, db_session, usuario_admin):
    from app.services.sincronizacao_pagamentos_service import montar_relatorio_sincronizacao
    from app.models import Solicitacao

    sol = Solicitacao(
        id_usuario_solicitante=usuario_admin.id,
        codigo_contrato='CT-999',
        protocolo_gerado_sei=PROTOCOLOS_504[0],
        link_processo_sei='http://sei/processo',
    )
    db_session.add(sol)
    db_session.commit()

    rel = montar_relatorio_sincronizacao(_payload_caso_real())
    item = next(i for i in rel['grupos'][0]['itens'] if i['protocolo'] == PROTOCOLOS_504[0])

    assert item['contrato'] == 'CT-999'
    assert item['link_sei'] == 'http://sei/processo'


# ──────────────────────────────────────────────────────────────────────────────
# Rota /api/relatorio-sincronizacao
# ──────────────────────────────────────────────────────────────────────────────

URL_RELATORIO = '/solicitacoes/api/relatorio-sincronizacao'


def test_rota_relatorio_logado_renderiza_html(client, db_session):
    _login_admin(client, db_session)

    resp = client.post(URL_RELATORIO, data={'payload': json.dumps(_payload_caso_real())})

    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Relatório da Sincronização' in html
    assert 'Concluída com alertas' in html
    assert 'ADMIN RELATORIO' in html
    for protocolo in PROTOCOLOS_504:
        assert protocolo in html
    assert 'muitos documentos' in html
    assert MOVIMENTADOS[0] in html


def test_rota_relatorio_aceita_json(client, db_session):
    _login_admin(client, db_session)

    resp = client.post(URL_RELATORIO, json=_payload_caso_real())

    assert resp.status_code == 200


def test_rota_relatorio_anonimo(client, db_session):
    resp = client.post(URL_RELATORIO, data={'payload': json.dumps(_payload_caso_real())})
    assert resp.status_code in (401, 302)


@pytest.mark.parametrize('data', [
    {},
    {'payload': 'isto não é json'},
    {'payload': '[1, 2, 3]'},
])
def test_rota_relatorio_payload_invalido(client, db_session, data):
    _login_admin(client, db_session)

    resp = client.post(URL_RELATORIO, data=data)

    assert resp.status_code == 400


# ──────────────────────────────────────────────────────────────────────────────
# Dashboard — modal ampliado com timeline
# ──────────────────────────────────────────────────────────────────────────────

def test_dashboard_modal_sync_ampliado_com_timeline(client, db_session):
    _login_admin(client, db_session)

    resp = client.get('/solicitacoes/dashboard')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert 'id="modalSyncSei"' in html
    assert 'modal-sync-sei' in html
    assert 'modal-xl' in html
    for fase in (1, 2, 3):
        assert f'data-fase="{fase}"' in html
    assert 'id="btnRelatorioSync"' in html
    assert '/solicitacoes/api/relatorio-sincronizacao' in html
