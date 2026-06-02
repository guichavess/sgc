from pathlib import Path
from unittest.mock import patch

import pytest
from flask import render_template

from app.cgfr.models import CgfrProcessoEnviado
from app.cgfr.services.processo_service import ProcessoService
from app.config import Config
from app.constants import CGFR_DELIBERACAO_OPTIONS


def _login_admin_cgfr(client, db_session):
    import uuid

    from app.models.usuario import Usuario

    uid = uuid.uuid4().hex[:12]
    usuario = Usuario(
        id_usuario_sei=f'cgfr_{uid}',
        nome='Admin CGFR',
        sigla_login=f'admin_cgfr_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()
    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
    return usuario


def test_modal_edicao_deliberacao_renderiza_dropdown(app):
    with app.test_request_context('/cgfr/'):
        html = render_template(
            'cgfr/partials/modal_edicao.html',
            cgfr_deliberacao_options=CGFR_DELIBERACAO_OPTIONS,
        )

    assert '<select class="form-select form-select-sm" id="modal-deliberacao">' in html
    assert 'id="modal-deliberacao" rows=' not in html
    for opcao in CGFR_DELIBERACAO_OPTIONS:
        assert f'value="{opcao}"' in html


def test_modal_edicao_renderiza_e_envia_data_envio(app):
    with app.test_request_context('/cgfr/'):
        html = render_template(
            'cgfr/partials/modal_edicao.html',
            cgfr_deliberacao_options=CGFR_DELIBERACAO_OPTIONS,
        )

    assert 'for="modal-data-envio"' in html
    assert 'id="modal-data-envio"' in html
    assert 'type="date"' in html
    assert "document.getElementById('modal-data-envio').value" in html
    assert "tramitado_sead_cgfr: $('#modal-data-envio').val() || null" in html


def test_api_salvar_atualiza_data_envio(client, app, db_session):
    protocolo = '00002.000077/2026-77'
    processo = CgfrProcessoEnviado(
        processo_formatado=protocolo,
        tramitado_sead_cgfr='01/05/2026',
    )
    db_session.add(processo)
    db_session.flush()

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        resp = client.post(
            '/cgfr/api/salvar',
            json={
                'processo_formatado': protocolo,
                'tramitado_sead_cgfr': '2026-05-15',
            },
        )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data['sucesso'] is True

    db_session.refresh(processo)
    assert processo.tramitado_sead_cgfr == '15/05/2026'


def test_constantes_trino_usam_credenciais_atualizadas():
    assert Config.TRINO_USER == 'admin'
    assert Config.TRINO_PASSWORD == 'LOC35q3dgZn'


def test_classificacao_rejeita_deliberacao_fora_do_dropdown(db_session):
    processo = CgfrProcessoEnviado(
        processo_formatado='00002.000001/2026-01',
        deliberacao='Aprovado',
    )
    db_session.add(processo)
    db_session.commit()

    with pytest.raises(ValueError, match='Deliberação inválida'):
        ProcessoService.classificar_processo(
            processo.processo_formatado,
            {'deliberacao': 'Texto livre indevido'},
        )

    db_session.refresh(processo)
    assert processo.deliberacao == 'Aprovado'


def test_opcoes_deliberacao_nao_expoem_aliases_antigos():
    assert 'Aprovado, com redução.' not in CGFR_DELIBERACAO_OPTIONS
    assert 'Retirado de pauta' not in CGFR_DELIBERACAO_OPTIONS
    assert 'Aprovado com redução' in CGFR_DELIBERACAO_OPTIONS
    assert 'Retirado' in CGFR_DELIBERACAO_OPTIONS


@pytest.mark.parametrize(
    ('entrada', 'esperado'),
    [
        ('Aprovado, com redução.', 'Aprovado com redução'),
        ('Retirado de pauta', 'Retirado'),
        ('Retirado de Pauta', 'Retirado'),
    ],
)
def test_classificacao_normaliza_aliases_antigos_de_deliberacao(db_session, entrada, esperado):
    processo = CgfrProcessoEnviado(processo_formatado=f'00002.0000{len(entrada)}/2026-01')
    db_session.add(processo)
    db_session.commit()

    ProcessoService.classificar_processo(
        processo.processo_formatado,
        {'deliberacao': entrada},
    )

    db_session.refresh(processo)
    assert processo.deliberacao == esperado


def test_api_data_normaliza_deliberacoes_antigas_para_filtro(client, app, db_session):
    processos = [
        CgfrProcessoEnviado(
            processo_formatado='00002.000201/2026-01',
            deliberacao='Aprovado, com redução.',
        ),
        CgfrProcessoEnviado(
            processo_formatado='00002.000202/2026-02',
            deliberacao='Aprovado com redução',
        ),
        CgfrProcessoEnviado(
            processo_formatado='00002.000203/2026-03',
            deliberacao='Retirado de pauta',
        ),
    ]
    db_session.add_all(processos)
    db_session.flush()

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        resp = client.post('/cgfr/api/data', json={})

    assert resp.status_code == 200
    deliberacoes = {
        record['deliberacao']
        for record in resp.get_json()['records']
        if record.get('deliberacao')
    }
    assert deliberacoes == {'Aprovado com redução', 'Retirado'}


def test_modal_edicao_nao_mantem_campo_aberto_para_deliberacao():
    template_path = Path('app/templates/cgfr/partials/modal_edicao.html')
    html = template_path.read_text(encoding='utf-8')

    assert 'textarea class="form-control form-control-sm" id="modal-deliberacao"' not in html


def test_sync_documentos_retorna_json_em_erro_inesperado(client, app, db_session):
    protocolo = '00002.000099/2026-09'
    processo = CgfrProcessoEnviado(processo_formatado=protocolo)
    db_session.add(processo)
    db_session.flush()

    with app.app_context():
        _login_admin_cgfr(client, db_session)
        with patch('app.cgfr.routes.acompanhar.gerar_token_sei_admin', return_value='TOKEN'), \
             patch('app.cgfr.routes.acompanhar._update_processo_from_sei'), \
             patch(
                 'app.cgfr.routes.acompanhar._fetch_and_save_docs',
                 side_effect=RuntimeError('falha simulada'),
             ):
            resp = client.post(f'/cgfr/acompanhar/{protocolo}/sync')

    assert resp.status_code == 500
    assert resp.is_json
    data = resp.get_json()
    assert data['success'] is False
    assert 'falha simulada' in data['error']
