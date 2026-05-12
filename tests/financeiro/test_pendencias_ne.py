from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import patch
import uuid


def _uid():
    return uuid.uuid4().hex[:10]


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    uid = _uid()
    usuario = Usuario(
        id_usuario_sei=f'fin_{uid}',
        nome='ADMIN FINANCEIRO TESTE',
        sigla_login=f'fin_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


def _make_contrato(db_session, codigo=None):
    from app.models import Contrato

    uid = _uid()
    contrato = Contrato(
        codigo=codigo or f'C{uid}',
        numeroOriginal=f'CT-{uid}',
        nomeContratado=f'CONTRATADO {uid}',
        nomeContratadoResumido=f'CONTR {uid}',
    )
    db_session.add(contrato)
    db_session.flush()
    return contrato


def _make_solicitacao(db_session, usuario, contrato, index=1, **kwargs):
    from app.models import Solicitacao

    defaults = dict(
        codigo_contrato=contrato.codigo,
        id_usuario_solicitante=usuario.id,
        data_solicitacao=datetime(2026, 5, 1) + timedelta(minutes=index),
        protocolo_gerado_sei=f'0000{index}.{_uid()}/2026-11',
        link_processo_sei=f'https://sei.example/processo/{index}',
        competencia='Maio/2026',
        status_empenho_id=1,
    )
    defaults.update(kwargs)
    solicitacao = Solicitacao(**defaults)
    db_session.add(solicitacao)
    db_session.flush()
    return solicitacao


def _make_empenho(db_session, usuario, solicitacao, **kwargs):
    from app.models import SolicitacaoEmpenho

    defaults = dict(
        id_solicitacao=solicitacao.id,
        valor=Decimal('1234.56'),
        competencia=solicitacao.competencia,
        id_user=usuario.id,
        ne=None,
        saldo_momento=Decimal('5000.00'),
        data=datetime(2026, 5, 1, 10, 0),
    )
    defaults.update(kwargs)
    empenho = SolicitacaoEmpenho(**defaults)
    db_session.add(empenho)
    db_session.flush()
    return empenho


class TestPendenciasNe:
    def test_lista_usa_valor_da_solicitacao_e_link_do_processo(self, client, db_session):
        usuario = _login_admin(client, db_session)
        contrato = _make_contrato(db_session)
        solicitacao = _make_solicitacao(db_session, usuario, contrato)
        _make_empenho(db_session, usuario, solicitacao, valor=Decimal('9876.54'))

        resp = client.get('/financeiro/pendencias_ne')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'N' in html and 'Processo' in html
        assert 'ID</th>' not in html
        assert solicitacao.protocolo_gerado_sei in html
        assert solicitacao.link_processo_sei in html
        assert '9.876,54' in html

    def test_lista_exibe_paginacao_no_mesmo_padrao_da_dashboard(self, client, db_session):
        usuario = _login_admin(client, db_session)
        contrato = _make_contrato(db_session)
        for index in range(1, 22):
            solicitacao = _make_solicitacao(db_session, usuario, contrato, index=index)
            _make_empenho(db_session, usuario, solicitacao)

        resp = client.get('/financeiro/pendencias_ne')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Mostrando <strong>20</strong> de <strong>21</strong>' in html
        assert 'pagination pagination-sm mb-0' in html
        assert 'page=2' in html

    def test_inserir_ne_atualiza_o_empenho_pendente_especifico(self, client, db_session):
        usuario = _login_admin(client, db_session)
        contrato = _make_contrato(db_session)
        solicitacao = _make_solicitacao(db_session, usuario, contrato)
        empenho_atendido = _make_empenho(
            db_session,
            usuario,
            solicitacao,
            ne='2026NE000001',
            data=datetime(2026, 5, 1, 9, 0),
        )
        empenho_pendente = _make_empenho(
            db_session,
            usuario,
            solicitacao,
            ne=None,
            data=datetime(2026, 5, 1, 10, 0),
        )

        with patch('app.financeiro.routes.pendencias.validar_ne_siafe', return_value={'sucesso': True}):
            resp = client.post(
                f'/financeiro/inserir_ne/{empenho_pendente.id}',
                data={'ne': '2026NE000099'},
                follow_redirects=False,
            )

        assert resp.status_code == 302
        db_session.refresh(empenho_atendido)
        db_session.refresh(empenho_pendente)
        db_session.refresh(solicitacao)
        assert empenho_atendido.ne == '2026NE000001'
        assert empenho_pendente.ne == '2026NE000099'
        assert solicitacao.status_empenho_id == 2
