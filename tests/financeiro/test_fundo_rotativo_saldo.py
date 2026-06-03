"""
Testes da aba Saldo do Fundo Rotativo via API SIAFE.
"""
from datetime import datetime
from decimal import Decimal
import re
import uuid

import pytest


def _uid():
    return uuid.uuid4().hex[:10]


def _login_admin(client, db_session):
    from app.models.usuario import Usuario

    uid = _uid()
    usuario = Usuario(
        id_usuario_sei=f'fr_admin_{uid}',
        nome='ADMIN FUNDO ROTATIVO',
        sigla_login=f'fr_admin_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


def _login_sem_permissao(client, db_session):
    from app.models.usuario import Usuario

    uid = _uid()
    usuario = Usuario(
        id_usuario_sei=f'fr_sem_perm_{uid}',
        nome='USUARIO SEM PERMISSAO',
        sigla_login=f'fr_sem_perm_{uid}',
        is_admin=False,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


def _sample_api_row(
    *,
    saldo='2270492.88',
    saldo_anterior='2270492.88',
    credito='0.00',
    debito='0.00',
    ano='2026',
    mes='12',
    fonte='7.55',
    exercicio='1',
):
    return {
        'codigoUG': '210102',
        'saldo': saldo,
        'contaCorrente': f'001.      3791.        95192.{exercicio}.{fonte}.0000.0.000000',
        'saldoAnterior': saldo_anterior,
        'valorCredito': credito,
        'valorDebito': debito,
        'mes': mes,
        'ano': ano,
        'classificacao': [
            {
                'codigoTipoClassificador': 23,
                'nomeTipoClassificador': 'Identificador Exercício Fonte',
                'nomeClassificador': exercicio,
                'valoresClassificador': [exercicio],
            },
            {
                'codigoTipoClassificador': 24,
                'nomeTipoClassificador': 'Marcador de Fonte',
                'nomeClassificador': f'{fonte}.0000',
                'valoresClassificador': fonte.split('.') + ['0000'],
            },
            {
                'codigoTipoClassificador': 28,
                'nomeTipoClassificador': 'Fonte',
                'nomeClassificador': fonte,
                'valoresClassificador': fonte.split('.'),
            },
            {
                'codigoTipoClassificador': 101,
                'nomeTipoClassificador': 'Domicílio bancário UG',
                'nomeClassificador': '001.3791.95192',
                'valoresClassificador': ['001', '3791', '95192'],
            },
            {
                'codigoTipoClassificador': 159,
                'nomeTipoClassificador': 'Detalhamento de Fonte',
                'nomeClassificador': f'{fonte}.0000.000000',
                'valoresClassificador': fonte.split('.') + ['0000', '000000'],
            },
            {
                'codigoTipoClassificador': 186,
                'nomeTipoClassificador': 'Tipo de Detalhamento de Fonte',
                'nomeClassificador': '0',
                'valoresClassificador': ['0'],
            },
        ],
        'classificacaoStr': f'001.      3791.        95192.{exercicio}.{fonte}.0000.0.000000',
    }


class FakeSiafeClient:
    def __init__(self, responses=None, fail_periods=None):
        self.responses = responses or {}
        self.fail_periods = set(fail_periods or [])
        self.calls = []

    def consultar_saldo_contabil(self, ano, mes, conta_contabil, codigo_ug):
        key = (int(ano), int(mes))
        self.calls.append((int(ano), int(mes), conta_contabil, codigo_ug))
        if key in self.fail_periods:
            raise RuntimeError(f'falha periodo {ano}/{mes}')
        return self.responses.get(
            key,
            [_sample_api_row(ano=str(ano), mes=str(mes).zfill(2))],
        )


def test_normaliza_linha_api_para_colunas_planilha(app):
    from app.services.fundo_rotativo_service import normalizar_linha_saldo_siafe

    row = _sample_api_row(fonte='7.55', exercicio='2')

    with app.app_context():
        parsed = normalizar_linha_saldo_siafe(row, 2026, 12, '111111901')

    assert parsed['valor'] == Decimal('2270492.88')
    assert parsed['ano'] == 2026
    assert parsed['mes'] == 12
    assert parsed['fonte_codigo'] == '755'
    assert parsed['fonte_formatada'] == '7.55'
    assert parsed['id_exercicio'] == '02'
    assert parsed['identificador_exercicio_fonte'] == '2'
    assert parsed['marcador_fonte'] == '7.55.0000'
    assert parsed['detalhamento_fonte'] == '7.55.0000.000000'
    assert parsed['tipo_detalhamento_fonte'] == '0'
    assert parsed['domicilio_bancario'] == '001.3791.95192'
    assert parsed['banco'] == '001'
    assert parsed['agencia'] == '3791'
    assert parsed['conta_bancaria'] == '95192'
    assert isinstance(parsed['classificacao_json'], str)


def test_normaliza_usa_periodo_consultado_quando_payload_retorna_mes_contabil(app):
    from app.services.fundo_rotativo_service import normalizar_linha_saldo_siafe

    row = _sample_api_row(ano='2026', mes='12')

    with app.app_context():
        parsed = normalizar_linha_saldo_siafe(row, 2026, 6, '111111901')

    assert parsed['ano'] == 2026
    assert parsed['mes'] == 6
    assert parsed['data'] == datetime(2026, 6, 1)


def test_sincronizacao_inicial_busca_17_periodos_e_grava_todas_fontes(app, db_session):
    from app.models.fundo_rotativo import FundoRotativoSaldo
    from app.services.fundo_rotativo_service import sincronizar_saldos_inicial

    client = FakeSiafeClient(responses={
        (2026, 5): [
            _sample_api_row(saldo='100.00', ano='2026', mes='05', fonte='7.55'),
            _sample_api_row(saldo='50.00', ano='2026', mes='05', fonte='5.00'),
        ],
    })

    with app.app_context():
        resultado = sincronizar_saldos_inicial(usuario_id=123, siafe_client=client)

    assert len(client.calls) == 17
    assert client.calls[0] == (2025, 1, '111111901', '210102')
    assert client.calls[-1] == (2026, 5, '111111901', '210102')
    assert resultado['periodos'] == 17
    assert resultado['registros'] == 18

    maio = (
        FundoRotativoSaldo.query
        .filter_by(ano=2026, mes=5)
        .order_by(FundoRotativoSaldo.fonte_codigo.asc())
        .all()
    )
    assert [s.fonte_codigo for s in maio] == ['500', '755']
    assert [s.sincronizado_por for s in maio] == [123, 123]


def test_sincronizacao_futura_busca_mes_atual_do_ano(app, db_session, monkeypatch):
    import app.services.fundo_rotativo_service as service

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 6, 2)

    client = FakeSiafeClient()
    monkeypatch.setattr(service, 'datetime', FakeDatetime)

    with app.app_context():
        resultado = service.sincronizar_saldos_mes_atual(usuario_id=None, siafe_client=client)

    assert client.calls == [(2026, 6, '111111901', '210102')]
    assert resultado['periodos'] == 1
    assert resultado['registros'] == 1


def test_sincronizacao_idempotente_substitui_snapshot_do_periodo(app, db_session):
    from app.models.fundo_rotativo import FundoRotativoSaldo
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos

    client_1 = FakeSiafeClient(responses={
        (2026, 12): [_sample_api_row(saldo='100.00', ano='2026', mes='12', fonte='7.55')]
    })
    client_2 = FakeSiafeClient(responses={
        (2026, 12): [_sample_api_row(saldo='200.00', ano='2026', mes='12', fonte='7.55')]
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 12)], usuario_id=None, siafe_client=client_1)
        sincronizar_saldos_periodos([(2026, 12)], usuario_id=None, siafe_client=client_2)

    registros = FundoRotativoSaldo.query.filter_by(ano=2026, mes=12).all()
    assert len(registros) == 1
    assert registros[0].valor == Decimal('200.00')


def test_sincronizacao_com_falha_preserva_dados_existentes(app, db_session):
    from app.models.fundo_rotativo import FundoRotativoSaldo
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos

    ok_client = FakeSiafeClient(responses={
        (2026, 12): [_sample_api_row(saldo='100.00', ano='2026', mes='12')]
    })
    fail_client = FakeSiafeClient(fail_periods={(2026, 12)})

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 12)], usuario_id=None, siafe_client=ok_client)
        with pytest.raises(RuntimeError):
            sincronizar_saldos_periodos([(2026, 12)], usuario_id=None, siafe_client=fail_client)

    registros = FundoRotativoSaldo.query.filter_by(ano=2026, mes=12).all()
    assert len(registros) == 1
    assert registros[0].valor == Decimal('100.00')


def test_listar_saldos_filtra_e_soma_por_ano_fonte(app, db_session):
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos, listar_saldos

    client = FakeSiafeClient(responses={
        (2026, 12): [
            _sample_api_row(saldo='100.00', ano='2026', mes='12', fonte='7.55'),
            _sample_api_row(saldo='50.00', ano='2026', mes='12', fonte='5.00'),
        ],
        (2025, 12): [_sample_api_row(saldo='25.00', ano='2025', mes='12', fonte='7.55')],
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 12), (2025, 12)], usuario_id=None, siafe_client=client)
        pagination, soma_total, soma_filtrada = listar_saldos(ano='2026', fonte_codigo='755')

    assert pagination.total == 1
    assert pagination.items[0].valor == Decimal('100.00')
    assert soma_filtrada == pytest.approx(100.0)


def test_listar_saldos_filtra_por_mes(app, db_session):
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos, listar_saldos

    client = FakeSiafeClient(responses={
        (2026, 5): [_sample_api_row(saldo='100.00', ano='2026', mes='05', fonte='7.55')],
        (2026, 6): [_sample_api_row(saldo='200.00', ano='2026', mes='06', fonte='7.55')],
        (2025, 6): [_sample_api_row(saldo='400.00', ano='2025', mes='06', fonte='7.55')],
    })

    with app.app_context():
        sincronizar_saldos_periodos(
            [(2026, 5), (2026, 6), (2025, 6)],
            usuario_id=None,
            siafe_client=client,
        )
        # filtro só por mes=6 deve retornar 2 (2026/6 + 2025/6)
        pagination, soma_total, soma_filtrada = listar_saldos(mes='6')

    assert pagination.total == 2
    assert soma_filtrada == pytest.approx(200.0)

    with app.app_context():
        # combinacao ano=2026 + mes=6
        pagination, soma_total, soma_filtrada = listar_saldos(ano='2026', mes='6')

    assert pagination.total == 1
    assert pagination.items[0].valor == Decimal('200.00')
    assert soma_filtrada == pytest.approx(200.0)


def test_listar_saldos_card_acumulativo_pega_snapshot_anterior_quando_mes_filtrado_vazio(app, db_session):
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos, listar_saldos

    client = FakeSiafeClient(responses={
        (2026, 6): [_sample_api_row(saldo='4000000.00', ano='2026', mes='06', fonte='7.55')],
        (2026, 8): [_sample_api_row(saldo='4787700.23', ano='2026', mes='08', fonte='7.55')],
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 6), (2026, 8)], usuario_id=None, siafe_client=client)
        # Filtra julho/2026 (sem snapshot proprio). Tabela fica vazia, mas o card
        # 'Saldo Atual da Conta' mostra o snapshot acumulativo de junho.
        pagination_jul, _, soma_jul = listar_saldos(ano='2026', mes='7')
        # Sanity: filtrando agosto, tabela mostra agosto e card tambem.
        pagination_ago, _, soma_ago = listar_saldos(ano='2026', mes='8')

    assert pagination_jul.total == 0
    assert soma_jul == pytest.approx(4000000.00)
    assert pagination_ago.total == 1
    assert soma_ago == pytest.approx(4787700.23)


def test_listar_saldos_calcula_saldo_atual_sem_somar_periodos_historicos(app, db_session):
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos, listar_saldos

    client = FakeSiafeClient(responses={
        (2026, 2): [
            _sample_api_row(saldo='100.00', ano='2026', mes='02', fonte='7.55', exercicio='1'),
            _sample_api_row(saldo='40.00', ano='2026', mes='02', fonte='7.59', exercicio='1'),
        ],
        (2026, 3): [
            # Saldo repetido porque nao houve movimento na fonte; nao pode ser somado de novo.
            _sample_api_row(saldo='100.00', ano='2026', mes='03', fonte='7.55', exercicio='1'),
            _sample_api_row(saldo='60.00', ano='2026', mes='03', fonte='7.59', exercicio='1'),
        ],
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 2), (2026, 3)], usuario_id=None, siafe_client=client)
        pagination, soma_total, soma_filtrada = listar_saldos(ano='2026')

    assert pagination.total == 4
    assert soma_filtrada == pytest.approx(160.0)
    assert soma_filtrada != pytest.approx(300.0)


def test_listar_saldos_filtra_por_conta_bancaria(app, db_session):
    from app.services.fundo_rotativo_service import sincronizar_saldos_periodos, listar_saldos

    # Linha customizada com numero de conta diferente
    row_outra_conta = _sample_api_row(saldo='999.00', ano='2026', mes='05', fonte='1.00')
    for c in row_outra_conta['classificacao']:
        if c['codigoTipoClassificador'] == 101:
            c['nomeClassificador'] = '001.9999.88888'
            c['valoresClassificador'] = ['001', '9999', '88888']

    client = FakeSiafeClient(responses={
        (2026, 5): [
            _sample_api_row(saldo='100.00', ano='2026', mes='05', fonte='7.55'),
            _sample_api_row(saldo='50.00', ano='2026', mes='05', fonte='5.00'),
            row_outra_conta,
        ],
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 5)], usuario_id=None, siafe_client=client)
        pagination, soma_total, soma_filtrada = listar_saldos(conta_bancaria='95192')

    assert pagination.total == 2
    assert soma_filtrada == pytest.approx(150.0)

    with app.app_context():
        pagination, soma_total, soma_filtrada = listar_saldos(conta_bancaria='88888')

    assert pagination.total == 1
    assert pagination.items[0].valor == Decimal('999.00')
    assert soma_filtrada == pytest.approx(999.0)


def test_listar_contas_disponiveis(app, db_session):
    from app.services.fundo_rotativo_service import (
        listar_contas_disponiveis,
        sincronizar_saldos_periodos,
    )

    row_outra_conta = _sample_api_row(saldo='30.00', ano='2026', mes='05', fonte='1.00')
    for c in row_outra_conta['classificacao']:
        if c['codigoTipoClassificador'] == 101:
            c['nomeClassificador'] = '001.9999.88888'
            c['valoresClassificador'] = ['001', '9999', '88888']

    client = FakeSiafeClient(responses={
        (2026, 5): [
            _sample_api_row(saldo='10.00', ano='2026', mes='05', fonte='7.55'),
            _sample_api_row(saldo='20.00', ano='2026', mes='05', fonte='5.00'),
            row_outra_conta,
        ],
    })

    with app.app_context():
        sincronizar_saldos_periodos([(2026, 5)], usuario_id=None, siafe_client=client)
        contas = listar_contas_disponiveis()

    # Apenas o numero da conta, sem banco/agencia
    assert contas == [
        {'codigo': '88888', 'label': '88888'},
        {'codigo': '95192', 'label': '95192'},
    ]


def test_listar_meses_disponiveis(app, db_session):
    from app.services.fundo_rotativo_service import (
        listar_meses_disponiveis,
        sincronizar_saldos_periodos,
    )

    client = FakeSiafeClient(responses={
        (2026, 5): [_sample_api_row(saldo='100.00', ano='2026', mes='05', fonte='7.55')],
        (2026, 6): [_sample_api_row(saldo='200.00', ano='2026', mes='06', fonte='7.55')],
        (2025, 1): [_sample_api_row(saldo='10.00', ano='2025', mes='01', fonte='7.55')],
    })

    with app.app_context():
        sincronizar_saldos_periodos(
            [(2026, 5), (2026, 6), (2025, 1)],
            usuario_id=None,
            siafe_client=client,
        )
        meses = listar_meses_disponiveis()

    assert meses == [1, 5, 6]


def test_opcoes_saldo_dependem_dos_filtros_selecionados(app, db_session):
    from app.services.fundo_rotativo_service import (
        listar_opcoes_saldo_dependentes,
        sincronizar_saldos_periodos,
    )

    row_conta_88888 = _sample_api_row(saldo='200.00', ano='2026', mes='05', fonte='5.00')
    for c in row_conta_88888['classificacao']:
        if c['codigoTipoClassificador'] == 101:
            c['nomeClassificador'] = '001.9999.88888'
            c['valoresClassificador'] = ['001', '9999', '88888']

    client = FakeSiafeClient(responses={
        (2026, 6): [_sample_api_row(saldo='100.00', ano='2026', mes='06', fonte='7.55')],
        (2026, 5): [row_conta_88888],
        (2025, 6): [_sample_api_row(saldo='300.00', ano='2025', mes='06', fonte='7.59')],
    })

    with app.app_context():
        sincronizar_saldos_periodos(
            [(2026, 6), (2026, 5), (2025, 6)],
            usuario_id=None,
            siafe_client=client,
        )
        opcoes = listar_opcoes_saldo_dependentes(ano='2026', fonte_codigo='755')

    assert {f['codigo'] for f in opcoes['fontes']} == {'500', '755'}
    assert opcoes['meses'] == [6]
    assert opcoes['contas'] == [{'codigo': '95192', 'label': '95192'}]
    assert opcoes['anos'] == [2026]
    assert opcoes['exercicios'][0][0] == '01'


class TestFundoRotativoRotas:
    def test_get_lista_retorna_200_sem_crud_para_admin(self, client, db_session):
        _login_admin(client, db_session)

        resp = client.get('/financeiro/fundo-rotativo/saldo')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Fundo Rotativo' in html
        assert 'Sincronizar Saldos' in html
        assert 'Carga Inicial' not in html
        assert 'Novo Saldo' not in html
        assert 'Editar Saldo' not in html
        assert 'Excluir Saldo' not in html
        assert 'name="natureza"' not in html
        assert 'data-filter-option-row' in html

    def test_get_lista_mostra_saldo_filtrado_como_saldo_principal(self, app, client, db_session):
        from app.services.fundo_rotativo_service import sincronizar_saldos_periodos

        _login_admin(client, db_session)
        siafe_client = FakeSiafeClient(responses={
            (2026, 6): [
                _sample_api_row(saldo='2270492.88', ano='2026', mes='06', fonte='7.55', exercicio='1'),
                _sample_api_row(saldo='135660.50', ano='2026', mes='06', fonte='7.59', exercicio='1'),
                _sample_api_row(saldo='673046.30', ano='2026', mes='06', fonte='7.59', exercicio='2'),
                _sample_api_row(saldo='1598971.32', ano='2026', mes='06', fonte='7.55', exercicio='2'),
                _sample_api_row(saldo='109529.23', ano='2026', mes='06', fonte='5.00', exercicio='1'),
            ],
            (2026, 5): [
                _sample_api_row(saldo='999999.00', ano='2026', mes='05', fonte='7.55', exercicio='1'),
            ],
        })

        with app.app_context():
            sincronizar_saldos_periodos(
                [(2026, 6), (2026, 5)],
                usuario_id=None,
                siafe_client=siafe_client,
            )

        resp = client.get('/financeiro/fundo-rotativo/saldo?ano=2026&mes=6')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        match = re.search(r'<div class="fr-extrato-saldo"[^>]*>(.*?)</div>', html, re.S)
        assert match is not None
        saldo_principal = re.sub(r'\s+', ' ', match.group(1)).strip()
        assert 'R$ 4.787.700,23' in saldo_principal
        assert 'R$ 5.787.699,23' not in saldo_principal

    def test_get_lista_redireciona_se_nao_logado(self, client):
        resp = client.get('/financeiro/fundo-rotativo/saldo', follow_redirects=False)
        assert resp.status_code in (302, 401)

    def test_post_crud_nao_existe_mais(self, client, db_session):
        _login_admin(client, db_session)

        resp = client.post('/financeiro/fundo-rotativo/saldo/cadastrar')

        assert resp.status_code == 404

    def test_post_sincronizar_chama_servico(self, client, db_session, monkeypatch):
        import app.financeiro.routes.fundo_rotativo as routes

        usuario = _login_admin(client, db_session)
        chamadas = []

        def fake_sync(usuario_id):
            chamadas.append(usuario_id)
            return {'periodos': 1, 'registros': 5}

        monkeypatch.setattr(routes, 'sincronizar_saldos_mes_atual', fake_sync)

        resp = client.post(
            '/financeiro/fundo-rotativo/saldo/sincronizar',
            follow_redirects=False,
        )

        assert resp.status_code == 302
        assert chamadas == [usuario.id]

    def test_usuario_sem_permissao_nao_acessa_lista(self, client, db_session):
        _login_sem_permissao(client, db_session)

        resp = client.get('/financeiro/fundo-rotativo/saldo', follow_redirects=False)

        assert resp.status_code == 302


class TestJobSincronizacaoAutomatica:
    def test_job_chama_sincronizar_saldos_mes_atual(self, app, monkeypatch):
        import app.services.scheduler as scheduler_mod

        chamadas = []

        def fake_sync(usuario_id, siafe_client=None):
            chamadas.append(usuario_id)
            return {'periodos': 1, 'registros': 3}

        monkeypatch.setattr(
            scheduler_mod,
            'sincronizar_saldos_mes_atual',
            fake_sync,
        )

        scheduler_mod._job_sincronizar_saldos(app)

        assert chamadas == [None]

    def test_job_engole_excecoes_e_loga(self, app, monkeypatch, caplog):
        import app.services.scheduler as scheduler_mod

        def fake_sync(usuario_id, siafe_client=None):
            raise RuntimeError('siafe fora do ar')

        monkeypatch.setattr(
            scheduler_mod,
            'sincronizar_saldos_mes_atual',
            fake_sync,
        )

        # nao deve propagar a excecao
        scheduler_mod._job_sincronizar_saldos(app)
