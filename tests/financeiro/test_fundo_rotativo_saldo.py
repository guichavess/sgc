"""
Testes do CRUD de Saldo do Fundo Rotativo (módulo financeiro).
"""
from datetime import datetime
from decimal import Decimal
import uuid

import pytest
from sqlalchemy import text


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


def _make_fonte(db_session, codigo=None, descricao='FONTE TESTE'):
    from app.models.class_fonte import ClassFonte

    fonte = ClassFonte(codigo=codigo or f'F{_uid()[:5]}', descricao=descricao)
    db_session.add(fonte)
    db_session.flush()
    return fonte


def _seed_natureza_disponivel(db_session, codigo='339030', titulo='Material de Consumo'):
    db_session.execute(
        text("INSERT INTO natdespesas (codigo, titulo) VALUES (:codigo, :titulo)"),
        {'codigo': int(codigo), 'titulo': titulo},
    )
    db_session.execute(
        text(
            "INSERT INTO loa (row_id, codigoUG, ano, mes, id, codNatureza) "
            "VALUES (:row_id, '210102', 2026, 1, '622110101', :codigo)"
        ),
        {'row_id': int(f'9{codigo[-5:]}'), 'codigo': codigo},
    )
    db_session.flush()


# ─────────────────────────────────────────────────────────────────────────────
# Service Layer
# ─────────────────────────────────────────────────────────────────────────────
class TestFundoRotativoService:
    def test_criar_saldo_com_dados_validos(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo
        from app.models.fundo_rotativo import FundoRotativoSaldo

        fonte = _make_fonte(db_session, codigo='100')

        with app.app_context():
            saldo = criar_saldo(
                valor=Decimal('1500.50'),
                data=datetime(2026, 5, 1, 10, 0),
                fonte_codigo=fonte.codigo,
                id_exercicio='01',
                usuario_id=None,
            )

            assert saldo.id is not None
            assert saldo.valor == Decimal('1500.50')
            assert saldo.fonte_codigo == fonte.codigo
            assert saldo.id_exercicio == '01'
            assert FundoRotativoSaldo.query.count() == 1

    def test_criar_saldo_rejeita_exercicio_invalido(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo

        fonte = _make_fonte(db_session)

        with app.app_context():
            with pytest.raises(ValueError, match='[Ee]xerc'):
                criar_saldo(
                    valor=Decimal('100'),
                    data=datetime(2026, 5, 1),
                    fonte_codigo=fonte.codigo,
                    id_exercicio='99',
                    usuario_id=None,
                )

    def test_criar_saldo_rejeita_fonte_inexistente(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo

        with app.app_context():
            with pytest.raises(ValueError, match='[Ff]onte'):
                criar_saldo(
                    valor=Decimal('100'),
                    data=datetime(2026, 5, 1),
                    fonte_codigo='INEXISTENTE',
                    id_exercicio='01',
                    usuario_id=None,
                )

    def test_criar_saldo_rejeita_valor_zero_ou_negativo(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo

        fonte = _make_fonte(db_session)

        with app.app_context():
            with pytest.raises(ValueError, match='[Vv]alor'):
                criar_saldo(
                    valor=Decimal('0'),
                    data=datetime(2026, 5, 1),
                    fonte_codigo=fonte.codigo,
                    id_exercicio='01',
                    usuario_id=None,
                )

    def test_atualizar_saldo(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo, atualizar_saldo

        fonte = _make_fonte(db_session, codigo='200')

        with app.app_context():
            saldo = criar_saldo(
                valor=Decimal('100'),
                data=datetime(2026, 5, 1),
                fonte_codigo=fonte.codigo,
                id_exercicio='01',
                usuario_id=None,
            )

            atualizado = atualizar_saldo(
                saldo.id,
                valor=Decimal('999.99'),
                data=datetime(2026, 6, 1),
                fonte_codigo=fonte.codigo,
                id_exercicio='02',
            )

            assert atualizado.valor == Decimal('999.99')
            assert atualizado.id_exercicio == '02'
            assert atualizado.data == datetime(2026, 6, 1)

    def test_excluir_saldo(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo, excluir_saldo
        from app.models.fundo_rotativo import FundoRotativoSaldo

        fonte = _make_fonte(db_session)

        with app.app_context():
            saldo = criar_saldo(
                valor=Decimal('100'),
                data=datetime(2026, 5, 1),
                fonte_codigo=fonte.codigo,
                id_exercicio='01',
                usuario_id=None,
            )
            saldo_id = saldo.id

            excluir_saldo(saldo_id)

            assert FundoRotativoSaldo.query.get(saldo_id) is None

    def test_listar_saldos_pagina_e_ordena_por_data_desc(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo, listar_saldos

        fonte = _make_fonte(db_session)

        with app.app_context():
            criar_saldo(Decimal('100'), datetime(2026, 5, 1), fonte.codigo, '01', None)
            criar_saldo(Decimal('200'), datetime(2026, 5, 3), fonte.codigo, '01', None)
            criar_saldo(Decimal('300'), datetime(2026, 5, 2), fonte.codigo, '02', None)

            pagination = listar_saldos(page=1)

            assert pagination.total == 3
            datas = [s.data for s in pagination.items]
            assert datas == sorted(datas, reverse=True)

    def test_criar_saldo_com_natureza(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo

        fonte = _make_fonte(db_session, codigo='770')

        with app.app_context():
            saldo = criar_saldo(
                valor=Decimal('500'),
                data=datetime(2026, 5, 1),
                fonte_codigo=fonte.codigo,
                id_exercicio='01',
                usuario_id=None,
                natureza='339030',
            )
            assert saldo.natureza == '339030'

    def test_filtrar_por_ano(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo, listar_saldos

        fonte = _make_fonte(db_session, codigo='780')

        with app.app_context():
            criar_saldo(Decimal('100'), datetime(2024, 5, 1), fonte.codigo, '01', None)
            criar_saldo(Decimal('200'), datetime(2025, 5, 1), fonte.codigo, '01', None)
            criar_saldo(Decimal('300'), datetime(2026, 5, 1), fonte.codigo, '01', None)

            pagination = listar_saldos(ano=2025)

            assert pagination.total == 1
            assert pagination.items[0].valor == Decimal('200')

    def test_filtrar_por_natureza(self, app, db_session):
        from app.services.fundo_rotativo_service import criar_saldo, listar_saldos

        fonte = _make_fonte(db_session, codigo='790')

        with app.app_context():
            criar_saldo(Decimal('100'), datetime(2026, 5, 1), fonte.codigo, '01', None, natureza='339030')
            criar_saldo(Decimal('200'), datetime(2026, 5, 2), fonte.codigo, '01', None, natureza='449052')
            criar_saldo(Decimal('300'), datetime(2026, 5, 3), fonte.codigo, '01', None, natureza=None)

            pagination = listar_saldos(natureza='339030')

            assert pagination.total == 1
            assert pagination.items[0].valor == Decimal('100')

    def test_listar_naturezas_disponiveis_com_codigo_titulo_e_label(self, app, db_session):
        from app.services.fundo_rotativo_service import listar_naturezas_disponiveis

        _seed_natureza_disponivel(db_session, codigo='339030', titulo='Material de Consumo')

        with app.app_context():
            naturezas = listar_naturezas_disponiveis()

            assert naturezas == [{
                'codigo': '339030',
                'titulo': 'Material de Consumo',
                'label': '339030 - Material de Consumo',
            }]


# ─────────────────────────────────────────────────────────────────────────────
# Rotas HTTP
# ─────────────────────────────────────────────────────────────────────────────
class TestFundoRotativoRotas:
    def test_get_lista_retorna_200_para_admin(self, client, db_session):
        _login_admin(client, db_session)
        _make_fonte(db_session)

        resp = client.get('/financeiro/fundo-rotativo/saldo')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Saldo' in html or 'Fundo Rotativo' in html

    def test_get_lista_formata_filtro_natureza_com_codigo_e_titulo(self, client, db_session):
        _login_admin(client, db_session)
        _seed_natureza_disponivel(db_session, codigo='339030', titulo='Material de Consumo')

        resp = client.get('/financeiro/fundo-rotativo/saldo')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'value="339030"' in html
        assert '339030 - Material de Consumo' in html

    def test_get_lista_redireciona_se_nao_logado(self, client):
        resp = client.get('/financeiro/fundo-rotativo/saldo', follow_redirects=False)
        assert resp.status_code in (302, 401)

    def test_post_cadastrar_cria_registro(self, client, db_session):
        from app.models.fundo_rotativo import FundoRotativoSaldo

        _login_admin(client, db_session)
        fonte = _make_fonte(db_session, codigo='300')

        resp = client.post(
            '/financeiro/fundo-rotativo/saldo/cadastrar',
            data={
                'valor': '1234.56',
                'data': '2026-05-15T10:30',
                'fonte_codigo': fonte.codigo,
                'natureza': '339030',
                'id_exercicio': '01',
            },
            follow_redirects=False,
        )

        assert resp.status_code == 302
        registros = FundoRotativoSaldo.query.all()
        assert len(registros) == 1
        assert registros[0].valor == Decimal('1234.56')
        assert registros[0].fonte_codigo == '300'
        assert registros[0].id_exercicio == '01'
        assert registros[0].natureza == '339030'

    def test_post_cadastrar_recusa_dados_invalidos(self, client, db_session):
        from app.models.fundo_rotativo import FundoRotativoSaldo

        _login_admin(client, db_session)
        _make_fonte(db_session, codigo='400')

        resp = client.post(
            '/financeiro/fundo-rotativo/saldo/cadastrar',
            data={
                'valor': '',
                'data': '',
                'fonte_codigo': '',
                'id_exercicio': '',
            },
            follow_redirects=False,
        )

        assert resp.status_code == 302
        assert FundoRotativoSaldo.query.count() == 0

    def test_post_editar_atualiza_registro(self, client, db_session):
        from app.services.fundo_rotativo_service import criar_saldo
        from app.models.fundo_rotativo import FundoRotativoSaldo

        _login_admin(client, db_session)
        fonte = _make_fonte(db_session, codigo='500')

        saldo = criar_saldo(
            valor=Decimal('100'),
            data=datetime(2026, 5, 1),
            fonte_codigo=fonte.codigo,
            id_exercicio='01',
            usuario_id=None,
        )

        resp = client.post(
            f'/financeiro/fundo-rotativo/saldo/{saldo.id}/editar',
            data={
                'valor': '2000.00',
                'data': '2026-06-10T14:00',
                'fonte_codigo': fonte.codigo,
                'id_exercicio': '02',
            },
            follow_redirects=False,
        )

        assert resp.status_code == 302
        registro = FundoRotativoSaldo.query.get(saldo.id)
        assert registro.valor == Decimal('2000.00')
        assert registro.id_exercicio == '02'

    def test_post_excluir_remove_registro(self, client, db_session):
        from app.services.fundo_rotativo_service import criar_saldo
        from app.models.fundo_rotativo import FundoRotativoSaldo

        _login_admin(client, db_session)
        fonte = _make_fonte(db_session, codigo='600')

        saldo = criar_saldo(
            valor=Decimal('100'),
            data=datetime(2026, 5, 1),
            fonte_codigo=fonte.codigo,
            id_exercicio='01',
            usuario_id=None,
        )

        resp = client.post(
            f'/financeiro/fundo-rotativo/saldo/{saldo.id}/excluir',
            follow_redirects=False,
        )

        assert resp.status_code == 302
        assert FundoRotativoSaldo.query.get(saldo.id) is None

    def test_usuario_sem_permissao_nao_acessa_lista(self, client, db_session):
        _login_sem_permissao(client, db_session)

        resp = client.get('/financeiro/fundo-rotativo/saldo', follow_redirects=False)

        # Sem permissão: redireciona (302) para hub ou login
        assert resp.status_code == 302
