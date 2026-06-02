"""
Testes do Dashboard do Fundo Rotativo (módulo financeiro).
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
        id_usuario_sei=f'fr_dash_admin_{uid}',
        nome='ADMIN FUNDO ROTATIVO DASH',
        sigla_login=f'fr_dash_admin_{uid}',
        is_admin=True,
        ativo=True,
    )
    db_session.add(usuario)
    db_session.flush()

    with client.session_transaction() as sess:
        sess['_user_id'] = str(usuario.id)
        sess['_fresh'] = True

    return usuario


def _make_saldo_siafe(valor, data, fonte_codigo, natureza=None, id_exercicio='01'):
    from app.models.fundo_rotativo import FundoRotativoSaldo

    fonte_formatada = {
        '755': '7.55',
        '759': '7.59',
        '500': '5.00',
    }.get(str(fonte_codigo), str(fonte_codigo))
    return FundoRotativoSaldo(
        valor=Decimal(valor),
        data=data,
        codigoUG='210102',
        conta_contabil='111111901',
        ano=data.year,
        mes=data.month,
        conta_corrente=f'001.3791.95192.{id_exercicio}.{fonte_formatada}.0000.0.000000',
        classificacaoStr=f'001.3791.95192.{id_exercicio}.{fonte_formatada}.0000.0.000000',
        fonte_codigo=str(fonte_codigo),
        fonte_formatada=fonte_formatada,
        natureza=natureza,
        id_exercicio=id_exercicio,
    )


def _seed_dashboard_data(db_session):
    from app.models.class_fonte import ClassFonte
    from app.models.contrato import Contrato
    from app.models.empenho import Empenho
    from app.models.liquidacao import Liquidacao
    from app.models.loa import Loa
    from app.models.nat_despesa import NatDespesa
    from app.models.ob import OB
    from app.models.pd import PD
    from app.models.reserva import Reserva

    ano_atual = datetime.now().year
    contrato_ug_fundo = Contrato(
        codigo='123',
        codigoUG='210102',
        nomeContratado='Fornecedor Fundo',
        nomeContratadoResumido='Fornecedor Fundo',
        objeto='Objeto do contrato do fundo',
    )
    contrato_ug_fundo_sem_exec = Contrato(
        codigo='456',
        codigoUG='210102',
        nomeContratado='Fornecedor Sem Execução',
        nomeContratadoResumido='Fornecedor Sem Execução',
        objeto='Contrato sem execução',
    )
    contrato_outra_ug = Contrato(
        codigo='999',
        codigoUG='210101',
        nomeContratado='Fornecedor Outra UG',
        nomeContratadoResumido='Fornecedor Outra UG',
    )

    db_session.add_all([
        ClassFonte(codigo='755', descricao='Recursos do Fundo Rotativo'),
        ClassFonte(codigo='759', descricao='Recursos Vinculados'),
        NatDespesa(codigo=339030, titulo='Material de Consumo'),
        NatDespesa(codigo=339039, titulo='Outros Servicos de Terceiros'),
        Loa(codigoUG='210102', codFonte='755', codNatureza='339030', ano=ano_atual),
        Loa(codigoUG='210102', codFonte='759', codNatureza='339039', ano=ano_atual),
        contrato_ug_fundo,
        contrato_ug_fundo_sem_exec,
        contrato_outra_ug,
        _make_saldo_siafe('1000.00', datetime(ano_atual, 1, 10), '755', '339030', '01'),
        _make_saldo_siafe('500.00', datetime(ano_atual, 1, 11), '759', '339039', '02'),
        Reserva(
            codigoUG='210102',
            codigo='NR001',
            statusDocumento='CONTABILIZADO',
            valor=Decimal('1000.00'),
            tipoAlteracao=None,
            codContrato='123',
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 12),
        ),
        Reserva(
            codigoUG='210102',
            codigo='NR002',
            statusDocumento='CONTABILIZADO',
            valor=Decimal('100.00'),
            tipoAlteracao='ANULACAO',
            codContrato='123',
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 13),
        ),
        Reserva(
            codigoUG='210101',
            codigo='NR003',
            statusDocumento='CONTABILIZADO',
            valor=Decimal('999.00'),
            tipoAlteracao=None,
            codContrato='999',
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 14),
        ),
        Empenho(
            statusDocumento='CONTABILIZADO',
            codigoUG='210102',
            valor=700.0,
            tipoAlteracaoNE=None,
            codContrato=123,
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 15),
        ),
        Liquidacao(
            statusDocumento='CONTABILIZADO',
            codigoUG='210102',
            valor=300.0,
            tipoAlteracao=None,
            codContrato=123,
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 16),
        ),
        Liquidacao(
            statusDocumento='CONTABILIZADO',
            codigoUG='210102',
            valor=50.0,
            tipoAlteracao='ANULACAO',
            codContrato=123,
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 17),
        ),
        Liquidacao(
            statusDocumento='CONTABILIZADO',
            codigoUG='210101',
            valor=999.0,
            tipoAlteracao=None,
            codContrato=999,
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 18),
        ),
        PD(
            codigo='PD001',
            statusDocumento='CONTABILIZADO',
            statusExecucao='STATUS_DISPONIVEL',
            codigoUG='210102',
            valor=80.0,
            codContrato=123,
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 19),
        ),
        OB(
            statusDocumento='CONTABILIZADO',
            codigoUG='210102',
            valor=200.0,
            codContrato='123',
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 20),
        ),
        OB(
            statusDocumento='CONTABILIZADO',
            codigoUG='210101',
            valor=999.0,
            codContrato='999',
            codFonte=755,
            codNatureza=339030,
            dataEmissao=datetime(ano_atual, 1, 21),
        ),
    ])
    db_session.flush()


class TestFundoRotativoDashboardService:
    def test_obter_dashboard_soma_ug_fundo_e_lista_todos_contratos(self, app, db_session):
        from app.services.fundo_rotativo_service import obter_dashboard_fundo_rotativo

        _seed_dashboard_data(db_session)

        with app.app_context():
            dashboard = obter_dashboard_fundo_rotativo()

        assert dashboard['kpis']['saldo_total'] == pytest.approx(1500.0)
        assert dashboard['kpis']['reservado'] == pytest.approx(900.0)
        assert dashboard['kpis']['liquidado'] == pytest.approx(250.0)
        assert dashboard['kpis']['pago'] == pytest.approx(200.0)

        rows = {row['contrato']: row for row in dashboard['rows']}
        assert set(rows) == {'123', '456'}
        assert rows['123']['credor'] == 'Fornecedor Fundo'
        assert rows['123']['objeto'] == 'Objeto do contrato do fundo'
        assert rows['123']['reserva'] == pytest.approx(900.0)
        assert rows['123']['empenho'] == pytest.approx(700.0)
        assert rows['123']['liquidacao'] == pytest.approx(250.0)
        assert rows['123']['pd'] == pytest.approx(80.0)
        assert rows['123']['pd_aberto'] == pytest.approx(80.0)
        assert rows['123']['pd_aberto_pds'] == [{
            'codigo': 'PD001',
            'competencia': '',
            'valor': 80.0,
        }]
        assert rows['123']['ob'] == pytest.approx(200.0)
        assert rows['456']['reserva'] == 0

    def test_obter_dashboard_filtra_saldo_por_ano_fonte_e_natureza_so_nas_execucoes(self, app, db_session):
        from app.models.liquidacao import Liquidacao
        from app.models.ob import OB
        from app.models.reserva import Reserva
        from app.services.fundo_rotativo_service import obter_dashboard_fundo_rotativo

        _seed_dashboard_data(db_session)
        ano_atual = datetime.now().year
        ano_anterior = ano_atual - 1

        db_session.add_all([
            _make_saldo_siafe('350.00', datetime(ano_atual, 2, 1), '755', '339030', '02'),
            Reserva(
                codigoUG='210102',
                codigo='NR900',
                statusDocumento='CONTABILIZADO',
                valor=Decimal('400.00'),
                tipoAlteracao=None,
                codContrato='123',
                codFonte=755,
                codNatureza=339030,
                dataEmissao=datetime(ano_anterior, 5, 1),
            ),
            Liquidacao(
                statusDocumento='CONTABILIZADO',
                codigoUG='210102',
                valor=120.0,
                tipoAlteracao=None,
                codContrato=123,
                codFonte=755,
                codNatureza=339030,
                dataEmissao=datetime(ano_anterior, 5, 2),
            ),
            OB(
                statusDocumento='CONTABILIZADO',
                codigoUG='210102',
                valor=70.0,
                codContrato='123',
                codFonte=755,
                codNatureza=339030,
                dataEmissao=datetime(ano_anterior, 5, 3),
            ),
        ])
        db_session.flush()

        with app.app_context():
            dashboard_atual = obter_dashboard_fundo_rotativo(
                ano=str(ano_atual),
                fonte_codigo='755',
                natureza='339030',
            )
            dashboard_anterior = obter_dashboard_fundo_rotativo(
                ano=str(ano_anterior),
                fonte_codigo='755',
                natureza='339030',
            )

        assert dashboard_atual['kpis']['saldo_total'] == pytest.approx(350.0)
        assert dashboard_atual['kpis']['reservado'] == pytest.approx(900.0)
        assert dashboard_atual['kpis']['liquidado'] == pytest.approx(250.0)
        assert dashboard_atual['kpis']['pago'] == pytest.approx(200.0)

        assert dashboard_anterior['kpis']['saldo_total'] == pytest.approx(0.0)
        assert dashboard_anterior['kpis']['reservado'] == pytest.approx(400.0)
        assert dashboard_anterior['kpis']['liquidado'] == pytest.approx(120.0)
        assert dashboard_anterior['kpis']['pago'] == pytest.approx(70.0)

    def test_dashboard_nao_filtra_saldo_por_natureza(self, app, db_session):
        from app.services.fundo_rotativo_service import obter_dashboard_fundo_rotativo

        _seed_dashboard_data(db_session)
        ano_atual = datetime.now().year
        db_session.add(_make_saldo_siafe('350.00', datetime(ano_atual, 2, 1), '755', '449052', '01'))
        db_session.flush()

        with app.app_context():
            dashboard = obter_dashboard_fundo_rotativo(
                ano=str(ano_atual),
                fonte_codigo='755',
                natureza='339030',
            )

        assert dashboard['kpis']['saldo_total'] == pytest.approx(350.0)

    def test_obter_dashboard_filtra_saldo_e_execucao_por_mes(self, app, db_session):
        from app.models.reserva import Reserva
        from app.services.fundo_rotativo_service import obter_dashboard_fundo_rotativo

        _seed_dashboard_data(db_session)
        ano_atual = datetime.now().year
        db_session.add_all([
            _make_saldo_siafe('350.00', datetime(ano_atual, 2, 1), '755', '339030', '01'),
            Reserva(
                codigoUG='210102',
                codigo='NR-FEV',
                statusDocumento='CONTABILIZADO',
                valor=Decimal('300.00'),
                tipoAlteracao=None,
                codContrato='123',
                codFonte=755,
                codNatureza=339030,
                dataEmissao=datetime(ano_atual, 2, 10),
            ),
        ])
        db_session.flush()

        with app.app_context():
            dashboard = obter_dashboard_fundo_rotativo(
                ano=str(ano_atual),
                mes='2',
                fonte_codigo='755',
                natureza='339030',
            )

        assert dashboard['kpis']['saldo_total'] == pytest.approx(350.0)
        assert dashboard['kpis']['reservado'] == pytest.approx(300.0)
        assert dashboard['kpis']['disponivel'] == pytest.approx(50.0)


class TestFundoRotativoDashboardRotas:
    def test_get_dashboard_retorna_200_com_kpis_e_tabela(self, client, db_session):
        _login_admin(client, db_session)
        _seed_dashboard_data(db_session)

        resp = client.get('/financeiro/fundo-rotativo/dashboard')

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'Fundo Rotativo — Dashboard' in html
        assert 'Saldo Total' in html
        assert 'Reservado' in html
        assert 'Liquidado' in html
        assert 'Pago' in html
        assert 'Execução por Contrato' in html
        assert 'Exportar Excel' in html
        assert 'Fornecedor Fundo' in html

    def test_get_dashboard_exibe_filtros_formatados_e_selecionados(self, client, db_session):
        _login_admin(client, db_session)
        _seed_dashboard_data(db_session)
        ano_atual = datetime.now().year

        resp = client.get(
            '/financeiro/fundo-rotativo/dashboard',
            query_string={
                'ano': str(ano_atual),
                'mes': '1',
                'fonte_codigo': '755',
                'natureza': '339030',
            },
        )

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert 'name="ano"' in html
        assert 'name="mes"' in html
        assert 'name="fonte_codigo"' in html
        assert 'name="natureza"' in html
        assert re.search(rf'name="ano"\s+value="{ano_atual}"[\s\S]{{0,120}}checked', html)
        assert re.search(r'name="mes"\s+value="1"[\s\S]{0,120}checked', html)
        assert re.search(r'name="fonte_codigo"\s+value="755"[\s\S]{0,120}checked', html)
        assert '755 - Recursos do Fundo Rotativo' in html
        assert re.search(r'name="natureza"\s+value="339030"[\s\S]{0,120}checked', html)
        assert '339030 - Material de Consumo' in html

    def test_get_dashboard_opcoes_dependem_dos_filtros_ativos(self, client, db_session):
        from app.models.reserva import Reserva

        _login_admin(client, db_session)
        _seed_dashboard_data(db_session)
        ano_atual = datetime.now().year
        db_session.add(Reserva(
            codigoUG='210102',
            codigo='NR-FEV-759',
            statusDocumento='CONTABILIZADO',
            valor=Decimal('80.00'),
            tipoAlteracao=None,
            codContrato='123',
            codFonte=759,
            codNatureza=339039,
            dataEmissao=datetime(ano_atual, 2, 10),
        ))
        db_session.flush()

        resp = client.get(
            '/financeiro/fundo-rotativo/dashboard',
            query_string={'ano': str(ano_atual), 'fonte_codigo': '755'},
        )

        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        assert re.search(r'name="mes"\s+value="1"', html)
        assert not re.search(r'name="mes"\s+value="2"', html)
        assert re.search(r'name="natureza"\s+value="339030"', html)
        assert not re.search(r'name="natureza"\s+value="339039"', html)
