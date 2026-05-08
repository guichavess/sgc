"""
Testes: uso do DataHora real do SEI para timestamps de assinaturas.

Garante que:
- parse_sei_datahora faz parsing correto do formato SEI
- verificar_assinatura_superintendente_sei retorna data_hora_assinatura
- verificar_assinaturas_requeridas retorna data_hora_ultima_assinatura
- verificar_assinaturas_requeridas retorna timestamps por papel (super/secretário)
- registrar_movimentacao aceita data_movimentacao customizada
"""
import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest


def _uid():
    return uuid.uuid4().hex[:8]


def _seed_etapas(db_session):
    """Garante que as etapas 1 e 3 existem no banco de testes."""
    from app.models.diaria import DiariasEtapa
    from app.constants import DiariasEtapaID
    for eid, nome, alias, ordem in [
        (DiariasEtapaID.SOLICITACAO_INICIAL, 'Solicitação Inicial', 'solicitacao_inicial', 1),
        (DiariasEtapaID.ANALISE_SOLICITACAO, 'Análise da Solicitação', 'analise_solicitacao', 3),
    ]:
        if not DiariasEtapa.query.get(int(eid)):
            db_session.add(DiariasEtapa(id=int(eid), nome=nome, alias=alias, ordem=ordem))
    db_session.flush()


def _criar_itinerario(db_session, etapa=1):
    from app.models.diaria import DiariasItinerario
    it = DiariasItinerario(
        usuario_gerador=f'gen_{_uid()}',
        tipo_solicitacao_id=1,
        tipo_itinerario=1,
        status_id=1,
        data_solicitacao=date(2026, 5, 1),
        data_viagem=datetime(2026, 6, 1),
        data_retorno=datetime(2026, 6, 3),
        qtd_diarias_solicitadas=Decimal('2.0'),
        etapa_atual_id=etapa,
    )
    db_session.add(it)
    db_session.flush()
    return it


class TestParseSeiDatahora:
    """Testes do helper parse_sei_datahora."""

    def test_formato_padrao(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        result = parse_sei_datahora('04/05/2026 10:35:59')
        assert result == datetime(2026, 5, 4, 10, 35, 59)

    def test_formato_com_espacos(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        result = parse_sei_datahora('  04/05/2026 10:35:59  ')
        assert result == datetime(2026, 5, 4, 10, 35, 59)

    def test_none_retorna_none(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        assert parse_sei_datahora(None) is None

    def test_string_vazia_retorna_none(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        assert parse_sei_datahora('') is None

    def test_formato_invalido_retorna_none(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        assert parse_sei_datahora('2026-05-04T10:35:59') is None

    def test_meia_noite(self):
        from app.services.diarias_autorizacao import parse_sei_datahora
        result = parse_sei_datahora('01/01/2026 00:00:00')
        assert result == datetime(2026, 1, 1, 0, 0, 0)


class TestRegistrarMovimentacaoDataCustomizada:
    """Testes para o parâmetro data_movimentacao em registrar_movimentacao."""

    def test_data_movimentacao_customizada(self, app, db_session):
        from app.services.diaria_service import DiariaService
        from app.constants import DiariasEtapaID

        _seed_etapas(db_session)
        itinerario = _criar_itinerario(db_session)

        data_sei = datetime(2026, 5, 4, 10, 35, 59)
        historico = DiariaService.registrar_movimentacao(
            itinerario.id,
            DiariasEtapaID.ANALISE_SOLICITACAO,
            data_movimentacao=data_sei,
            auto_commit=False,
        )

        assert historico.data_movimentacao == data_sei

    def test_data_movimentacao_default_now(self, app, db_session):
        from app.services.diaria_service import DiariaService
        from app.constants import DiariasEtapaID

        _seed_etapas(db_session)
        itinerario = _criar_itinerario(db_session)

        antes = datetime.now()
        historico = DiariaService.registrar_movimentacao(
            itinerario.id,
            DiariasEtapaID.ANALISE_SOLICITACAO,
            auto_commit=False,
        )
        depois = datetime.now()

        assert antes <= historico.data_movimentacao <= depois


class TestVerificarAssinaturasRequeridas:
    """Testa que data_hora_ultima_assinatura é extraída corretamente."""

    def test_retorna_data_hora_ultima_quando_completa(self, app):
        from app.services.diarias_assinaturas import verificar_assinaturas_requeridas
        from app.models.usuario import Usuario
        from unittest.mock import patch

        doc = {
            'DocumentoFormatado': '0023874518',
            'Serie': {'IdSerie': '532'},
            'Assinaturas': [
                {
                    'Nome': 'SECRETARIO TESTE',
                    'CargoFuncao': 'Secretário de Estado',
                    'DataHora': '04/05/2026 10:21:13',
                    'Sigla': 'sec@sead.pi.gov.br',
                    'IdOrigem': '00011122233',
                },
                {
                    'Nome': 'SUPER TESTE',
                    'CargoFuncao': 'Superintendente',
                    'DataHora': '04/05/2026 10:35:59',
                    'Sigla': 'super@sead.pi.gov.br',
                    'IdOrigem': '00044455566',
                },
            ],
        }

        with app.app_context():
            mock_result = {
                'completa': True,
                'tem_superintendente': True,
                'tem_secretario': True,
                'assinaturas': doc['Assinaturas'],
                'nomes': ['SECRETARIO TESTE', 'SUPER TESTE'],
                'assinantes': [
                    {'eh_superintendente': False, 'eh_secretario': True, 'via_banco': True, 'nome': 'SECRETARIO TESTE'},
                    {'eh_superintendente': True, 'eh_secretario': False, 'via_banco': True, 'nome': 'SUPER TESTE'},
                ],
            }

            with patch('app.services.diarias_assinaturas._carregar_indices_autorizadores') as mock_idx, \
                 patch('app.services.diarias_assinaturas._resolver_assinante') as mock_resolver:

                mock_idx.return_value = ({}, {})

                def side_effect(a, supers_idx, secs_idx):
                    nome = a.get('Nome', '')
                    if 'SUPER' in nome:
                        return {'eh_superintendente': True, 'eh_secretario': False, 'via_banco': True, 'nome': nome, 'usuario': None, 'cargo_texto': '', 'sigla': ''}
                    return {'eh_superintendente': False, 'eh_secretario': True, 'via_banco': True, 'nome': nome, 'usuario': None, 'cargo_texto': '', 'sigla': ''}

                mock_resolver.side_effect = side_effect

                result = verificar_assinaturas_requeridas(doc)

                assert result['completa'] is True
                assert result['data_hora_ultima_assinatura'] == datetime(2026, 5, 4, 10, 35, 59)

    def test_retorna_none_quando_incompleta(self, app):
        from app.services.diarias_assinaturas import verificar_assinaturas_requeridas
        from unittest.mock import patch

        doc = {
            'DocumentoFormatado': '0023874518',
            'Serie': {'IdSerie': '532'},
            'Assinaturas': [
                {
                    'Nome': 'SUPER TESTE',
                    'CargoFuncao': 'Superintendente',
                    'DataHora': '04/05/2026 10:35:59',
                    'Sigla': 'super@sead.pi.gov.br',
                    'IdOrigem': '00044455566',
                },
            ],
        }

        with app.app_context():
            with patch('app.services.diarias_assinaturas._carregar_indices_autorizadores') as mock_idx, \
                 patch('app.services.diarias_assinaturas._resolver_assinante') as mock_resolver:

                mock_idx.return_value = ({}, {})
                mock_resolver.return_value = {
                    'eh_superintendente': True, 'eh_secretario': False,
                    'via_banco': True, 'nome': 'SUPER TESTE',
                    'usuario': None, 'cargo_texto': '', 'sigla': '',
                }

                result = verificar_assinaturas_requeridas(doc)

                assert result['completa'] is False
                assert result['data_hora_ultima_assinatura'] is None


class TestTimestampsPorPapel:
    """Testa que data_hora_super e data_hora_secretario são retornados."""

    def test_retorna_timestamps_por_papel_quando_completa(self, app):
        from app.services.diarias_assinaturas import verificar_assinaturas_requeridas
        from unittest.mock import patch

        doc = {
            'DocumentoFormatado': '0023874518',
            'Serie': {'IdSerie': '532'},
            'Assinaturas': [
                {
                    'Nome': 'SECRETARIO TESTE',
                    'CargoFuncao': 'Secretário de Estado',
                    'DataHora': '04/05/2026 10:21:13',
                    'Sigla': 'sec@sead.pi.gov.br',
                    'IdOrigem': '00011122233',
                },
                {
                    'Nome': 'SUPER TESTE',
                    'CargoFuncao': 'Superintendente',
                    'DataHora': '04/05/2026 10:35:59',
                    'Sigla': 'super@sead.pi.gov.br',
                    'IdOrigem': '00044455566',
                },
            ],
        }

        with app.app_context():
            with patch('app.services.diarias_assinaturas._carregar_indices_autorizadores') as mock_idx, \
                 patch('app.services.diarias_assinaturas._resolver_assinante') as mock_resolver:

                mock_idx.return_value = ({}, {})

                def side_effect(a, supers_idx, secs_idx):
                    nome = a.get('Nome', '')
                    if 'SUPER' in nome:
                        return {'eh_superintendente': True, 'eh_secretario': False, 'via_banco': True, 'nome': nome, 'usuario': None, 'cargo_texto': '', 'sigla': ''}
                    return {'eh_superintendente': False, 'eh_secretario': True, 'via_banco': True, 'nome': nome, 'usuario': None, 'cargo_texto': '', 'sigla': ''}

                mock_resolver.side_effect = side_effect

                result = verificar_assinaturas_requeridas(doc)

                assert result['data_hora_super'] == datetime(2026, 5, 4, 10, 35, 59)
                assert result['data_hora_secretario'] == datetime(2026, 5, 4, 10, 21, 13)

    def test_timestamps_por_papel_quando_incompleta(self, app):
        from app.services.diarias_assinaturas import verificar_assinaturas_requeridas
        from unittest.mock import patch

        doc = {
            'DocumentoFormatado': '0023874518',
            'Serie': {'IdSerie': '532'},
            'Assinaturas': [
                {
                    'Nome': 'SUPER TESTE',
                    'CargoFuncao': 'Superintendente',
                    'DataHora': '04/05/2026 10:35:59',
                    'Sigla': 'super@sead.pi.gov.br',
                    'IdOrigem': '00044455566',
                },
            ],
        }

        with app.app_context():
            with patch('app.services.diarias_assinaturas._carregar_indices_autorizadores') as mock_idx, \
                 patch('app.services.diarias_assinaturas._resolver_assinante') as mock_resolver:

                mock_idx.return_value = ({}, {})
                mock_resolver.return_value = {
                    'eh_superintendente': True, 'eh_secretario': False,
                    'via_banco': True, 'nome': 'SUPER TESTE',
                    'usuario': None, 'cargo_texto': '', 'sigla': '',
                }

                result = verificar_assinaturas_requeridas(doc)

                assert result['data_hora_super'] == datetime(2026, 5, 4, 10, 35, 59)
                assert result['data_hora_secretario'] is None


class TestModelCamposAssinatura:
    """Testa que os novos campos do modelo persistem corretamente."""

    def test_campos_superintendente_nome(self, app, db_session):
        _seed_etapas(db_session)
        it = _criar_itinerario(db_session)

        it.superintendente_assinou = True
        it.superintendente_assinou_data = datetime(2026, 5, 4, 10, 35, 59)
        it.superintendente_assinou_nome = 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'
        db_session.flush()

        from app.models.diaria import DiariasItinerario
        reloaded = DiariasItinerario.query.get(it.id)
        assert reloaded.superintendente_assinou_nome == 'PEDRO ALEXANDRE CABRAL DE OLIVEIRA'
        assert reloaded.superintendente_assinou_data == datetime(2026, 5, 4, 10, 35, 59)

    def test_campos_secretario(self, app, db_session):
        _seed_etapas(db_session)
        it = _criar_itinerario(db_session)

        it.secretario_assinou = True
        it.secretario_assinou_data = datetime(2026, 5, 4, 10, 21, 13)
        it.secretario_assinou_nome = 'SAMUEL PONTES DO NASCIMENTO'
        db_session.flush()

        from app.models.diaria import DiariasItinerario
        reloaded = DiariasItinerario.query.get(it.id)
        assert reloaded.secretario_assinou is True
        assert reloaded.secretario_assinou_data == datetime(2026, 5, 4, 10, 21, 13)
        assert reloaded.secretario_assinou_nome == 'SAMUEL PONTES DO NASCIMENTO'

    def test_secretario_default_false(self, app, db_session):
        _seed_etapas(db_session)
        it = _criar_itinerario(db_session)
        db_session.flush()

        from app.models.diaria import DiariasItinerario
        reloaded = DiariasItinerario.query.get(it.id)
        assert reloaded.secretario_assinou is False
        assert reloaded.secretario_assinou_data is None
        assert reloaded.secretario_assinou_nome is None
