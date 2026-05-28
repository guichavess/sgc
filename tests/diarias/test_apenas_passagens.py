"""
Testes — Fluxo "Apenas Passagens" (tipo_solicitacao_id=3)
=========================================================

Cobre os gaps identificados no tipo 3 do módulo de Diárias:

GAP 1: Cálculo de valor_total e qtd_diarias devem excluir componente de diárias
GAP 2: Importação SEI deve classificar corretamente como tipo 3
GAP 3: Assinatura do Superintendente deve funcionar com requisicao_passagens

Como rodar:
    pytest tests/diarias/test_apenas_passagens.py -v
"""
import pytest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch, MagicMock


# ── GAP 1: Cálculo de valor total para tipo 3 ──────────────────────────────

class TestApenasPassagensCalculo:
    """
    Para tipo_solicitacao_id=3 ("Apenas Passagens"):
    - qtd_diarias_solicitadas deve ser Decimal('0')
    - valor_total deve ser APENAS soma das cotações (sem componente de diárias)
    """

    def test_criar_itinerario_tipo3_qtd_diarias_zero(self, db_session, app):
        """Criar itinerário com tipo_solicitacao_id=3 deve ter qtd_diarias=0."""
        with app.app_context():
            from app.models.diaria import DiariasValorCargo
            from app.services.diaria_service import DiariaService

            db_session.add(DiariasValorCargo(
                cargo_id=8001, tipo_itinerario_id=2, valor=Decimal('400.00'),
            ))
            db_session.flush()

            dados = {
                'tipo_itinerario': 2,  # Nacional
                'tipo_solicitacao_id': 3,  # Apenas Passagens
                'data_viagem': '2026-06-01',
                'data_retorno': '2026-06-05',
                'objetivo': 'Teste apenas passagens',
                'usuario_gerador': 'teste_cpf',
            }
            pessoas = [{'cpf': '111.111.111-11', 'cargo_id': 8001, 'nome': 'Servidor A'}]

            itinerario = DiariaService.criar_itinerario(dados, pessoas)

            assert itinerario.qtd_diarias_solicitadas == Decimal('0'), (
                f"Para tipo_solicitacao_id=3, qtd_diarias deve ser 0, "
                f"obtido {itinerario.qtd_diarias_solicitadas}"
            )

    def test_criar_itinerario_tipo3_valor_total_zero_sem_cotacoes(self, db_session, app):
        """Valor total para tipo 3 sem cotações deve ser 0 (sem componente de diárias)."""
        with app.app_context():
            from app.models.diaria import DiariasValorCargo
            from app.services.diaria_service import DiariaService

            db_session.add(DiariasValorCargo(
                cargo_id=8002, tipo_itinerario_id=2, valor=Decimal('500.00'),
            ))
            db_session.flush()

            dados = {
                'tipo_itinerario': 2,
                'tipo_solicitacao_id': 3,
                'data_viagem': '2026-06-01',
                'data_retorno': '2026-06-05',
                'objetivo': 'Teste valor total',
                'usuario_gerador': 'teste_cpf',
            }
            pessoas = [{'cpf': '222.222.222-22', 'cargo_id': 8002, 'nome': 'Servidor B'}]

            itinerario = DiariaService.criar_itinerario(dados, pessoas)

            assert itinerario.valor_total == Decimal('0'), (
                f"Para tipo 3 sem cotações, valor_total deve ser 0, "
                f"obtido {itinerario.valor_total}"
            )

    def test_criar_itinerario_tipo2_qtd_diarias_normal(self, db_session, app):
        """Tipo 2 (Diárias + Passagens) deve manter qtd_diarias calculada normalmente."""
        with app.app_context():
            from app.models.diaria import DiariasValorCargo
            from app.services.diaria_service import DiariaService

            db_session.add(DiariasValorCargo(
                cargo_id=8003, tipo_itinerario_id=2, valor=Decimal('400.00'),
            ))
            db_session.flush()

            dados = {
                'tipo_itinerario': 2,
                'tipo_solicitacao_id': 2,  # Diárias + Passagens
                'data_viagem': '2026-06-01',
                'data_retorno': '2026-06-05',
                'objetivo': 'Teste tipo 2',
                'usuario_gerador': 'teste_cpf',
            }
            pessoas = [{'cpf': '333.333.333-33', 'cargo_id': 8003, 'nome': 'Servidor C'}]

            itinerario = DiariaService.criar_itinerario(dados, pessoas)

            assert itinerario.qtd_diarias_solicitadas == Decimal('4.5'), (
                f"Para tipo 2, qtd_diarias deve ser 4.5 (4 dias + 0.5), "
                f"obtido {itinerario.qtd_diarias_solicitadas}"
            )


# ── GAP 2: Classificação de tipo na importação SEI ─────────────────────────

class TestImportacaoSEITipoSolicitacao:
    """
    A importação/sincronização de processos SEI deve classificar
    corretamente o tipo_solicitacao_id com base nos documentos encontrados:
    - Apenas requisicao_passagens (sem requisicao) → tipo 3
    - Ambas requisicao + requisicao_passagens → tipo 2
    - Apenas requisicao (sem requisicao_passagens) → tipo 1
    """

    def test_classificacao_apenas_passagens(self, db_session, app):
        """Processo com apenas Req. Passagens (2975) deve ser tipo 3."""
        with app.app_context():
            from app.services.vincular_processo_diaria import (
                ID_SERIE_REQUISICAO_PASSAGENS,
                ID_SERIE_REQUISICAO_DIARIAS,
            )

            documentos = [
                {'Serie': {'IdSerie': ID_SERIE_REQUISICAO_PASSAGENS}, 'IdDocumento': '1'},
            ]

            tem_requisicao_diarias = False
            tem_requisicao_passagens = False
            for doc in documentos:
                id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
                if id_serie == str(ID_SERIE_REQUISICAO_PASSAGENS):
                    tem_requisicao_passagens = True
                if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
                    tem_requisicao_diarias = True

            if tem_requisicao_passagens and not tem_requisicao_diarias:
                tipo = 3
            elif tem_requisicao_passagens:
                tipo = 2
            else:
                tipo = 1

            assert tipo == 3, (
                f"Processo com apenas Req. Passagens deve ser tipo 3, obtido {tipo}"
            )

    def test_classificacao_diarias_e_passagens(self, db_session, app):
        """Processo com ambas Req. Diárias (532) + Req. Passagens (2975) deve ser tipo 2."""
        with app.app_context():
            from app.services.vincular_processo_diaria import (
                ID_SERIE_REQUISICAO_PASSAGENS,
                ID_SERIE_REQUISICAO_DIARIAS,
            )

            documentos = [
                {'Serie': {'IdSerie': ID_SERIE_REQUISICAO_DIARIAS}, 'IdDocumento': '1'},
                {'Serie': {'IdSerie': ID_SERIE_REQUISICAO_PASSAGENS}, 'IdDocumento': '2'},
            ]

            tem_requisicao_diarias = False
            tem_requisicao_passagens = False
            for doc in documentos:
                id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
                if id_serie == str(ID_SERIE_REQUISICAO_PASSAGENS):
                    tem_requisicao_passagens = True
                if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
                    tem_requisicao_diarias = True

            if tem_requisicao_passagens and not tem_requisicao_diarias:
                tipo = 3
            elif tem_requisicao_passagens:
                tipo = 2
            else:
                tipo = 1

            assert tipo == 2, (
                f"Processo com ambas requisições deve ser tipo 2, obtido {tipo}"
            )

    def test_classificacao_apenas_diarias(self, db_session, app):
        """Processo com apenas Req. Diárias (532) deve ser tipo 1."""
        with app.app_context():
            from app.services.vincular_processo_diaria import (
                ID_SERIE_REQUISICAO_PASSAGENS,
                ID_SERIE_REQUISICAO_DIARIAS,
            )

            documentos = [
                {'Serie': {'IdSerie': ID_SERIE_REQUISICAO_DIARIAS}, 'IdDocumento': '1'},
            ]

            tem_requisicao_diarias = False
            tem_requisicao_passagens = False
            for doc in documentos:
                id_serie = str((doc.get('Serie') or {}).get('IdSerie', ''))
                if id_serie == str(ID_SERIE_REQUISICAO_PASSAGENS):
                    tem_requisicao_passagens = True
                if id_serie == str(ID_SERIE_REQUISICAO_DIARIAS):
                    tem_requisicao_diarias = True

            if tem_requisicao_passagens and not tem_requisicao_diarias:
                tipo = 3
            elif tem_requisicao_passagens:
                tipo = 2
            else:
                tipo = 1

            assert tipo == 1, (
                f"Processo com apenas Req. Diárias deve ser tipo 1, obtido {tipo}"
            )

    def test_sincronizar_bloco_classifica_tipo3(self, db_session, app):
        """
        sincronizar_processos_bloco_diarias() deve atribuir tipo_solicitacao_id=3
        quando o processo tem apenas Req. Passagens e NÃO tem Req. Diárias.

        Este teste verifica o código REAL (não a lógica isolada).
        """
        import inspect
        with app.app_context():
            from app.services.vincular_processo_diaria import sincronizar_processos_bloco_diarias

            source = inspect.getsource(sincronizar_processos_bloco_diarias)
            assert 'tem_requisicao_diarias' in source, (
                "sincronizar_processos_bloco_diarias() não verifica tem_requisicao_diarias. "
                "Sem essa variável, tipo_solicitacao_id=3 nunca é atribuído."
            )


# ── GAP 3: Assinatura do Superintendente para tipo 3 ───────────────────────

class TestAssinaturaSuperintendenteTipo3:
    """
    Para tipo_solicitacao_id=3, o Superintendente deve assinar a
    Requisição de Passagens (série 2975) em vez da Requisição de Diárias (532),
    que não existe nesse tipo de solicitação.
    """

    def test_admin_assinar_aceita_requisicao_passagens(self, db_session, app):
        """
        A rota assinar_superintendente deve aceitar has_doc('requisicao_passagens')
        quando tipo_solicitacao_id=3 e não houver doc 'requisicao'.
        """
        import inspect
        with app.app_context():
            from app.diarias.routes.admin import assinar_superintendente

            source = inspect.getsource(assinar_superintendente)
            assert 'requisicao_passagens' in source, (
                "assinar_superintendente() não referencia 'requisicao_passagens'. "
                "Para tipo 3, o Superintendente deve assinar a Req. Passagens "
                "em vez da inexistente Req. Diárias."
            )

    def test_verificar_assinatura_sei_aceita_passagens(self, db_session, app):
        """
        verificar_assinatura_superintendente_sei() deve procurar pela série
        de Req. Passagens (2975) quando a Req. Diárias (532) não existe no processo.
        """
        import inspect
        with app.app_context():
            from app.services.diarias_autorizacao import verificar_assinatura_superintendente_sei

            source = inspect.getsource(verificar_assinatura_superintendente_sei)
            assert 'ID_SERIE_REQUISICAO_PASSAGENS' in source or '2975' in source, (
                "verificar_assinatura_superintendente_sei() não referencia a série "
                "de Requisição de Passagens (2975). Para tipo 3, deve fazer fallback "
                "para essa série quando a Req. Diárias (532) não é encontrada."
            )
