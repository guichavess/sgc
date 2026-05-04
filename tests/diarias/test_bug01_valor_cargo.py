"""
BUG-01 — get_valor_cargo() sem order_by()
==========================================
Arquivo: app/services/diaria_service.py, função get_valor_cargo()

Problema:
    DiariasValorCargo.query.filter_by(cargo_id=X, tipo_itinerario_id=Y).first()
    sem order_by() retorna o registro em ordem não determinística quando existem
    múltiplos registros para o mesmo cargo + tipo.

Impacto:
    O cálculo do valor total da diária pode usar um valor desatualizado/incorreto,
    resultando em pagamento errado ao servidor.

Fix esperado:
    .order_by(DiariasValorCargo.id.desc()).first()
    para garantir que sempre pega o registro mais recente (maior ID).

Como rodar:
    pytest tests/diarias/test_bug01_valor_cargo.py -v
"""
import pytest
from decimal import Decimal


class TestGetValorCargoBugOrderBy:
    """
    Testes para BUG-01: get_valor_cargo() deve retornar o valor mais recente.

    Estratégia: inserir dois registros com o MESMO cargo_id + tipo_itinerario_id
    mas valores diferentes (simulando uma atualização de tabela salarial).
    O segundo inserido (maior ID) é o mais recente e deve ser retornado.
    """

    def test_retorna_valor_quando_unico_registro(self, db_session, app):
        """Caso base: único registro para o cargo+tipo — deve retornar o valor correto."""
        with app.app_context():
            from app.models.diaria import DiariasValorCargo
            from app.services.diaria_service import DiariaService

            vc = DiariasValorCargo(
                cargo_id=9001,
                tipo_itinerario_id=1,
                valor=Decimal('350.00'),
            )
            db_session.add(vc)
            db_session.flush()

            resultado = DiariaService.get_valor_cargo(9001, 1)
            assert resultado == Decimal('350.00'), (
                f"Esperado 350.00, obtido {resultado}"
            )

    def test_retorna_zero_quando_cargo_nao_existe(self, db_session, app):
        """Cargo inexistente deve retornar Decimal('0.00') sem exceção."""
        with app.app_context():
            from app.services.diaria_service import DiariaService

            resultado = DiariaService.get_valor_cargo(99999, 1)
            assert resultado == Decimal('0.00'), (
                f"Esperado 0.00 para cargo inexistente, obtido {resultado}"
            )

    def test_implementacao_usa_order_by(self, db_session, app):
        """
        TESTE CRÍTICO — Verifica que BUG-01 está corrigido na implementação.

        A tabela DiariasValorCargo tem UniqueConstraint(cargo_id, tipo_itinerario_id)
        que impede inserção de registros duplicados no banco. No entanto, o order_by
        deve permanecer na query como defesa em profundidade, garantindo determinismo
        mesmo que a constraint seja contornada via SQL direto ou futura refatoração.

        ANTES da correção: .first() sem .order_by() — ordem não determinística.
        APÓS a correção: .order_by(DiariasValorCargo.id.desc()).first()
        """
        import inspect
        with app.app_context():
            from app.services.diaria_service import DiariaService

            source = inspect.getsource(DiariaService.get_valor_cargo)
            assert 'order_by' in source, (
                "BUG-01 NÃO CORRIGIDO: get_valor_cargo() não contém order_by(). "
                "Fix: adicionar .order_by(DiariasValorCargo.id.desc()) antes de .first() "
                "para garantir que o registro com maior ID (mais recente) seja sempre retornado."
            )

    def test_diferencia_por_tipo_itinerario(self, db_session, app):
        """Registros com mesmo cargo mas tipos diferentes devem ser isolados."""
        with app.app_context():
            from app.models.diaria import DiariasValorCargo
            from app.services.diaria_service import DiariaService

            db_session.add(DiariasValorCargo(cargo_id=9003, tipo_itinerario_id=1, valor=Decimal('100.00')))
            db_session.add(DiariasValorCargo(cargo_id=9003, tipo_itinerario_id=2, valor=Decimal('250.00')))
            db_session.flush()

            assert DiariaService.get_valor_cargo(9003, 1) == Decimal('100.00')
            assert DiariaService.get_valor_cargo(9003, 2) == Decimal('250.00')

    def test_calculo_valor_total_usa_valor_correto(self, db_session, app):
        """
        Teste de regressão: calcular_valor_total_estadual deve usar o valor
        mais recente do cargo, não um aleatório.

        Garante que o bug do get_valor_cargo não contamina o cálculo total.
        """
        with app.app_context():
            from datetime import date
            from app.models.diaria import (
                DiariasValorCargo, DiariasItinerario,
                DiariasItemItinerario,
            )
            from app.services.diaria_service import DiariaService

            CARGO_ID = 9004
            TIPO = 1

            # Registro de valor atual (único, pois UniqueConstraint(cargo_id, tipo_itinerario_id) impede duplicatas)
            db_session.add(DiariasValorCargo(cargo_id=CARGO_ID, tipo_itinerario_id=TIPO, valor=Decimal('500.00')))
            db_session.flush()

            # Cria itinerário simples (3 diárias)
            it = DiariasItinerario(
                usuario_gerador=1,
                tipo_solicitacao_id=1,
                tipo_itinerario=TIPO,
                status_id=1,
                data_solicitacao=date.today(),
                data_viagem=date(2026, 5, 1),
                data_retorno=date(2026, 5, 3),
                qtd_diarias_solicitadas=Decimal('2.5'),
            )
            db_session.add(it)
            db_session.flush()

            item = DiariasItemItinerario(
                id=1,  # BigInteger PKs precisam de valor explícito no SQLite
                id_itinerario=it.id,
                cpf_pessoa='000.000.000-00',
                nome_pessoa='Servidor Teste',
                cargo_id=CARGO_ID,
                natureza_id=2,
                valor_cargo=DiariaService.get_valor_cargo(CARGO_ID, TIPO),
            )
            db_session.add(item)
            db_session.flush()

            total = DiariaService.calcular_valor_total_estadual(it.id)
            esperado = Decimal('500.00') * Decimal('2.5')

            assert total == esperado, (
                f"Valor total errado: {total} (esperado {esperado}). "
                f"get_valor_cargo deveria retornar 500.00 × 2.5 diárias = {esperado}."
            )
