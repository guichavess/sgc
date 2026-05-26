"""
Testes da PK composta (codigoUG, codigo) da tabela reserva.

Cobre o fix do bug de duplicação massiva descoberto em 2026-05-26:
a tabela `reserva` foi criada pelo `pandas.to_sql` sem PK/UNIQUE, permitindo
que o script de sincronização inserisse a mesma reserva múltiplas vezes.

Cada teste deve:
- FALHAR antes do fix (model sem primary_key=True em codigoUG/codigo)
- PASSAR depois do fix
"""
import pytest
from sqlalchemy.exc import IntegrityError
from decimal import Decimal
from datetime import datetime


def _make_reserva(**overrides):
    """Cria uma Reserva com defaults válidos; aceita overrides por kwarg."""
    from app.models.reserva import Reserva
    defaults = dict(
        codigoUG='210101',
        codigo='2025NR99999',
        valor=Decimal('1000.00'),
        statusDocumento='CONTABILIZADO',
        dataEmissao=datetime(2025, 6, 1),
        codFonte=100,
        codNatureza=339030,
        codContrato='C-TESTE-1',
    )
    defaults.update(overrides)
    return Reserva(**defaults)


class TestPkComposta:
    """A PK composta (codigoUG, codigo) deve impedir duplicatas."""

    def test_inserir_duas_reservas_mesma_chave_falha(self, db_session):
        """Não pode existir duas linhas com mesmo (codigoUG, codigo)."""
        db_session.add(_make_reserva(codigoUG='210101', codigo='2025NR00001'))
        db_session.flush()

        db_session.add(_make_reserva(codigoUG='210101', codigo='2025NR00001'))
        with pytest.raises(IntegrityError):
            db_session.flush()

        db_session.rollback()

    def test_mesma_nr_em_ugs_diferentes_permitido(self, db_session):
        """codigo igual em UGs diferentes deve ser aceito (chave composta)."""
        db_session.add(_make_reserva(codigoUG='210101', codigo='2025NR00001'))
        db_session.add(_make_reserva(codigoUG='999999', codigo='2025NR00001'))
        db_session.flush()  # não deve falhar

    def test_busca_por_chave_composta_retorna_um(self, db_session):
        """Filtro por (codigoUG, codigo) retorna exatamente a reserva alvo."""
        from app.models.reserva import Reserva

        db_session.add(_make_reserva(
            codigoUG='210101', codigo='2025NR00010', valor=Decimal('123.45')
        ))
        db_session.add(_make_reserva(
            codigoUG='210101', codigo='2025NR00011', valor=Decimal('999.99')
        ))
        db_session.flush()

        found = (
            db_session.query(Reserva)
            .filter_by(codigoUG='210101', codigo='2025NR00010')
            .one()
        )
        assert found.valor == Decimal('123.45')


class TestUpsert:
    """O fluxo de UPSERT atualiza dados existentes sem criar duplicatas."""

    def test_upsert_atualiza_valor_sem_duplicar(self, db_session, app):
        """INSERT ... ON DUPLICATE KEY UPDATE não cria nova linha."""
        from sqlalchemy.dialects.mysql import insert as mysql_insert
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        from app.models.reserva import Reserva

        # Insere reserva inicial
        db_session.add(_make_reserva(
            codigoUG='210101',
            codigo='2025NR00050',
            valor=Decimal('500.00'),
            statusDocumento='CONTABILIZADO',
        ))
        db_session.flush()

        # Em SQLite (testes), usa-se ON CONFLICT — equivalente semântico do UPSERT
        # do MySQL (ON DUPLICATE KEY UPDATE). O service real chamará a versão MySQL.
        stmt = sqlite_insert(Reserva.__table__).values(
            codigoUG='210101',
            codigo='2025NR00050',
            valor=Decimal('750.00'),
            statusDocumento='CANCELADO',
            dataEmissao=datetime(2025, 6, 1),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['codigoUG', 'codigo'],
            set_={
                'valor': stmt.excluded.valor,
                'statusDocumento': stmt.excluded.statusDocumento,
            },
        )
        db_session.execute(stmt)
        db_session.flush()

        # Deve haver exatamente UMA linha — atualizada
        rows = (
            db_session.query(Reserva)
            .filter_by(codigoUG='210101', codigo='2025NR00050')
            .all()
        )
        assert len(rows) == 1
        assert rows[0].valor == Decimal('750.00')
        assert rows[0].statusDocumento == 'CANCELADO'
