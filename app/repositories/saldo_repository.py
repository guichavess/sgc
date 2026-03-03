"""
Repositório de Saldos.
"""
from typing import Optional
from datetime import datetime

from app.repositories.base import BaseRepository
from app.models import SaldoEmpenho
from app.extensions import db


class SaldoRepository(BaseRepository[SaldoEmpenho]):
    """Repositório para operações com Saldos de Empenho."""

    model = SaldoEmpenho

    @classmethod
    def get_saldo_atual(
        cls,
        codigo_contrato: str,
        competencia: str
    ) -> Optional[SaldoEmpenho]:
        """Busca o saldo mais recente para um contrato e competência."""
        return cls.model.query.filter_by(
            cod_contrato=codigo_contrato,
            competencia=competencia
        ).order_by(cls.model.data.desc()).first()

    @classmethod
    def get_valor_saldo(
        cls,
        codigo_contrato: str,
        competencia: str
    ) -> float:
        """Retorna o valor do saldo atual ou 0.0 se não existir."""
        saldo = cls.get_saldo_atual(codigo_contrato, competencia)
        return float(saldo.saldo) if saldo else 0.0

    @classmethod
    def registrar_saldo(
        cls,
        codigo_contrato: str,
        competencia: str,
        valor: float
    ) -> SaldoEmpenho:
        """Registra um novo saldo para o contrato."""
        saldo = cls.model(
            cod_contrato=codigo_contrato,
            competencia=competencia,
            saldo=valor,
            data=datetime.now()
        )
        db.session.add(saldo)
        db.session.commit()
        return saldo

    @classmethod
    def atualizar_ou_criar(
        cls,
        codigo_contrato: str,
        competencia: str,
        valor: float
    ) -> SaldoEmpenho:
        """Atualiza o saldo existente ou cria um novo."""
        saldo_existente = cls.get_saldo_atual(codigo_contrato, competencia)

        if saldo_existente:
            saldo_existente.saldo = valor
            saldo_existente.data = datetime.now()
            db.session.commit()
            return saldo_existente

        return cls.registrar_saldo(codigo_contrato, competencia, valor)

    @classmethod
    def listar_por_contrato(cls, codigo_contrato: str):
        """Lista histórico de saldos de um contrato."""
        return cls.model.query.filter_by(
            cod_contrato=codigo_contrato
        ).order_by(cls.model.data.desc()).all()

    @classmethod
    def listar_por_competencia(cls, competencia: str):
        """Lista todos os saldos de uma competência."""
        return cls.model.query.filter_by(
            competencia=competencia
        ).order_by(cls.model.cod_contrato).all()
