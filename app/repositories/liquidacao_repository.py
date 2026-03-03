"""
Repositório de Liquidações.
"""
from typing import List, Optional
from sqlalchemy import func, case, extract

from app.repositories.base import BaseRepository
from app.models.liquidacao import Liquidacao
from app.extensions import db
from app.constants import UG_CODE


class LiquidacaoRepository(BaseRepository[Liquidacao]):
    """Repositório para operações com Liquidações do SIAFE."""

    model = Liquidacao

    @classmethod
    def calcular_total_liquidado(
        cls,
        codigo_contrato: str,
        ano: int,
        codigo_ug: str = UG_CODE
    ) -> float:
        """
        Calcula o total liquidado para um contrato em um ano específico.

        Regra:
        - statusDocumento = 'CONTABILIZADO' → soma valor positivo
        - statusDocumento = 'ANULADO' → soma valor * -1 (abate)
        - Filtra por dataEmissao BETWEEN ano-01-01 e ano-12-31
        """
        from datetime import date
        cod_contrato_limpo = "".join(filter(str.isdigit, str(codigo_contrato)))

        data_inicio = date(ano, 1, 1)
        data_fim = date(ano, 12, 31)

        vlr_calculado = case(
            (cls.model.statusDocumento == 'ANULADO', cls.model.valor * -1),
            else_=cls.model.valor
        )

        result = db.session.query(
            func.sum(vlr_calculado)
        ).filter(
            cls.model.codigoUG == codigo_ug,
            cls.model.statusDocumento.in_(['CONTABILIZADO', 'ANULADO']),
            cls.model.dataEmissao.between(data_inicio, data_fim),
            cls.model.codContrato == cod_contrato_limpo
        ).scalar()

        return float(result) if result else 0.0
