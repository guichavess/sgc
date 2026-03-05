"""
Repositório de Empenhos.
"""
from typing import List, Optional
from sqlalchemy import func, extract, case

from app.repositories.base import BaseRepository
from app.models import Empenho
from app.extensions import db
from app.constants import UG_CODE


class EmpenhoRepository(BaseRepository[Empenho]):
    """Repositório para operações com Empenhos."""

    model = Empenho

    @classmethod
    def buscar_por_codigo(cls, codigo: str) -> Optional[Empenho]:
        """Busca empenho pelo código."""
        return cls.model.query.filter_by(codigo=codigo).first()

    @classmethod
    def calcular_total_empenhado(
        cls,
        codigo_contrato: str,
        ano: int,
        codigo_ug: str = UG_CODE
    ) -> float:
        """
        Calcula o total empenhado para um contrato em um ano específico.

        Regra:
        - Filtra apenas statusDocumento = 'CONTABILIZADO'
        - Se tipoAlteracaoNE = 'ANULACAO' → valor * -1 (abate)
        - Caso contrário → valor positivo
        - Filtra por dataEmissao BETWEEN ano-01-01 e ano-12-31
        """
        from datetime import date
        cod_contrato_limpo = "".join(filter(str.isdigit, str(codigo_contrato)))

        data_inicio = date(ano, 1, 1)
        data_fim = date(ano, 12, 31)

        vlr_calculado = case(
            (cls.model.tipoAlteracaoNE == 'ANULACAO', cls.model.valor * -1),
            else_=cls.model.valor
        )

        result = db.session.query(
            func.sum(vlr_calculado)
        ).filter(
            cls.model.codigoUG == codigo_ug,
            cls.model.statusDocumento == 'CONTABILIZADO',
            cls.model.dataEmissao.between(data_inicio, data_fim),
            cls.model.codContrato == cod_contrato_limpo
        ).scalar()

        return float(result) if result else 0.0

    @classmethod
    def listar_por_contrato(
        cls,
        codigo_contrato: str,
        ano: Optional[int] = None
    ) -> List[Empenho]:
        """Lista empenhos de um contrato específico."""
        cod_contrato_limpo = "".join(filter(str.isdigit, str(codigo_contrato)))

        query = cls.model.query.filter(
            cls.model.codContrato == cod_contrato_limpo
        )

        if ano:
            query = query.filter(cls.model.anoProcesso == ano)

        return query.order_by(cls.model.dataEmissao.desc()).all()

    @classmethod
    def obter_natureza_despesa(
        cls,
        codigo_contrato: str,
        ano: int
    ) -> Optional[int]:
        """Obtém o código de natureza de despesa predominante."""
        cod_contrato_limpo = "".join(filter(str.isdigit, str(codigo_contrato)))

        empenho = cls.model.query.filter(
            cls.model.codContrato == cod_contrato_limpo,
            cls.model.anoProcesso == ano
        ).first()

        return empenho.codNatureza if empenho else None

    @classmethod
    def listar_por_ug(
        cls,
        codigo_ug: str = UG_CODE,
        ano: Optional[int] = None,
        status: str = 'CONTABILIZADO'
    ) -> List[Empenho]:
        """Lista empenhos de uma unidade gestora."""
        query = cls.model.query.filter(
            cls.model.codigoUG == codigo_ug,
            cls.model.statusDocumento == status
        )

        if ano:
            query = query.filter(cls.model.anoProcesso == ano)

        return query.order_by(cls.model.dataEmissao.desc()).all()

    @classmethod
    def contar_por_ano(cls, codigo_ug: str = UG_CODE) -> dict:
        """Conta empenhos agrupados por ano."""
        result = db.session.query(
            cls.model.anoProcesso,
            func.count(cls.model.id)
        ).filter(
            cls.model.codigoUG == codigo_ug
        ).group_by(cls.model.anoProcesso).all()

        return {ano: count for ano, count in result if ano}
