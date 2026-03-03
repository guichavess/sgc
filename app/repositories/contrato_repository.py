"""
Repositório de Contratos.
"""
from typing import List, Optional
from sqlalchemy import or_

from app.repositories.base import BaseRepository
from app.models import Contrato


class ContratoRepository(BaseRepository[Contrato]):
    """Repositório para operações com Contratos."""

    model = Contrato

    @classmethod
    def buscar_por_codigo(cls, codigo: str) -> Optional[Contrato]:
        """Busca contrato pelo código."""
        return cls.model.query.filter_by(codigo=codigo).first()

    @classmethod
    def buscar_por_termo(cls, termo: str, limite: int = 10) -> List[Contrato]:
        """
        Busca contratos por termo (código, nome do contratado ou objeto).
        Usado para autocomplete.
        """
        termo_like = f'%{termo}%'

        return cls.model.query.filter(
            or_(
                cls.model.codigo.ilike(termo_like),
                cls.model.nomeContratado.ilike(termo_like),
                cls.model.nomeContratadoResumido.ilike(termo_like),
                cls.model.objeto.ilike(termo_like)
            )
        ).limit(limite).all()

    @classmethod
    def listar_ativos(cls) -> List[Contrato]:
        """Lista contratos com situação ativa."""
        return cls.model.query.filter(
            cls.model.situacao.in_(['ATIVO', 'VIGENTE', 'EM EXECUÇÃO'])
        ).order_by(cls.model.codigo).all()

    @classmethod
    def listar_por_contratado(cls, nome_contratado: str) -> List[Contrato]:
        """Lista contratos de um contratado específico."""
        return cls.model.query.filter(
            cls.model.nomeContratado.ilike(f'%{nome_contratado}%')
        ).order_by(cls.model.codigo).all()

    @classmethod
    def listar_contratados_distintos(cls) -> List[str]:
        """Retorna lista de nomes de contratados únicos."""
        from app.extensions import db

        result = db.session.query(
            cls.model.nomeContratadoResumido
        ).distinct().filter(
            cls.model.nomeContratadoResumido.isnot(None)
        ).order_by(cls.model.nomeContratadoResumido).all()

        return [c[0] for c in result if c[0]]

    @classmethod
    def to_dict(cls, contrato: Contrato) -> dict:
        """Converte contrato para dicionário (para JSON)."""
        return {
            'codigo': contrato.codigo,
            'situacao': contrato.situacao,
            'numeroOriginal': contrato.numeroOriginal,
            'numProcesso': contrato.numProcesso,
            'objeto': contrato.objeto,
            'nomeContratado': contrato.nomeContratado,
            'nomeContratadoResumido': contrato.nomeContratadoResumido,
            'valor': float(contrato.valor) if contrato.valor else None,
        }
