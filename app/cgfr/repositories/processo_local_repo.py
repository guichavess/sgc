"""
Repository MySQL para processos CGFR.
CRUD local usando BaseRepository.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import or_, and_

from app.repositories.base import BaseRepository
from app.cgfr.models import CgfrProcessoEnviado
from app.extensions import db


class ProcessoLocalRepository(BaseRepository[CgfrProcessoEnviado]):
    """Repository para operações CRUD na tabela cgfr_processo_enviado."""

    model = CgfrProcessoEnviado

    @classmethod
    def get_by_protocolo(cls, protocolo: str) -> Optional[CgfrProcessoEnviado]:
        """Busca processo pelo protocolo formatado (PK)."""
        return cls.model.query.get(protocolo)

    @classmethod
    def listar_com_filtros(cls, filtros: dict = None, search: str = None):
        """Lista processos com filtros dinâmicos.

        Args:
            filtros: Dict com filtros opcionais:
                - status: 'classificado' | 'pendente' | 'todos'
                - natureza_despesa_id: int
                - fonte_id: int
                - acao_id: int
                - tipo_processo: str
                - possui_reserva: 0|1
                - nivel_prioridade: str
            search: Texto para busca full-text.

        Returns:
            Query SQLAlchemy filtrada.
        """
        query = cls.model.query
        filtros = filtros or {}

        # Filtro de status
        status = filtros.get('status')
        if status == 'classificado':
            query = query.filter(
                and_(
                    cls.model.natureza_despesa_id.isnot(None),
                    cls.model.fonte_id.isnot(None),
                    cls.model.acao_id.isnot(None),
                )
            )
        elif status == 'pendente':
            query = query.filter(
                or_(
                    cls.model.natureza_despesa_id.is_(None),
                    cls.model.fonte_id.is_(None),
                    cls.model.acao_id.is_(None),
                )
            )

        # Filtros por coluna
        if filtros.get('natureza_despesa_id'):
            query = query.filter(cls.model.natureza_despesa_id == filtros['natureza_despesa_id'])
        if filtros.get('fonte_id'):
            query = query.filter(cls.model.fonte_id == filtros['fonte_id'])
        if filtros.get('acao_id'):
            query = query.filter(cls.model.acao_id == filtros['acao_id'])
        if filtros.get('tipo_processo'):
            query = query.filter(cls.model.tipo_processo == filtros['tipo_processo'])
        if filtros.get('possui_reserva') is not None and filtros['possui_reserva'] != '':
            query = query.filter(cls.model.possui_reserva == int(filtros['possui_reserva']))
        if filtros.get('nivel_prioridade'):
            query = query.filter(cls.model.nivel_prioridade == filtros['nivel_prioridade'])

        # Busca full-text
        if search:
            termo = f'%{search}%'
            query = query.filter(
                or_(
                    cls.model.processo_formatado.ilike(termo),
                    cls.model.especificacao.ilike(termo),
                    cls.model.tipo_processo.ilike(termo),
                    cls.model.fornecedor.ilike(termo),
                    cls.model.objeto_do_pedido.ilike(termo),
                )
            )

        return query.order_by(cls.model.data_inclusao.desc())

    @classmethod
    def classificar(cls, protocolo: str, dados: dict, usuario_id=None) -> Optional[CgfrProcessoEnviado]:
        """Atualiza campos editáveis de classificação.

        Args:
            protocolo: Processo formatado (PK).
            dados: Dict com campos editáveis a atualizar.
            usuario_id: ID do usuário que classificou (reservado para auditoria).

        Returns:
            Processo atualizado ou None se não encontrado.
        """
        processo = cls.get_by_protocolo(protocolo)
        if not processo:
            return None

        campos_editaveis = [
            'natureza_despesa_id', 'fonte_id', 'acao_id',
            'fornecedor', 'objeto_do_pedido', 'necessidade', 'deliberacao',
            'tipo_despesa', 'valor_solicitado', 'valor_aprovado',
            'data_da_reuniao', 'observacao', 'possui_reserva', 'valor_reserva',
            'nivel_prioridade',
        ]

        for campo in campos_editaveis:
            if campo in dados:
                valor = dados[campo]
                # Converter strings vazias em None para campos FK
                if campo in ('natureza_despesa_id', 'fonte_id', 'acao_id') and valor == '':
                    valor = None
                elif campo in ('natureza_despesa_id', 'fonte_id', 'acao_id') and valor is not None:
                    valor = int(valor)
                elif campo in ('valor_solicitado', 'valor_aprovado') and valor:
                    from decimal import Decimal
                    # Formato BR: "1.234,56" → Decimal
                    valor = str(valor).replace('.', '').replace(',', '.')
                    valor = Decimal(valor) if valor else None
                elif campo == 'possui_reserva' and valor is not None:
                    valor = int(valor) if valor != '' else 0
                setattr(processo, campo, valor)

        db.session.commit()
        return processo

    @classmethod
    def contar_por_status(cls) -> dict:
        """Conta processos por status de classificação."""
        total = cls.model.query.count()

        classificados = cls.model.query.filter(
            and_(
                cls.model.natureza_despesa_id.isnot(None),
                cls.model.fonte_id.isnot(None),
                cls.model.acao_id.isnot(None),
            )
        ).count()

        pendentes = total - classificados

        return {
            'total': total,
            'classificados': classificados,
            'pendentes': pendentes,
        }

    @classmethod
    def get_all_for_export(cls, filtros: dict = None, search: str = None):
        """Retorna todos os processos filtrados para exportação."""
        return cls.listar_com_filtros(filtros, search).all()
