"""
Repository MySQL para processos CGFR.
CRUD local usando BaseRepository.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import or_, and_

from sqlalchemy.orm import joinedload

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
        query = cls.model.query.filter(
            cls.model.filtro_visiveis()
        ).options(
            joinedload(cls.model.natureza_rel),
            joinedload(cls.model.fonte_rel),
        )
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

        from decimal import Decimal, InvalidOperation
        from datetime import date as dt_date

        def _parse_decimal(val):
            """Converte string monetária BR/US para Decimal ou None."""
            s = str(val).strip() if val else ''
            if not s:
                return None
            if ',' in s:
                # Formato BR: "1.234,56" → "1234.56"
                s = s.replace('.', '').replace(',', '.')
            try:
                return Decimal(s)
            except (InvalidOperation, ValueError):
                return None

        def _parse_int(val):
            """Converte para int, retorna None se vazio/inválido."""
            if val is None or val == '':
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        def _parse_date(val):
            """Converte ISO 'YYYY-MM-DD' para date, retorna None se inválido."""
            if not val or val == '-':
                return None
            try:
                return dt_date.fromisoformat(str(val))
            except (ValueError, TypeError):
                return None

        for campo in campos_editaveis:
            if campo in dados:
                valor = dados[campo]

                if campo in ('natureza_despesa_id', 'fonte_id', 'acao_id'):
                    valor = _parse_int(valor)
                elif campo in ('valor_solicitado', 'valor_aprovado'):
                    valor = _parse_decimal(valor)
                elif campo == 'data_da_reuniao':
                    valor = _parse_date(valor)
                elif campo == 'possui_reserva':
                    valor = _parse_int(valor) or 0
                elif campo == 'valor_reserva':
                    # VARCHAR(30): limpar e armazenar como texto
                    valor = str(valor).strip()[:30] if valor else ''
                elif campo == 'nivel_prioridade':
                    # VARCHAR(10): truncar para evitar erro de overflow
                    valor = str(valor).strip()[:10] if valor else ''
                elif campo == 'tipo_despesa':
                    valor = str(valor).strip()[:50] if valor else ''
                else:
                    # Campos texto livres: converter None para ''
                    valor = str(valor).strip() if valor else ''

                setattr(processo, campo, valor)

        db.session.commit()
        return processo

    @classmethod
    def contar_por_status(cls) -> dict:
        """Conta processos por status de classificação."""
        base = cls.model.query.filter(
            cls.model.filtro_visiveis()
        )
        total = base.count()

        classificados = base.filter(
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
