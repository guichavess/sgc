"""
Repositório de Solicitações.
"""
from typing import List, Optional
from datetime import datetime
from sqlalchemy import or_, func

from app.repositories.base import BaseRepository
from app.models import Solicitacao, Contrato, Etapa, HistoricoMovimentacao
from app.extensions import db


class SolicitacaoRepository(BaseRepository[Solicitacao]):
    """Repositório para operações com Solicitações."""

    model = Solicitacao

    @classmethod
    def get_com_contrato(cls, solicitacao_id: int) -> Optional[Solicitacao]:
        """Busca solicitação com dados do contrato carregados."""
        return cls.model.query.options(
            db.joinedload(cls.model.contrato)
        ).get(solicitacao_id)

    @classmethod
    def listar_por_usuario(cls, usuario_id: int, page: int = 1, per_page: int = 20):
        """Lista solicitações de um usuário específico."""
        return cls.model.query.filter_by(
            id_usuario_solicitante=usuario_id
        ).order_by(
            cls.model.data_solicitacao.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)

    @classmethod
    def listar_com_filtros(
        cls,
        busca: Optional[str] = None,
        competencias: Optional[List[str]] = None,
        etapa_ids: Optional[List[int]] = None,
        tipo_pagamento_ids: Optional[List[int]] = None,
        status_empenho_ids: Optional[List[int]] = None,
        contratado: Optional[str] = None,
        status_geral: Optional[str] = None,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None,
        page: int = 1,
        per_page: int = 20
    ):
        """Lista solicitações com múltiplos filtros."""
        query = cls.model.query.join(Contrato)

        # Busca textual (contratado, protocolo, código contrato, descrição)
        if busca:
            termo = f'%{busca}%'
            query = query.filter(
                or_(
                    Contrato.nomeContratado.ilike(termo),
                    cls.model.protocolo_gerado_sei.ilike(termo),
                    cls.model.codigo_contrato.ilike(termo),
                    cls.model.descricao.ilike(termo)
                )
            )

        # Multi-select competências
        if competencias:
            query = query.filter(cls.model.competencia.in_(competencias))

        # Multi-select etapas (por ID)
        if etapa_ids:
            query = query.filter(cls.model.etapa_atual_id.in_(etapa_ids))

        # Multi-select tipo de pagamento (por ID)
        if tipo_pagamento_ids:
            query = query.filter(cls.model.id_tipo_pagamento.in_(tipo_pagamento_ids))

        # Multi-select status de empenho (por ID)
        if status_empenho_ids:
            query = query.filter(cls.model.status_empenho_id.in_(status_empenho_ids))

        if contratado:
            query = query.filter(
                Contrato.nomeContratado.ilike(f'%{contratado}%')
            )

        if status_geral:
            query = query.filter(cls.model.status_geral == status_geral)

        if data_inicio:
            query = query.filter(cls.model.data_solicitacao >= data_inicio)

        if data_fim:
            query = query.filter(cls.model.data_solicitacao <= data_fim)

        return query.order_by(
            cls.model.data_solicitacao.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)

    @classmethod
    def buscar_por_protocolo(cls, protocolo: str) -> Optional[Solicitacao]:
        """Busca solicitação pelo protocolo SEI."""
        return cls.model.query.filter_by(
            protocolo_gerado_sei=protocolo
        ).first()

    @classmethod
    def listar_competencias_distintas(cls) -> List[str]:
        """Retorna lista de competências únicas."""
        result = db.session.query(
            cls.model.competencia
        ).distinct().filter(
            cls.model.competencia.isnot(None)
        ).all()
        return [c[0] for c in result if c[0]]

    @classmethod
    def contar_por_etapa(cls) -> dict:
        """Conta solicitações agrupadas por etapa."""
        result = db.session.query(
            cls.model.etapa_atual_id,
            func.count(cls.model.id)
        ).group_by(cls.model.etapa_atual_id).all()

        return {etapa_id: count for etapa_id, count in result}

    @classmethod
    def contar_por_status(cls) -> dict:
        """Conta solicitações agrupadas por status geral."""
        result = db.session.query(
            cls.model.status_geral,
            func.count(cls.model.id)
        ).group_by(cls.model.status_geral).all()

        return {status: count for status, count in result}

    @classmethod
    def atualizar_etapa(
        cls,
        solicitacao: Solicitacao,
        nova_etapa_id: int,
        usuario_id: int,
        comentario: str = None
    ) -> Solicitacao:
        """Atualiza a etapa da solicitação e registra no histórico."""
        etapa_anterior_id = solicitacao.etapa_atual_id

        # Atualiza a etapa
        solicitacao.etapa_atual_id = nova_etapa_id

        # Registra no histórico
        historico = HistoricoMovimentacao(
            id_solicitacao=solicitacao.id,
            id_etapa_anterior=etapa_anterior_id,
            id_etapa_nova=nova_etapa_id,
            id_usuario_responsavel=usuario_id,
            data_movimentacao=datetime.now(),
            comentario=comentario
        )
        db.session.add(historico)
        db.session.commit()

        return solicitacao

    @classmethod
    def listar_pendentes_ne(cls, page: int = 1, per_page: int = 50):
        """Lista solicitações pendentes de inserção de NE."""
        from app.models import SolicitacaoEmpenho

        # Solicitações que têm empenho mas sem NE preenchida
        subquery = db.session.query(
            SolicitacaoEmpenho.id_solicitacao
        ).filter(
            SolicitacaoEmpenho.ne.is_(None)
        ).subquery()

        return cls.model.query.join(
            Contrato
        ).filter(
            cls.model.id.in_(subquery)
        ).order_by(
            cls.model.data_solicitacao.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)
