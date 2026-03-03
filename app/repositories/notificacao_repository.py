"""
Repositorio de Notificacoes - Queries especializadas.
"""
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import and_, desc

from app.extensions import db
from app.models.notificacao import (
    Notificacao, NotificacaoTipo,
    NotificacaoCriticaConfirmacao, NotificacaoPreferencia
)
from app.repositories.base import BaseRepository


class NotificacaoRepository(BaseRepository):
    """Repositorio para operacoes com notificacoes."""

    model = Notificacao

    @classmethod
    def contar_nao_lidas(cls, usuario_id: int) -> int:
        """Conta notificacoes nao lidas do usuario."""
        return Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.lida == False,
            Notificacao.descartada == False,
        ).count()

    @classmethod
    def contar_criticas_pendentes(cls, usuario_id: int) -> int:
        """Conta notificacoes criticas nao descartadas."""
        return Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.nivel == 'critica',
            Notificacao.descartada == False,
        ).count()

    @classmethod
    def listar_por_usuario(
        cls,
        usuario_id: int,
        page: int = 1,
        per_page: int = 20,
        apenas_nao_lidas: bool = False,
        modulo: Optional[str] = None,
        nivel: Optional[str] = None,
    ):
        """Lista notificacoes do usuario com filtros e paginacao."""
        query = Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.descartada == False,
        )

        if apenas_nao_lidas:
            query = query.filter(Notificacao.lida == False)

        if modulo:
            query = query.filter(Notificacao.ref_modulo == modulo)

        if nivel:
            query = query.filter(Notificacao.nivel == nivel)

        return query.order_by(desc(Notificacao.created_at)).paginate(
            page=page, per_page=per_page, error_out=False
        )

    @classmethod
    def listar_recentes(cls, usuario_id: int, limite: int = 10) -> List[Notificacao]:
        """Retorna as N notificacoes mais recentes (para o dropdown do sino)."""
        return Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.descartada == False,
        ).order_by(desc(Notificacao.created_at)).limit(limite).all()

    @classmethod
    def buscar_criticas_pendentes(cls, usuario_id: int) -> List[Notificacao]:
        """Retorna notificacoes criticas nao descartadas do usuario."""
        return Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.nivel == 'critica',
            Notificacao.descartada == False,
        ).order_by(desc(Notificacao.created_at)).all()

    @classmethod
    def marcar_como_lida(cls, notificacao_id: int, usuario_id: int) -> bool:
        """Marca uma notificacao como lida."""
        notif = Notificacao.query.filter_by(
            id=notificacao_id, usuario_id=usuario_id
        ).first()
        if not notif:
            return False
        notif.lida = True
        notif.lida_em = datetime.now()
        db.session.commit()
        return True

    @classmethod
    def marcar_todas_lidas(cls, usuario_id: int) -> int:
        """Marca todas como lidas. Retorna quantidade atualizada."""
        agora = datetime.now()
        count = Notificacao.query.filter(
            Notificacao.usuario_id == usuario_id,
            Notificacao.lida == False,
            Notificacao.descartada == False,
            Notificacao.nivel != 'critica',
        ).update({
            Notificacao.lida: True,
            Notificacao.lida_em: agora,
        }, synchronize_session=False)
        db.session.commit()
        return count

    @classmethod
    def confirmar_critica(
        cls,
        notificacao_id: int,
        usuario_id: int,
        cpf: str
    ) -> bool:
        """Confirma notificacao critica via CPF. Retorna True se confirmada."""
        notif = Notificacao.query.filter_by(
            id=notificacao_id,
            usuario_id=usuario_id,
            nivel='critica',
            descartada=False,
        ).first()
        if not notif:
            return False

        confirmacao = NotificacaoCriticaConfirmacao(
            notificacao_id=notificacao_id,
            usuario_id=usuario_id,
            cpf_informado=cpf,
            confirmada_em=datetime.now(),
        )
        db.session.add(confirmacao)

        notif.descartada = True
        notif.descartada_em = datetime.now()
        notif.lida = True
        notif.lida_em = datetime.now()
        db.session.commit()
        return True

    @classmethod
    def existe_recente(
        cls,
        tipo_codigo: str,
        usuario_id: int,
        ref_id: str,
        dias: int = 10
    ) -> bool:
        """Verifica se ja existe notificacao recente para deduplicacao."""
        data_limite = datetime.now() - timedelta(days=dias)
        return Notificacao.query.join(NotificacaoTipo).filter(
            NotificacaoTipo.codigo == tipo_codigo,
            Notificacao.usuario_id == usuario_id,
            Notificacao.ref_id == ref_id,
            Notificacao.created_at >= data_limite,
        ).first() is not None

    @classmethod
    def criar_notificacao(
        cls,
        tipo: NotificacaoTipo,
        usuario_id: int,
        titulo: str,
        mensagem: str,
        nivel: Optional[str] = None,
        ref_modulo: Optional[str] = None,
        ref_id: Optional[str] = None,
        ref_url: Optional[str] = None,
    ) -> Notificacao:
        """Cria uma nova notificacao."""
        notif = Notificacao(
            tipo_id=tipo.id,
            usuario_id=usuario_id,
            titulo=titulo,
            mensagem=mensagem,
            nivel=nivel or tipo.nivel,
            ref_modulo=ref_modulo or tipo.modulo,
            ref_id=ref_id,
            ref_url=ref_url,
        )
        db.session.add(notif)
        return notif

    @classmethod
    def limpar_expiradas(cls, dias: int = 90) -> int:
        """Remove notificacoes lidas mais antigas que N dias."""
        data_limite = datetime.now() - timedelta(days=dias)
        count = Notificacao.query.filter(
            Notificacao.lida == True,
            Notificacao.created_at < data_limite,
        ).delete(synchronize_session=False)
        db.session.commit()
        return count

    @classmethod
    def obter_preferencia(
        cls,
        usuario_id: int,
        tipo_id: int
    ) -> Optional[NotificacaoPreferencia]:
        """Retorna preferencia do usuario para um tipo de notificacao."""
        return NotificacaoPreferencia.query.filter_by(
            usuario_id=usuario_id, tipo_id=tipo_id
        ).first()
