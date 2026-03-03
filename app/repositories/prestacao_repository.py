"""
Repositório para Prestacao (Execuções).
"""
from app.repositories.base import BaseRepository
from app.models.prestacao import Prestacao
from app.models.contrato import Contrato
from app.models.usuario import Usuario
from app.extensions import db


class PrestacaoRepository(BaseRepository[Prestacao]):
    model = Prestacao

    @classmethod
    def listar_com_detalhes(cls):
        """Lista todas as execuções com contrato e nome do criador."""
        return (
            db.session.query(
                Prestacao,
                Contrato.objeto.label('contrato_objeto'),
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Contrato, Contrato.codigo == Prestacao.codigo_contrato)
            .outerjoin(Usuario, Usuario.id == Prestacao.usuario_id)
            .order_by(Prestacao.data.desc())
            .all()
        )

    @classmethod
    def listar_com_detalhes_paginado(cls, page=1, per_page=20):
        """Lista execuções paginadas com contrato e nome do criador."""
        return (
            db.session.query(
                Prestacao,
                Contrato.objeto.label('contrato_objeto'),
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Contrato, Contrato.codigo == Prestacao.codigo_contrato)
            .outerjoin(Usuario, Usuario.id == Prestacao.usuario_id)
            .order_by(Prestacao.data.desc())
            .paginate(page=page, per_page=per_page, error_out=False)
        )
