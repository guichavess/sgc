"""
Repositório para ItemContrato.
"""
from app.repositories.base import BaseRepository
from app.models.item_contrato import ItemContrato
from app.models.usuario import Usuario
from app.extensions import db


class ItemContratoRepository(BaseRepository[ItemContrato]):
    model = ItemContrato

    @classmethod
    def listar_com_criador(cls):
        """Lista todos os itens com o nome do usuário criador."""
        return (
            db.session.query(
                ItemContrato,
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Usuario, Usuario.id == ItemContrato.usuario_id)
            .all()
        )

    @classmethod
    def listar_com_criador_paginado(cls, page=1, per_page=20):
        """Lista itens paginados com o nome do usuário criador."""
        return (
            db.session.query(
                ItemContrato,
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Usuario, Usuario.id == ItemContrato.usuario_id)
            .order_by(ItemContrato.id.desc())
            .paginate(page=page, per_page=per_page, error_out=False)
        )
