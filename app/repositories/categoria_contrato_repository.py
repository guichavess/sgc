"""
Repositório para CategoriaContrato.
"""
from app.repositories.base import BaseRepository
from app.models.categoria_contrato import CategoriaContrato
from app.models.usuario import Usuario
from app.extensions import db


class CategoriaContratoRepository(BaseRepository[CategoriaContrato]):
    model = CategoriaContrato

    @classmethod
    def listar_com_criador_paginado(cls, page=1, per_page=20):
        """Lista categorias paginadas com o nome do usuário criador."""
        return (
            db.session.query(
                CategoriaContrato,
                Usuario.nome.label('nome_criador')
            )
            .outerjoin(Usuario, Usuario.id == CategoriaContrato.usuario_id)
            .order_by(CategoriaContrato.id.desc())
            .paginate(page=page, per_page=per_page, error_out=False)
        )
