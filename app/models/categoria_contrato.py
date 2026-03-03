"""
Modelo de CategoriaContrato - Categorias para classificação de contratos.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db


class CategoriaContrato(db.Model):
    """Modelo para categorias de contratos."""

    __tablename__ = 'categoria_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    descricao = db.Column(db.String(255), nullable=False)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    editado_por = db.Column(db.String(255), nullable=True)

    # Relacionamento com o usuário do sistema
    usuario = db.relationship('Usuario', backref='categorias_contrato', lazy=True)

    def __repr__(self):
        return f'<CategoriaContrato {self.descricao}>'
