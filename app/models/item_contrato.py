"""
Modelo de ItemContrato - Itens/produtos associados a contratos.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db


class ItemContrato(db.Model):
    """Modelo para itens de contratos."""

    __tablename__ = 'itens_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    descricao = db.Column(db.String(255), nullable=False)
    tipo_item = db.Column(db.String(100))
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    editado_por = db.Column(db.String(255), nullable=True)

    # Colunas de DE-PARA (preenchidas pela equipe externa)
    # Mapeia item_contrato → serviço CATSERV OU item CATMAT
    catserv_servico_id = db.Column(
        db.Integer,
        db.ForeignKey('catserv_servicos.codigo_servico', ondelete='SET NULL'),
        nullable=True
    )
    catmat_item_id = db.Column(db.Integer, nullable=True)  # Sem FK real (catmat é cópia local)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='itens_contrato', lazy=True)
    servico_catserv = db.relationship('CatservServico', lazy=True)

    def __repr__(self):
        return f'<ItemContrato {self.descricao}>'
