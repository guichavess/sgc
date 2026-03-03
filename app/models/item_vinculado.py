"""
Modelo de Item Vinculado ao Contrato.
Tabela de junção polimórfica que vincula contratos a itens
de serviço (CATSERV) ou material (CATMAT).
"""
from datetime import datetime
from app.extensions import db


class ItemVinculado(db.Model):
    """Vinculação de um item (serviço ou material) a um contrato."""

    __tablename__ = 'itens_vinculados'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    codigo_contrato = db.Column(
        db.String(20),
        db.ForeignKey('contratos.codigo', ondelete='CASCADE'),
        nullable=False,
        index=True
    )

    # Tipo do item: 'S' = Serviço (CATSERV), 'M' = Material (CATMAT)
    tipo = db.Column(db.String(1), nullable=False)

    # FK para catserv_servicos (quando tipo='S')
    catserv_servico_id = db.Column(
        db.Integer,
        db.ForeignKey('catserv_servicos.codigo_servico', ondelete='CASCADE'),
        nullable=True
    )

    # Referência para catmat_itens (quando tipo='M')
    # Sem FK real pois catmat é cópia local sem constraints
    catmat_item_id = db.Column(db.Integer, nullable=True)

    # Referência para itens_contrato (item que a usuária conhece)
    item_contrato_id = db.Column(
        db.Integer,
        db.ForeignKey('itens_contrato.id', ondelete='SET NULL'),
        nullable=True
    )

    # Auditoria
    data_vinculacao = db.Column(db.DateTime, nullable=False, default=datetime.now)
    vinculado_por = db.Column(
        db.BigInteger,
        db.ForeignKey('sis_usuarios.id', ondelete='SET NULL'),
        nullable=True
    )

    # Relationships
    contrato = db.relationship(
        'Contrato',
        backref=db.backref('itens_vinculados', lazy='dynamic', cascade='all, delete-orphan')
    )
    servico = db.relationship(
        'CatservServico',
        foreign_keys=[catserv_servico_id],
        lazy=True
    )
    usuario = db.relationship(
        'Usuario',
        foreign_keys=[vinculado_por],
        lazy=True
    )
    item_contrato = db.relationship(
        'ItemContrato',
        lazy=True
    )

    __table_args__ = (
        db.UniqueConstraint(
            'codigo_contrato', 'tipo', 'catserv_servico_id', 'catmat_item_id',
            name='uk_contrato_item'
        ),
        db.Index('idx_iv_contrato_tipo', 'codigo_contrato', 'tipo'),
        db.Index('idx_iv_catserv', 'catserv_servico_id'),
        db.Index('idx_iv_catmat', 'catmat_item_id'),
    )

    def __repr__(self):
        item_ref = self.catserv_servico_id if self.tipo == 'S' else self.catmat_item_id
        return f'<ItemVinculado {self.codigo_contrato}:{self.tipo}:{item_ref}>'
