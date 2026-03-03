"""
Modelo de Centro de Custo.
"""
from app.extensions import db


class CentroDeCusto(db.Model):
    """Tabela de lookup para centros de custo."""

    __tablename__ = 'centrodecusto'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    descricao = db.Column(db.String(255), nullable=False)

    def __repr__(self):
        return f'<CentroDeCusto {self.id} - {self.descricao}>'
