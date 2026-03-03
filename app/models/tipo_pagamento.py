"""
Modelo de Tipo de Pagamento.
"""
from app.extensions import db


class TipoPagamento(db.Model):
    """Modelo para tipos de pagamento (DEA, Indenizatório, Regular)."""

    __tablename__ = 'tipo_pagamento'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f'<TipoPagamento {self.nome}>'
