"""
Modelos de Etapa e Status.
"""
from app.extensions import db


class Etapa(db.Model):
    """Modelo para etapas do fluxo de pagamento."""

    __tablename__ = 'sis_etapas_fluxo'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)
    alias = db.Column(db.String(50), nullable=False)
    ordem = db.Column(db.Integer, nullable=False)
    cor_hex = db.Column(db.String(10))

    def __repr__(self):
        return f'<Etapa {self.nome}>'


class StatusEmpenho(db.Model):
    """Modelo para status do empenho."""

    __tablename__ = 'sis_status_empenho'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(50), nullable=False)
    cor_badge = db.Column(db.String(20), default='secondary')

    def __repr__(self):
        return f'<StatusEmpenho {self.nome}>'
