"""
Modelo para Planejamento Orçamentário.
"""
from datetime import datetime
from app.extensions import db


class PlanejamentoOrcamentario(db.Model):
    """Planejamento orçamentário por contrato e competência."""

    __tablename__ = 'planejamento_orcamentario'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cod_contrato = db.Column(db.String(20), nullable=False)
    competencia = db.Column(db.String(7), nullable=False)  # MM/YYYY
    valor = db.Column(db.Numeric(15, 2))
    dt_lancamento = db.Column(db.DateTime, default=datetime.now)
    usuario = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))
    planejamento_inicial = db.Column(db.Boolean, default=False)
    repactuacao_prorrogacao = db.Column(db.Boolean, default=False)

    usuario_rel = db.relationship('Usuario', foreign_keys=[usuario], lazy=True)

    def __repr__(self):
        return f'<PlanejamentoOrcamentario {self.id} - {self.cod_contrato}>'
