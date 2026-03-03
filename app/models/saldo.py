"""
Modelo de Saldo de Empenho.
"""
from datetime import datetime
from app.extensions import db


class SaldoEmpenho(db.Model):
    """Modelo para registro de saldo de empenho por contrato."""

    __tablename__ = 'sis_saldo_empenho_contrato'

    id = db.Column(db.Integer, primary_key=True)
    saldo = db.Column(db.Numeric(20, 2), nullable=False)
    data = db.Column(db.DateTime, default=datetime.now)
    cod_contrato = db.Column(db.String(20), nullable=False)
    competencia = db.Column(db.String(25), nullable=False)

    def __repr__(self):
        return f'<SaldoEmpenho {self.cod_contrato} - {self.competencia}>'

    @classmethod
    def get_saldo_atual(cls, cod_contrato, competencia):
        """Busca o saldo mais recente para o contrato e competência."""
        return cls.query.filter_by(
            cod_contrato=cod_contrato,
            competencia=competencia
        ).order_by(cls.data.desc()).first()
