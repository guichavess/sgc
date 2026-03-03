"""
Modelo de FiscalContrato - Fiscais associados a contratos.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db


class FiscalContrato(db.Model):
    """Modelo para fiscais de contratos."""

    __tablename__ = 'fiscais_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'))
    tipo = db.Column(db.String(20))
    nome = db.Column(db.String(100))
    cpf = db.Column(db.String(11))
    telefone = db.Column(db.String(20))
    email = db.Column(db.String(100))
    registroProfissional = db.Column(db.String(50))
    data_atualizacao = db.Column(db.TIMESTAMP)

    def __repr__(self):
        return f'<FiscalContrato {self.nome} - {self.codigo_contrato}>'
