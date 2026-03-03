"""
Modelo ContratoAditivo - Aditivos contratuais.

Tabela: contratos_aditivo
Vinculação ao contrato via coluna codigo_contrato.
"""
from app.extensions import db


class ContratoAditivo(db.Model):
    """Aditivo de contrato."""

    __tablename__ = 'contratos_aditivo'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=True)
    codAditivo = db.Column(db.String(10))
    numOriginal = db.Column(db.String(50))
    numProcesso = db.Column(db.String(50))
    dtVigenciaIni = db.Column(db.Date)
    dtVigenciaFim = db.Column(db.Date)
    dtPublicacao = db.Column(db.Date)
    valor = db.Column(db.Numeric(15, 2))
    dataCelebracao = db.Column(db.Date)
    objeto = db.Column(db.Text)
    data_atualizacao = db.Column(db.TIMESTAMP)

    def __repr__(self):
        return f'<ContratoAditivo {self.id} - Contrato {self.codigo_contrato}>'
