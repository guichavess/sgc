"""
Modelo Reserva (Nota de Reserva).
Tabela populada pelo script scripts/atualizar_reserva.py via API SIAFE.
"""
from app.extensions import db


class Reserva(db.Model):
    """Notas de Reserva orçamentária."""

    __tablename__ = 'reserva'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo = db.Column(db.String(50))
    codigoDocAlterado = db.Column(db.String(50))
    codProcesso = db.Column(db.String(50))
    dataProcesso = db.Column(db.DateTime)
    assuntoProcesso = db.Column(db.Text)
    resumoProcesso = db.Column(db.Text)
    anoProcesso = db.Column(db.Integer)
    statusDocumento = db.Column(db.String(50))
    codigoUG = db.Column(db.String(10))
    ordenadoresDespesa = db.Column(db.Text)
    codFonte = db.Column(db.Integer)
    codNatureza = db.Column(db.Integer)
    codClassificacao = db.Column(db.String(200))
    codigoCredor = db.Column(db.String(20))
    nomeCredor = db.Column(db.String(255))
    valor = db.Column(db.Numeric(20, 2))
    observacao = db.Column(db.Text)
    tipoAlteracao = db.Column(db.String(50))
    dataEmissao = db.Column(db.DateTime)
    codigoEmpenhoVinculado = db.Column(db.Integer)
    tipoReserva = db.Column(db.String(50))
    codContrato = db.Column(db.String(50))

    def __repr__(self):
        return f'<Reserva {self.codigo}>'
