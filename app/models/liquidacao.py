"""
Modelo de Liquidação (dados do SIAFE).
"""
from app.extensions import db


class Liquidacao(db.Model):
    """Modelo para dados de liquidações do SIAFE."""

    __tablename__ = 'liquidacao'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo = db.Column(db.Text)
    codProcesso = db.Column(db.Text)
    dataProcesso = db.Column(db.DateTime)
    assuntoProcesso = db.Column(db.Text)
    resumoProcesso = db.Column(db.Text)
    anoProcesso = db.Column(db.BigInteger)
    statusDocumento = db.Column(db.Text)
    codigoUG = db.Column(db.Text)
    codFonte = db.Column(db.BigInteger)
    codNatureza = db.Column(db.BigInteger)
    codigoCredor = db.Column(db.Text)
    nomeCredor = db.Column(db.Text)
    dataEmissao = db.Column(db.DateTime)
    dataCancelamento = db.Column(db.DateTime)
    dataContabilizacao = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    observacao = db.Column(db.Text)
    codigoEmpenhoVinculado = db.Column(db.Text)
    exercicioNE = db.Column(db.BigInteger)
    codigoEL = db.Column(db.Text)
    exercicioEL = db.Column(db.BigInteger)
    codClassificacao = db.Column(db.Text)
    tipoAlteracao = db.Column(db.Text)
    codContrato = db.Column(db.BigInteger)

    def __repr__(self):
        return f'<Liquidacao {self.codigo}>'
