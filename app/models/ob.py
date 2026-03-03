"""
Modelo de OB - Ordem Bancária (Pagamento) (dados do SIAFE).
"""
from app.extensions import db


class OB(db.Model):
    """Modelo para dados de ordens bancárias (pagamentos) do SIAFE."""

    __tablename__ = 'ob'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo = db.Column(db.Text)
    codProcesso = db.Column(db.Text)
    assuntoProcesso = db.Column(db.Text)
    anoProcesso = db.Column(db.BigInteger)
    statusDocumento = db.Column(db.Text)
    codigoUG = db.Column(db.Text)
    nomeUG = db.Column(db.Text)
    codigoGestao = db.Column(db.BigInteger)
    codFonte = db.Column(db.BigInteger)
    codNatureza = db.Column(db.BigInteger)
    codigoCredor = db.Column(db.Text)
    nomeCredor = db.Column(db.Text)
    dataEmissao = db.Column(db.DateTime)
    dataContabilizacao = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    observacao = db.Column(db.Text)
    competencia = db.Column(db.Text)
    objectType = db.Column(db.Text)
    envioStatus = db.Column(db.Text)
    codigoRegistroEnvio = db.Column(db.Text)
    ugEmitente = db.Column(db.BigInteger)
    dataPagamento = db.Column(db.DateTime)
    codigoUGPagadora = db.Column(db.BigInteger)
    codigoUGEmpenho = db.Column(db.BigInteger)
    tipoOB = db.Column(db.Text)
    idNE = db.Column(db.BigInteger)
    codigoNE = db.Column(db.Text)
    exercicioNE = db.Column(db.BigInteger)
    idNL = db.Column(db.BigInteger)
    codigoNL = db.Column(db.Text)
    exercicioNL = db.Column(db.BigInteger)
    codigoPDO = db.Column(db.Text)
    codClassificacao = db.Column(db.Text)
    codContrato = db.Column(db.Text)

    def __repr__(self):
        return f'<OB {self.codigo}>'
