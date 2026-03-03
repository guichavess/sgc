"""
Modelo de Empenho (dados do SIAFE).
"""
from app.extensions import db


class Empenho(db.Model):
    """Modelo para dados de empenhos do SIAFE."""

    __tablename__ = 'empenho'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo = db.Column(db.Text)
    codProcesso = db.Column(db.Text)
    dataProcesso = db.Column(db.DateTime)
    assuntoProcesso = db.Column(db.Text)
    anoProcesso = db.Column(db.BigInteger)
    statusDocumento = db.Column(db.Text)
    codigoUG = db.Column(db.Text)
    nomeUG = db.Column(db.Text)
    codFonte = db.Column(db.BigInteger)
    codNatureza = db.Column(db.BigInteger)
    codigoCredor = db.Column(db.Text)
    nomeCredor = db.Column(db.Text)
    dataEmissao = db.Column(db.DateTime)
    dataCancelamento = db.Column(db.DateTime)
    dataContabilizacao = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    vlr = db.Column(db.Float)
    observacao = db.Column(db.Text)
    cnpjCredor = db.Column(db.BigInteger)
    idNR = db.Column(db.BigInteger)
    codNR = db.Column(db.Text)
    modalidade = db.Column(db.Text)
    tipoAlteracaoNE = db.Column(db.Text)
    codContrato = db.Column(db.BigInteger)
    codAcao = db.Column(db.BigInteger)
    codDetalhamentoFonte = db.Column(db.BigInteger)
    codigoOrgao = db.Column(db.BigInteger)
    codigoModalidadeLicitacao = db.Column(db.BigInteger)
    descModalidadeLicitacao = db.Column(db.Text)
    codClassificacao = db.Column(db.Text)

    def __repr__(self):
        return f'<Empenho {self.codigo}>'
