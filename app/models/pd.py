"""
Modelo PD - Programacao de Desembolso (dados do SIAFE).

Tabela: pd
Populada pelo script scripts/atualizar_pd.py via API SIAFE.
"""
from app.extensions import db


class PD(db.Model):
    """Modelo para dados de Programacao de Desembolso do SIAFE."""

    __tablename__ = 'pd'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo = db.Column(db.Text)
    codProcesso = db.Column(db.Text)
    statusDocumento = db.Column(db.Text)
    codigoUG = db.Column(db.Text)
    codigoGestao = db.Column(db.BigInteger)
    codFonte = db.Column(db.BigInteger)
    codNatureza = db.Column(db.BigInteger)
    codigoCredor = db.Column(db.BigInteger)
    nomeCredor = db.Column(db.Text)
    dataEmissao = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    observacao = db.Column(db.Text)
    codClassificacao = db.Column(db.Text)
    statusExecucao = db.Column(db.Text)
    codUgPagadora = db.Column(db.BigInteger)
    codigoOB = db.Column(db.Text)
    codigoNE = db.Column(db.Text)
    codigoNL = db.Column(db.Text)
    valorTotalPD = db.Column(db.Float)
    codContrato = db.Column(db.BigInteger)

    def __repr__(self):
        return f'<PD {self.codigo}>'
