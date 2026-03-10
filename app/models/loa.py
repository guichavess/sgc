"""
Modelo LOA (Lei Orçamentária Anual).
Tabela populada pelo script scripts/atualizar_loa.py via API SIAFE.
"""
from app.extensions import db


class Loa(db.Model):
    """Saldos contábeis da LOA por classificação orçamentária."""

    __tablename__ = 'loa_2026'

    # PK autoincrement (pandas to_sql gera sem PK, adicionamos para o ORM)
    row_id = db.Column('row_id', db.BigInteger, primary_key=True, autoincrement=True)

    codigoUG = db.Column(db.String(10))
    saldo = db.Column(db.Numeric(20, 2))
    classificacaoStr = db.Column(db.String(200))
    contaCorrente = db.Column(db.String(100))
    saldoAnterior = db.Column(db.Numeric(20, 2))
    valorCredito = db.Column(db.Numeric(20, 2))
    valorDebito = db.Column(db.Numeric(20, 2))
    mes = db.Column(db.Integer)
    ano = db.Column(db.Integer)
    id_conta = db.Column('id', db.String(20))
    descricao = db.Column(db.String(200))

    def __repr__(self):
        return f'<Loa UG={self.codigoUG} conta={self.id_conta} mes={self.mes}>'

    @property
    def acao(self):
        """Extrai código da ação do classificacaoStr (1º segmento antes do '.')."""
        if not self.classificacaoStr:
            return None
        partes = self.classificacaoStr.split('.')
        return partes[0] if partes else None

    @property
    def natureza_cod(self):
        """Extrai código da natureza do classificacaoStr (2º segmento)."""
        if not self.classificacaoStr:
            return None
        partes = self.classificacaoStr.split('.')
        return partes[1] if len(partes) > 1 else None

    @property
    def fonte_cod(self):
        """Extrai código da fonte do classificacaoStr (3º segmento)."""
        if not self.classificacaoStr:
            return None
        partes = self.classificacaoStr.split('.')
        return partes[2] if len(partes) > 2 else None
