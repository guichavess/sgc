"""
Modelos do modulo Gestao do Fundo Rotativo.

Tabelas para registrar snapshots de saldo contabil do fundo rotativo.
"""
from datetime import datetime

from sqlalchemy.dialects import mysql

from app.extensions import db


USER_ID_TYPE = db.BigInteger().with_variant(mysql.BIGINT(unsigned=True), 'mysql')


class FundoRotativoSaldo(db.Model):
    """Snapshot de saldo contabil do Fundo Rotativo retornado pelo SIAFE."""

    __tablename__ = 'fundo_rotativo_saldos'
    __table_args__ = (
        db.UniqueConstraint(
            'codigoUG',
            'conta_contabil',
            'ano',
            'mes',
            'conta_corrente',
            'classificacaoStr',
            name='uq_fr_saldo_snapshot',
        ),
        db.Index('idx_fr_saldo_periodo', 'ano', 'mes'),
        db.Index('idx_fr_saldo_fonte_periodo', 'fonte_codigo', 'ano', 'mes'),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    valor = db.Column(db.Numeric(15, 2), nullable=False)
    data = db.Column(db.DateTime, nullable=False)

    codigoUG = db.Column(db.String(10), nullable=True, index=True)
    conta_contabil = db.Column(db.String(20), nullable=True, index=True)
    ano = db.Column(db.Integer, nullable=True, index=True)
    mes = db.Column(db.Integer, nullable=True, index=True)
    conta_corrente = db.Column(db.String(200), nullable=True)
    classificacaoStr = db.Column(db.String(200), nullable=True)

    saldoAnterior = db.Column(db.Numeric(15, 2), nullable=True)
    valorCredito = db.Column(db.Numeric(15, 2), nullable=True)
    valorDebito = db.Column(db.Numeric(15, 2), nullable=True)

    fonte_codigo = db.Column(db.String(10), nullable=True, index=True)
    fonte_formatada = db.Column(db.String(20), nullable=True)
    natureza = db.Column(db.String(20), nullable=True, index=True)
    id_exercicio = db.Column(db.String(2), nullable=False, index=True)
    identificador_exercicio_fonte = db.Column(db.String(20), nullable=True)
    marcador_fonte = db.Column(db.String(50), nullable=True)
    detalhamento_fonte = db.Column(db.String(80), nullable=True)
    tipo_detalhamento_fonte = db.Column(db.String(20), nullable=True)

    domicilio_bancario = db.Column(db.String(50), nullable=True)
    banco = db.Column(db.String(10), nullable=True)
    agencia = db.Column(db.String(20), nullable=True)
    conta_bancaria = db.Column(db.String(30), nullable=True)
    classificacao_json = db.Column(db.Text, nullable=True)

    criado_por = db.Column(USER_ID_TYPE, db.ForeignKey('sis_usuarios.id'), nullable=True)
    sincronizado_por = db.Column(USER_ID_TYPE, db.ForeignKey('sis_usuarios.id'), nullable=True)
    data_sincronizacao = db.Column(db.DateTime, nullable=True)
    data_criacao = db.Column(db.DateTime, default=datetime.utcnow)
    data_atualizacao = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    fonte = db.relationship(
        'ClassFonte',
        primaryjoin='foreign(FundoRotativoSaldo.fonte_codigo) == ClassFonte.codigo',
        viewonly=True,
    )

    @property
    def exercicio_titulo(self):
        from app.constants import FUNDO_ROTATIVO_EXERCICIOS_MAP
        return FUNDO_ROTATIVO_EXERCICIOS_MAP.get(self.id_exercicio, '')

    @property
    def exercicio_label(self):
        titulo = self.exercicio_titulo
        return f'{self.id_exercicio} - {titulo}' if titulo else self.id_exercicio

    @property
    def fonte_label(self):
        if self.fonte:
            return f'{self.fonte.codigo} - {self.fonte.descricao}'
        if self.fonte_formatada and self.fonte_codigo:
            return f'{self.fonte_codigo} ({self.fonte_formatada})'
        return self.fonte_codigo or ''

    @property
    def mes_ano_label(self):
        return f'{self.mes:02d}/{self.ano}' if self.ano and self.mes else ''

    def __repr__(self):
        return (
            f'<FundoRotativoSaldo id={self.id} valor={self.valor} '
            f'fonte={self.fonte_codigo} ano={self.ano} mes={self.mes}>'
        )
