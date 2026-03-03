"""
Modelo de SaldoContrato - Saldo global e histórico de movimentações de contratos.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db
from datetime import datetime


class SaldoContrato(db.Model):
    """Modelo para saldo de contratos."""

    __tablename__ = 'saldo_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    saldo_global = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    data_inicio = db.Column(db.Date, nullable=True)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.utcnow)
    updated_at = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='saldos_contrato', lazy=True)
    movimentacoes = db.relationship('MovimentacaoSaldo', backref='saldo', lazy=True,
                                     order_by='MovimentacaoSaldo.created_at.desc()')
    itens_saldo = db.relationship('SaldoContratoItem', backref='saldo_contrato', lazy=True)

    def __repr__(self):
        return f'<SaldoContrato {self.codigo_contrato} - R$ {self.saldo_global}>'

    @property
    def saldo_formatado(self):
        """Retorna o saldo formatado em moeda brasileira."""
        if self.saldo_global is None:
            return 'R$ 0,00'
        return f'R$ {self.saldo_global:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


class SaldoContratoItem(db.Model):
    """Divisão do saldo global por item vinculado ao contrato."""

    __tablename__ = 'saldo_contrato_item'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    saldo_contrato_id = db.Column(db.Integer, db.ForeignKey('saldo_contrato.id'), nullable=False)
    item_vinculado_id = db.Column(db.Integer, db.ForeignKey('itens_vinculados.id'), nullable=False)
    valor = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    updated_at = db.Column(db.TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relacionamentos
    item_vinculado = db.relationship('ItemVinculado', lazy=True)

    __table_args__ = (
        db.UniqueConstraint('saldo_contrato_id', 'item_vinculado_id', name='uq_saldo_item'),
    )

    def __repr__(self):
        return f'<SaldoContratoItem saldo={self.saldo_contrato_id} item={self.item_vinculado_id} R$ {self.valor}>'

    @property
    def valor_formatado(self):
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


class MovimentacaoSaldo(db.Model):
    """Modelo para histórico de movimentações do saldo."""

    __tablename__ = 'movimentacao_saldo'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    saldo_contrato_id = db.Column(db.Integer, db.ForeignKey('saldo_contrato.id'), nullable=False)
    tipo = db.Column(db.String(20), nullable=False)  # 'CREDITO' ou 'DEBITO'
    valor = db.Column(db.Numeric(15, 2), nullable=False)
    descricao = db.Column(db.String(255), nullable=True)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='movimentacoes_saldo', lazy=True)

    def __repr__(self):
        return f'<MovimentacaoSaldo {self.tipo} R$ {self.valor}>'

    @property
    def valor_formatado(self):
        """Retorna o valor formatado em moeda brasileira."""
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
