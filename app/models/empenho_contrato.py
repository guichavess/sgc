"""
Modelo de EmpenhoContrato - Empenhos associados a contratos.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db
from datetime import datetime


class EmpenhoContrato(db.Model):
    """Modelo para empenhos de contratos."""

    __tablename__ = 'empenhos_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    codigo = db.Column(db.String(50), nullable=False)
    observacao = db.Column(db.Text, nullable=True)
    data_emissao = db.Column(db.Date, nullable=True)
    valor = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='empenhos_contrato', lazy=True)

    def __repr__(self):
        return f'<EmpenhoContrato {self.codigo}>'

    @property
    def valor_formatado(self):
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def data_emissao_fmt(self):
        if self.data_emissao is None:
            return '—'
        return self.data_emissao.strftime('%d/%m/%Y')


class LiquidacaoContrato(db.Model):
    """Modelo para liquidações de contratos."""

    __tablename__ = 'liquidacoes_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    codigo = db.Column(db.String(50), nullable=False)
    observacao = db.Column(db.Text, nullable=True)
    data_emissao = db.Column(db.Date, nullable=True)
    valor = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='liquidacoes_contrato', lazy=True)

    def __repr__(self):
        return f'<LiquidacaoContrato {self.codigo}>'

    @property
    def valor_formatado(self):
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def data_emissao_fmt(self):
        if self.data_emissao is None:
            return '—'
        return self.data_emissao.strftime('%d/%m/%Y')


class PagamentoContrato(db.Model):
    """Modelo para pagamentos de contratos."""

    __tablename__ = 'pagamentos_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    codigo = db.Column(db.String(50), nullable=False)
    observacao = db.Column(db.Text, nullable=True)
    data_emissao = db.Column(db.Date, nullable=True)
    valor = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    usuario_id = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='pagamentos_contrato', lazy=True)

    def __repr__(self):
        return f'<PagamentoContrato {self.codigo}>'

    @property
    def valor_formatado(self):
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def data_emissao_fmt(self):
        if self.data_emissao is None:
            return '—'
        return self.data_emissao.strftime('%d/%m/%Y')
