"""
Modelos para Fornecedores sem Contrato e vínculo de contratos.
"""
from datetime import datetime
from app.extensions import db


class FornecedorSemContrato(db.Model):
    """Fornecedor cadastrado sem contrato vinculado inicialmente."""

    __tablename__ = 'fornecedores_sem_contrato'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    descricao = db.Column(db.String(255), nullable=False)
    cnpj = db.Column(db.String(18), nullable=False)
    telefone = db.Column(db.String(20))
    data_criacao = db.Column(db.DateTime, default=datetime.now)
    criado_por = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))

    # Relacionamentos
    contratos = db.relationship('FornecedorContrato', backref='fornecedor',
                                lazy=True, cascade='all, delete-orphan')
    execucoes = db.relationship('ExecucaoOrcamentaria', backref='fornecedor', lazy=True)
    criador = db.relationship('Usuario', foreign_keys=[criado_por], lazy=True)

    def __repr__(self):
        return f'<FornecedorSemContrato {self.id} - {self.cnpj}>'

    @property
    def cnpj_formatado(self):
        """Retorna CNPJ formatado XX.XXX.XXX/XXXX-XX."""
        digitos = ''.join(c for c in (self.cnpj or '') if c.isdigit())
        if len(digitos) == 14:
            return f'{digitos[:2]}.{digitos[2:5]}.{digitos[5:8]}/{digitos[8:12]}-{digitos[12:14]}'
        return self.cnpj

    @property
    def qtd_contratos(self):
        return len(self.contratos)


class FornecedorContrato(db.Model):
    """Vínculo entre fornecedor e código de contrato."""

    __tablename__ = 'fornecedores_contratos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fornecedor_id = db.Column(db.Integer, db.ForeignKey('fornecedores_sem_contrato.id'), nullable=False)
    cod_contrato = db.Column(db.String(20), nullable=False)
    data_vinculacao = db.Column(db.DateTime, default=datetime.now)
    vinculado_por = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))

    vinculador = db.relationship('Usuario', foreign_keys=[vinculado_por], lazy=True)

    def __repr__(self):
        return f'<FornecedorContrato forn={self.fornecedor_id} contrato={self.cod_contrato}>'
