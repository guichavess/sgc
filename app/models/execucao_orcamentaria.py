"""
Modelo para Execuções Orçamentárias.
"""
from datetime import datetime
from app.extensions import db


class ExecucaoOrcamentaria(db.Model):
    """Execução orçamentária vinculada a fornecedor."""

    __tablename__ = 'execucoes_orcamentarias'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    fornecedor_id = db.Column(db.Integer, db.ForeignKey('fornecedores_sem_contrato.id'), nullable=False)
    descricao = db.Column(db.Text, nullable=False)
    item = db.Column(db.String(255))
    quantidade = db.Column(db.Numeric(15, 2))
    valor = db.Column(db.Numeric(15, 2))
    competencia = db.Column(db.String(7))  # MM/YYYY
    cod_contrato = db.Column(db.String(20))  # NULL até vincular
    acao = db.Column(db.String(50))
    natureza = db.Column(db.String(50))
    fonte = db.Column(db.String(50))
    data_criacao = db.Column(db.DateTime, default=datetime.now)
    criado_por = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))

    criador = db.relationship('Usuario', foreign_keys=[criado_por], lazy=True)

    def __repr__(self):
        return f'<ExecucaoOrcamentaria {self.id}>'

    @property
    def vinculada(self):
        """Retorna True se a execução está vinculada a um contrato."""
        return self.cod_contrato is not None and self.cod_contrato.strip() != ''
