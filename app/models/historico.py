"""
Modelo de Histórico de Movimentações.
"""
from datetime import datetime
from app.extensions import db


class HistoricoMovimentacao(db.Model):
    """Modelo para histórico de movimentações das solicitações."""

    __tablename__ = 'sis_historico_movimentacoes'

    id = db.Column(db.BigInteger, primary_key=True)
    id_solicitacao = db.Column(db.BigInteger, db.ForeignKey('sis_solicitacoes.id'), nullable=False)
    id_etapa_anterior = db.Column(db.Integer)
    id_etapa_nova = db.Column(db.Integer, db.ForeignKey('sis_etapas_fluxo.id'), nullable=False)
    id_usuario_responsavel = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))
    data_movimentacao = db.Column(db.DateTime, default=datetime.now)
    comentario = db.Column(db.Text)

    # Relacionamentos
    etapa_nova = db.relationship('Etapa', foreign_keys=[id_etapa_nova])
    usuario = db.relationship('Usuario')

    def __repr__(self):
        return f'<HistoricoMovimentacao {self.id} - Etapa {self.id_etapa_nova}>'
