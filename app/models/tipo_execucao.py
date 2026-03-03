"""
Modelo de Tipo de Execução.
"""
from app.extensions import db


class TipoExecucao(db.Model):
    """Tabela com tipos de execução: CONTINUADO / NÃO CONTINUADO."""

    __tablename__ = 'tipoexecucao'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    descricao = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f'<TipoExecucao {self.id} - {self.descricao}>'
