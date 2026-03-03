"""
Modelo ClassFonte - Classificador de Fonte de Recurso.

Tabela populada a partir do JSON do SIAFE (endpoint de Fontes).
Cada registro = 1 fonte distinta com código e descrição.
"""
from app.extensions import db


class ClassFonte(db.Model):
    """Classificador de Fonte de Recurso."""

    __tablename__ = 'class_fonte'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.String(10), nullable=False, unique=True)
    descricao = db.Column(db.Text, nullable=False)
    createdAt = db.Column(db.DateTime)

    def __repr__(self):
        return f'<ClassFonte {self.codigo} - {self.descricao}>'
