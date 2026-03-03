"""
Modelo de NatDespesa - Naturezas de Despesa.
Usada pelo módulo de Prestações de Contratos.
"""
from app.extensions import db


class NatDespesa(db.Model):
    """Modelo para naturezas de despesa."""

    __tablename__ = 'natdespesas'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    codigo = db.Column(db.Integer)
    titulo = db.Column(db.Text)
    codigoCategoria = db.Column(db.Integer)
    NomeCategoria = db.Column(db.Text)
    codigoGrupoDespesa = db.Column(db.Integer)
    NomeGrupoDespesa = db.Column(db.Text)
    codigoModalidade = db.Column(db.Integer)
    NomeModalidade = db.Column(db.Text)
    codigoElemento = db.Column(db.Integer)
    Natureza = db.Column(db.Text)
    id_titulo = db.Column(db.Text)

    def __repr__(self):
        return f'<NatDespesa {self.codigo} - {self.titulo}>'
