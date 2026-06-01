"""
Modelos do módulo Gestão do Fundo Rotativo.

Tabelas para registrar e gerenciar saldos do fundo rotativo (UG 210102).
"""
from datetime import datetime

from app.extensions import db


class FundoRotativoSaldo(db.Model):
    """Registro de saldo do Fundo Rotativo por fonte e exercício."""

    __tablename__ = 'fundo_rotativo_saldos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    valor = db.Column(db.Numeric(15, 2), nullable=False)
    data = db.Column(db.DateTime, nullable=False)
    fonte_codigo = db.Column(
        db.String(10),
        db.ForeignKey('class_fonte.codigo'),
        nullable=False,
        index=True,
    )
    natureza = db.Column(db.String(20), nullable=True, index=True)
    id_exercicio = db.Column(db.String(2), nullable=False, index=True)

    criado_por = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    data_criacao = db.Column(db.DateTime, default=datetime.utcnow)
    data_atualizacao = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    fonte = db.relationship(
        'ClassFonte',
        foreign_keys=[fonte_codigo],
        primaryjoin='FundoRotativoSaldo.fonte_codigo == ClassFonte.codigo',
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
        return self.fonte_codigo

    def __repr__(self):
        return (
            f'<FundoRotativoSaldo id={self.id} valor={self.valor} '
            f'fonte={self.fonte_codigo} exerc={self.id_exercicio}>'
        )
