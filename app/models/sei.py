"""
Modelo de Movimentação SEI.
"""
from app.extensions import db


class SeiMovimentacao(db.Model):
    """Modelo para dados de movimentação do SEI."""

    __tablename__ = 'seimovimentacao'

    # Chaves Principais
    id_documento = db.Column('IdDocumento', db.String(50), primary_key=True)
    protocolo_procedimento = db.Column(db.String(50), index=True)

    # Dados do Procedimento
    id_procedimento = db.Column('IdProcedimento', db.String(50))
    procedimento_formatado = db.Column('ProcedimentoFormatado', db.String(50))

    # Dados do Documento
    documento_formatado = db.Column('DocumentoFormatado', db.String(50))
    link_acesso = db.Column('LinkAcesso', db.Text)
    descricao = db.Column('Descricao', db.Text)
    data = db.Column('Data', db.String(20))
    numero = db.Column('Numero', db.String(50))

    # Dados da Série
    id_serie = db.Column('IdSerie', db.Integer)
    serie_nome = db.Column('Serie.Nome', db.String(255))
    serie_aplicabilidade = db.Column('Serie.Aplicabilidade', db.String(100))

    # Dados da Unidade Elaboradora
    unidade_id = db.Column('UnidadeElaboradora.IdUnidade', db.String(50))
    unidade_sigla = db.Column('UnidadeElaboradora.Sigla', db.String(50))
    unidade_descricao = db.Column('UnidadeElaboradora.Descricao', db.String(255))

    # Campos de Controle
    obs = db.Column(db.Text)
    tempo_execucao = db.Column(db.Float)

    def __repr__(self):
        return f'<SeiMovimentacao {self.id_documento}>'
