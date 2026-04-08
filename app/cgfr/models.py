"""
Models do módulo CGFR (Consultoria de Gestão Financeira).
Tabela principal: cgfr_processo_enviado
Reutiliza tabelas existentes: natdespesas (NatDespesa), class_fonte (ClassFonte), acao (Acao).
"""
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import or_

from app.extensions import db


class Acao(db.Model):
    """Ações orçamentárias (tabela existente no banco)."""

    __tablename__ = 'acao'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.Integer)
    titulo = db.Column(db.Text)

    def __repr__(self):
        return f'<Acao {self.id} - {self.titulo}>'


class CgfrProcessoEnviado(db.Model):
    """Processo SEI enviado à CGFR.

    Campos sync: atualizados pela sincronização Trino, nunca editados pelo usuário.
    Campos editáveis: preenchidos pelo usuário, NUNCA sobrescritos pelo sync.
    """

    __tablename__ = 'cgfr_processo_enviado'

    # Prefixo SEAD — listagens filtram apenas processos da unidade
    PREFIXO_SEAD = '00002.'

    # === PK ===
    processo_formatado = db.Column(db.String(255), primary_key=True)

    # === Campos SYNC (atualizados pelo sync, nunca editáveis) ===
    link_acesso = db.Column(db.String(255))
    especificacao = db.Column(db.String(500))
    tipo_processo = db.Column(db.String(200))
    data_hora_processo = db.Column(db.DateTime)

    # Geração do processo
    id_unidade_geradora = db.Column(db.String(255))
    geracao_sigla = db.Column(db.String(255))
    geracao_data = db.Column(db.DateTime)
    geracao_descricao = db.Column(db.Text)
    usuario_gerador = db.Column(db.String(255))

    # Último andamento
    ultimo_andamento_sigla = db.Column(db.String(255))
    ultimo_andamento_descricao = db.Column(db.Text)
    ultimo_andamento_data = db.Column(db.DateTime)
    ultimo_andamento_usuario = db.Column(db.String(255))

    # Tramitação
    tramitado_sead_cgfr = db.Column(db.String(50))
    recebido_cgfr = db.Column(db.Integer, default=0)
    data_recebido_cgfr = db.Column(db.String(50))
    devolvido_cgfr_sead = db.Column(db.Integer, default=0)
    data_devolvido_cgfr_sead = db.Column(db.String(50))

    # === Campos EDITÁVEIS (preenchidos pelo usuário) ===
    # FKs: natureza → natdespesas.id, fonte → class_fonte.id, acao → acao.id
    natureza_despesa_id = db.Column(db.Integer, db.ForeignKey('natdespesas.id'), nullable=True)
    fonte_id = db.Column(db.Integer, db.ForeignKey('class_fonte.id'), nullable=True)
    acao_id = db.Column(db.Integer, nullable=True)  # ref acao.id (sem FK constraint pois acao não tem PRI)

    fornecedor = db.Column(db.String(255))
    objeto_do_pedido = db.Column(db.Text)
    necessidade = db.Column(db.Text)
    deliberacao = db.Column(db.Text)
    tipo_despesa = db.Column(db.String(50))  # 'Custeio' | 'Investimento'

    valor_solicitado = db.Column(db.Numeric(12, 2))
    valor_aprovado = db.Column(db.Numeric(12, 2))

    data_da_reuniao = db.Column(db.Date)
    observacao = db.Column(db.Text)
    possui_reserva = db.Column(db.Integer, default=0)
    valor_reserva = db.Column(db.String(30))
    nivel_prioridade = db.Column(db.String(10))  # 'Alto' | 'Médio' | 'Baixo'

    data_inclusao = db.Column(db.DateTime, default=datetime.now)
    vinculado_manualmente = db.Column(db.Boolean, default=False, server_default='0')

    # === Relationships ===
    natureza_rel = db.relationship('NatDespesa', backref='cgfr_processos', lazy=True,
                                    foreign_keys=[natureza_despesa_id])
    fonte_rel = db.relationship('ClassFonte', backref='cgfr_processos', lazy=True,
                                 foreign_keys=[fonte_id])

    @classmethod
    def filtro_visiveis(cls):
        """Retorna filtro SQLAlchemy: processos SEAD OU vinculados manualmente."""
        return or_(
            cls.processo_formatado.like(f'{cls.PREFIXO_SEAD}%'),
            cls.vinculado_manualmente == True
        )

    @property
    def classificado(self):
        """Processo está classificado quando natureza, fonte e ação estão preenchidos."""
        return all([self.natureza_despesa_id, self.fonte_id, self.acao_id])

    @property
    def status_classificacao(self):
        """Retorna string de status para exibição."""
        return 'Classificado' if self.classificado else 'Pendente'

    def to_dict(self) -> dict:
        """Serializa todas as colunas para JSON."""
        result = {}
        for c in self.__table__.columns:
            value = getattr(self, c.name)
            if isinstance(value, datetime):
                result[c.name] = value.strftime('%d/%m/%Y')
            elif isinstance(value, date):
                result[c.name] = value.strftime('%d/%m/%Y')
            elif isinstance(value, Decimal):
                result[c.name] = float(value)
            else:
                result[c.name] = value
        return result

    def __repr__(self):
        return f'<CgfrProcesso {self.processo_formatado}>'


class CgfrMovimentacao(db.Model):
    """Movimentação de documentos SEI para processos CGFR.
    Espelha a estrutura de SeiMovimentacao (app/models/sei.py).
    """

    __tablename__ = 'cgfrmovimentacao'

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
        return f'<CgfrMovimentacao {self.id_documento}>'
