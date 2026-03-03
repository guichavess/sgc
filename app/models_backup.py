from app import db, login_manager
from datetime import datetime
from flask_login import UserMixin

# --- Tabela de Usuários ---
class Usuario(db.Model, UserMixin):
    __tablename__ = 'sis_usuarios'
    id = db.Column(db.BigInteger, primary_key=True)
    id_usuario_sei = db.Column(db.String(50), unique=True, nullable=False)
    nome = db.Column(db.String(255), nullable=False)
    sigla_login = db.Column(db.String(100), nullable=False)
    cargo = db.Column(db.String(255))
    unidade_padrao_id = db.Column(db.String(50))
    ultimo_login = db.Column(db.DateTime, default=datetime.now)
    ativo = db.Column(db.Boolean, default=True)

    @property
    def is_active(self):
        return self.ativo

@login_manager.user_loader
def load_user(user_id):
    return Usuario.query.get(int(user_id))

# --- Tabela de Saldo (Mova para cima da classe Solicitacao) ---
class SaldoEmpenho(db.Model):
    __tablename__ = 'sis_saldo_empenho_contrato'
    id = db.Column(db.Integer, primary_key=True)
    saldo = db.Column(db.Numeric(20, 2), nullable=False)
    data = db.Column(db.DateTime, default=datetime.now)
    cod_contrato = db.Column(db.String(20), nullable=False)
    competencia = db.Column(db.String(25), nullable=False) # Aumentado para 25

# --- Outras Tabelas de Apoio ---
class Empenho(db.Model):
    __tablename__ = 'empenho'
    id = db.Column(db.BigInteger, primary_key=True) 
    codigo = db.Column(db.Text)
    codProcesso = db.Column(db.Text)
    dataProcesso = db.Column(db.DateTime)
    assuntoProcesso = db.Column(db.Text)
    anoProcesso = db.Column(db.BigInteger)
    statusDocumento = db.Column(db.Text)
    codigoUG = db.Column(db.Text)
    nomeUG = db.Column(db.Text)
    codFonte = db.Column(db.BigInteger)
    codNatureza = db.Column(db.BigInteger)
    codigoCredor = db.Column(db.Text)
    nomeCredor = db.Column(db.Text)
    dataEmissao = db.Column(db.DateTime)
    dataCancelamento = db.Column(db.DateTime)
    dataContabilizacao = db.Column(db.DateTime)
    valor = db.Column(db.Float)
    vlr = db.Column(db.Float)
    observacao = db.Column(db.Text)
    cnpjCredor = db.Column(db.BigInteger)
    idNR = db.Column(db.BigInteger)
    codNR = db.Column(db.Text)
    modalidade = db.Column(db.Text)
    tipoAlteracaoNE = db.Column(db.Text)
    codContrato = db.Column(db.BigInteger)
    codAcao = db.Column(db.BigInteger)
    codDetalhamentoFonte = db.Column(db.BigInteger)
    codigoOrgao = db.Column(db.BigInteger)
    codigoModalidadeLicitacao = db.Column(db.BigInteger)
    descModalidadeLicitacao = db.Column(db.Text)
    codClassificacao = db.Column(db.Text)

class Contrato(db.Model):
    __tablename__ = 'contratos'
    codigo = db.Column(db.String(20), primary_key=True)
    situacao = db.Column(db.String(50))
    numeroOriginal = db.Column(db.String(50))
    numProcesso = db.Column(db.String(50))
    objeto = db.Column(db.Text)
    nomeContratado = db.Column(db.String(255))
    valor = db.Column(db.Numeric(15, 2))
    nomeContratadoResumido = db.Column(db.String(255))

class Etapa(db.Model):
    __tablename__ = 'sis_etapas_fluxo'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)
    alias = db.Column(db.String(50), nullable=False)
    ordem = db.Column(db.Integer, nullable=False)
    cor_hex = db.Column(db.String(10))

class StatusEmpenho(db.Model):
    __tablename__ = 'sis_status_empenho'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(50), nullable=False)
    cor_badge = db.Column(db.String(20), default='secondary')

# --- Tabela Principal ---
class Solicitacao(db.Model):
    __tablename__ = 'sis_solicitacoes'
    id = db.Column(db.BigInteger, primary_key=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    id_usuario_solicitante = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    etapa_atual_id = db.Column(db.Integer, db.ForeignKey('sis_etapas_fluxo.id'), default=1)
    status_empenho_id = db.Column(db.Integer, db.ForeignKey('sis_status_empenho.id'), nullable=True)
    data_solicitacao = db.Column(db.DateTime, default=datetime.now)
    protocolo_gerado_sei = db.Column(db.String(50)) 
    id_procedimento_sei = db.Column(db.String(50))
    link_processo_sei = db.Column(db.Text)
    competencia = db.Column(db.String(25)) # Aumentado de 7 para 25
    especificacao = db.Column(db.Text)
    descricao = db.Column(db.Text) 
    id_caixa_sei = db.Column(db.String(50))
    status_geral = db.Column(db.String(100), default='ABERTO')
    num_nl = db.Column(db.String(50))
    num_pd = db.Column(db.String(50))
    num_ob = db.Column(db.String(50))
    tempo_total = db.Column(db.String(50))
    
    # Relacionamentos
    contrato = db.relationship('Contrato', backref='solicitacoes')
    usuario = db.relationship('Usuario', backref='solicitacoes')
    etapa = db.relationship('Etapa', backref='solicitacoes')
    status_empenho = db.relationship('StatusEmpenho', backref='solicitacoes')

    @property
    def saldo_atual_contrato(self):
        """Busca o saldo mais recente para o contrato e competência"""
        # Como SaldoEmpenho agora está definido ANTES desta classe, não haverá erro de importação
        return SaldoEmpenho.query.filter_by(
            cod_contrato=self.codigo_contrato,
            competencia=self.competencia
        ).order_by(SaldoEmpenho.data.desc()).first()
    

    @property
    def valor_empenho_solicitado(self):
        """Retorna o valor da última solicitação de empenho vinculada"""
        from app.models import SolicitacaoEmpenho
        sol = SolicitacaoEmpenho.query.filter_by(id_solicitacao=self.id).first()
        return sol.valor if sol else None
    

    @property
    def tempo_decorrido_visual(self):
        """
        Retorna o tempo total do processo.
        - Se finalizado: Retorna o valor gravado em 'tempo_total'.
        - Se em aberto: Calcula a diferença entre AGORA e a DATA DE CRIAÇÃO (contador contínuo).
        """
        # 1. Se o processo já fechou (tem tempo total gravado), retorna ele estático.
        if self.tempo_total:
            return self.tempo_total
        
        if not self.data_solicitacao:
            return "--"

        # 2. Lógica Atualizada (Contagem Contínua):
        # Para processos em andamento, calculamos a diferença até o momento atual (agora).
        # Assim, se o processo ficar parado, os dias continuam sendo somados no relatório.
        diff = datetime.now() - self.data_solicitacao
        
        # 3. Formatação do tempo
        total_seconds = int(diff.total_seconds())
        days = diff.days
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        
        if days > 0:
            return f"{days} dias"
        elif hours > 0:
            return f"{hours} horas"
        elif minutes > 0:
            return f"{minutes} min"
        else:
            return "recentemente"
        

    @property
    def natureza_despesa(self):
        """
        Busca o código da natureza de despesa (codNatureza) na tabela Empenho
        baseado no contrato e no ano da competência.
        """
        try:
            # Tenta extrair o ano da competência (ex: 'Junho/2025' -> 2025)
            ano = datetime.now().year
            if self.competencia and '/' in self.competencia:
                ano = int(self.competencia.split('/')[-1])
            
            cod_limpo = "".join(filter(str.isdigit, str(self.codigo_contrato)))
            
            # Busca o primeiro empenho encontrado para este contrato neste ano
            # para pegar a natureza predominante
            empenho = Empenho.query.filter(
                Empenho.codContrato == cod_limpo,
                Empenho.anoProcesso == ano
            ).first()
            
            return empenho.codNatureza if empenho else "N/D"
        except:
            return "Erro"


class HistoricoMovimentacao(db.Model):
    __tablename__ = 'sis_historico_movimentacoes'
    id = db.Column(db.BigInteger, primary_key=True)
    id_solicitacao = db.Column(db.BigInteger, db.ForeignKey('sis_solicitacoes.id'), nullable=False)
    id_etapa_anterior = db.Column(db.Integer)
    id_etapa_nova = db.Column(db.Integer, db.ForeignKey('sis_etapas_fluxo.id'), nullable=False)
    id_usuario_responsavel = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'))
    data_movimentacao = db.Column(db.DateTime, default=datetime.now)
    comentario = db.Column(db.Text)
    etapa_nova = db.relationship('Etapa', foreign_keys=[id_etapa_nova])
    usuario = db.relationship('Usuario')

class SolicitacaoEmpenho(db.Model):
    __tablename__ = 'solicitacaoempenho'
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.DateTime, default=datetime.now)
    id_solicitacao = db.Column(db.BigInteger, db.ForeignKey('sis_solicitacoes.id'), nullable=False)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    competencia = db.Column(db.String(30)) 
    id_user = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    ne = db.Column(db.String(50), nullable=True)
    saldo_momento = db.Column(db.Numeric(20, 2))



# No arquivo: pagamentos/app/models.py

class SeiMovimentacao(db.Model):
    __tablename__ = 'seimovimentacao'
    
    # Chaves Principais
    # Alterado para String para garantir compatibilidade com IDs grandes/formatados
    id_documento = db.Column('IdDocumento', db.String(50), primary_key=True)
    protocolo_procedimento = db.Column(db.String(50), index=True) # Sua chave de ligação
    
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
    
    # Campos de Controle (opcionais, mas estavam no seu CSV)
    obs = db.Column(db.Text)
    tempo_execucao = db.Column(db.Float)