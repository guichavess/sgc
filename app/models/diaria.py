"""
Models do módulo de Diárias (Solicitação de Viagens).

Tabelas com prefixo diarias_ para evitar conflito com tabelas existentes.
Tabelas de referência (estados, municipios, setor, orgao) vivem no banco
original 'solicitacoes' e são mapeadas sem prefixo.
"""
from datetime import date, datetime
from decimal import Decimal
from app.extensions import db


# ── Tabelas de referência (já existem no banco 'solicitacoes') ──────────────

class Estado(db.Model):
    """Estados brasileiros (tabela já existente)."""
    __tablename__ = 'estados'

    cod_ibge = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<Estado {self.cod_ibge} - {self.nome}>'


class Municipio(db.Model):
    """Municípios brasileiros (tabela já existente)."""
    __tablename__ = 'municipios'

    cod_ibge = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(200), nullable=False)
    regiao_economica = db.Column(db.String(200))

    def __repr__(self):
        return f'<Municipio {self.cod_ibge} - {self.nome}>'


class Orgao(db.Model):
    """Órgãos governamentais (tabela já existente)."""
    __tablename__ = 'orgao'

    idorgao = db.Column(db.BigInteger, primary_key=True)
    nome = db.Column(db.Text)
    sigla = db.Column(db.Text)

    def __repr__(self):
        return f'<Orgao {self.idorgao} - {self.sigla}>'


class Setor(db.Model):
    """Setores (tabela já existente, atualizada com sigla)."""
    __tablename__ = 'setor'

    identidade = db.Column(db.BigInteger, primary_key=True)
    nome = db.Column(db.Text)
    idorgao = db.Column(db.BigInteger, db.ForeignKey('orgao.idorgao'))
    sigla = db.Column(db.String(50))

    orgao = db.relationship('Orgao', backref='setores', lazy='joined')

    def __repr__(self):
        return f'<Setor {self.identidade} - {self.sigla or self.nome}>'


# ── Tabelas do módulo Diárias (prefixo diarias_) ───────────────────────────

class DiariasStatusViagem(db.Model):
    """Status possíveis de uma viagem."""
    __tablename__ = 'diarias_status_viagens'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f'<DiariasStatusViagem {self.id} - {self.nome}>'


class DiariasTipoItinerario(db.Model):
    """Tipo de itinerário (Estadual, Nacional, Internacional)."""
    __tablename__ = 'diarias_tipo_itinerario'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f'<DiariasTipoItinerario {self.id} - {self.nome}>'


class DiariasTipoSolicitacao(db.Model):
    """Tipo da solicitação (Apenas Diárias, Diárias + Passagens Aéreas, Apenas Passagens Aéreas)."""
    __tablename__ = 'diarias_tipo_solicitacao'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<DiariasTipoSolicitacao {self.id} - {self.nome}>'


class DiariasCargo(db.Model):
    """Cargos/funções para cálculo de diárias."""
    __tablename__ = 'diarias_cargos'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<DiariasCargo {self.id} - {self.nome}>'


class DiariasValorCargo(db.Model):
    """Valor da diária por cargo e tipo de itinerário."""
    __tablename__ = 'diarias_valor_cargo'

    id = db.Column(db.Integer, primary_key=True)
    cargo_id = db.Column(db.Integer, db.ForeignKey('diarias_cargos.id'), nullable=False)
    tipo_itinerario_id = db.Column(db.Integer, db.ForeignKey('diarias_tipo_itinerario.id'), nullable=False)
    valor = db.Column(db.Numeric(10, 2), nullable=False)

    cargo = db.relationship('DiariasCargo', backref='valores', lazy='joined')
    tipo_itinerario = db.relationship('DiariasTipoItinerario', lazy='joined')

    def __repr__(self):
        return f'<DiariasValorCargo {self.id} - Cargo {self.cargo_id} Tipo {self.tipo_itinerario_id}>'


class DiariasNatureza(db.Model):
    """Natureza da despesa de viagem."""
    __tablename__ = 'diarias_natureza'

    id = db.Column(db.Integer, primary_key=True)
    cod_natureza = db.Column(db.Integer)
    cod_subnatureza = db.Column(db.Integer)
    nome_natureza = db.Column(db.String(255))
    nome_subnatureza = db.Column(db.String(255))

    def __repr__(self):
        return f'<DiariasNatureza {self.id} - {self.nome_natureza}>'


class DiariasServidor(db.Model):
    """Servidores disponíveis para viagens (importados do CSV ou cadastro manual)."""
    __tablename__ = 'diarias_servidores'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(255), nullable=False)
    matricula = db.Column(db.String(20), unique=True, nullable=True)
    cpf = db.Column(db.String(20), nullable=False, unique=True, index=True)
    cargo = db.Column(db.String(255))
    setor = db.Column(db.String(255))
    vinculo = db.Column(db.String(100))
    num_banco = db.Column(db.String(10))
    num_agencia_banco = db.Column(db.String(20))
    num_op_banco = db.Column(db.String(10))
    num_conta_banco = db.Column(db.String(30))
    nome_orgao = db.Column(db.String(255))
    nome_entidade = db.Column(db.String(255))
    nome_superintendencia = db.Column(db.String(255))

    def __repr__(self):
        return f'<DiariasServidor {self.nome} - {self.cpf}>'


class DiariasAgencia(db.Model):
    """Agências de viagem para cotações."""
    __tablename__ = 'diarias_agencias'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nome = db.Column(db.String(255), nullable=False)
    siafe = db.Column(db.String(50))

    def __repr__(self):
        return f'<DiariasAgencia {self.id} - {self.nome}>'


class DiariasItinerario(db.Model):
    """Solicitação de viagem (itinerário principal)."""
    __tablename__ = 'diarias_itinerario'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    usuario_gerador = db.Column(db.String(100), nullable=False)
    tipo_solicitacao_id = db.Column(db.Integer, db.ForeignKey('diarias_tipo_solicitacao.id'), nullable=False)
    qtd_diarias_solicitadas = db.Column(db.Float, nullable=False)
    tipo_itinerario = db.Column(db.Integer, db.ForeignKey('diarias_tipo_itinerario.id'), nullable=False)
    n_processo = db.Column(db.String(100))
    status_id = db.Column(db.Integer, db.ForeignKey('diarias_status_viagens.id'), nullable=False, default=1)
    data_solicitacao = db.Column(db.Date, nullable=False, default=date.today)
    data_viagem = db.Column(db.DateTime, nullable=False)
    data_retorno = db.Column(db.DateTime, nullable=False)
    origem = db.Column(db.String(255))
    estado_origem = db.Column(db.Integer)
    estado_destino = db.Column(db.Integer)
    objetivo = db.Column(db.Text, nullable=True)
    valor_total = db.Column(db.Numeric(10, 2))

    # SEI Integration
    sei_protocolo = db.Column(db.String(50), nullable=True)         # Número formatado do processo SEI
    sei_id_procedimento = db.Column(db.String(50), nullable=True)   # ID interno do procedimento SEI
    sei_id_memorando = db.Column(db.String(50), nullable=True)      # ID do documento MEMORANDO_SGA
    sei_memorando_formatado = db.Column(db.String(50), nullable=True)  # Número formatado do memorando
    sei_id_requisicao = db.Column(db.String(50), nullable=True)     # ID do documento REQUISIÇÃO DE DIÁRIAS
    sei_requisicao_formatado = db.Column(db.String(50), nullable=True)  # Número formatado da requisição
    sei_id_requisicao_passagens = db.Column(db.String(50), nullable=True)  # ID do documento REQUISIÇÃO DE PASSAGENS AÉREAS
    sei_requisicao_passagens_formatado = db.Column(db.String(50), nullable=True)  # Número formatado da req. passagens
    sei_id_doc_externo = db.Column(db.String(50), nullable=True)     # ID do documento externo (anexo)
    sei_doc_externo_formatado = db.Column(db.String(50), nullable=True)  # Número formatado do doc externo

    # Autorização do Secretário (gerada pelo sistema ou detectada via SEI)
    sei_id_autorizacao = db.Column(db.String(50), nullable=True)
    sei_autorizacao_formatado = db.Column(db.String(50), nullable=True)

    # Despacho DFIN (gerado automaticamente após autorização)
    sei_id_despacho_dfin = db.Column(db.String(50), nullable=True)
    sei_despacho_dfin_formatado = db.Column(db.String(50), nullable=True)

    # Nota de Reserva (inserida pelo Financeiro)
    nota_reserva = db.Column(db.String(50), nullable=True)
    sei_id_nota_reserva = db.Column(db.String(50), nullable=True)
    sei_nota_reserva_formatado = db.Column(db.String(50), nullable=True)

    # Quadro Orçamentário (inserido pelo Financeiro após NR)
    quadro_ug = db.Column(db.String(20), nullable=True)
    quadro_funcao = db.Column(db.String(10), nullable=True)
    quadro_subfuncao = db.Column(db.String(10), nullable=True)
    quadro_programa = db.Column(db.String(10), nullable=True)
    quadro_plano_interno = db.Column(db.String(10), nullable=True)
    quadro_fonte_recursos = db.Column(db.String(20), nullable=True)
    quadro_natureza_despesa = db.Column(db.String(20), nullable=True)
    quadro_valor_inicial_nr = db.Column(db.Numeric(14, 2), nullable=True)
    quadro_saldo_nr = db.Column(db.Numeric(14, 2), nullable=True)
    quadro_valor_despesa = db.Column(db.Numeric(14, 2), nullable=True)
    quadro_saldo_atual_nr = db.Column(db.Numeric(14, 2), nullable=True)
    sei_id_quadro_orcamentario = db.Column(db.String(50), nullable=True)
    sei_quadro_orcamentario_formatado = db.Column(db.String(50), nullable=True)

    # Escolha de Passagens (administração)
    escolha_voo_ida_id = db.Column(db.BigInteger, db.ForeignKey('diarias_cotacoes_voos.id'), nullable=True)
    escolha_voo_volta_id = db.Column(db.BigInteger, db.ForeignKey('diarias_cotacoes_voos.id'), nullable=True)
    escolha_menor_valor = db.Column(db.Boolean, nullable=True)
    escolha_justificativa_codigos = db.Column(db.String(500), nullable=True)
    escolha_justificativa_outros = db.Column(db.Text, nullable=True)
    escolha_declaracao_responsabilidade = db.Column(db.Boolean, default=False)
    sei_id_escolha_passagens = db.Column(db.String(50), nullable=True)
    sei_escolha_passagens_formatado = db.Column(db.String(50), nullable=True)
    sei_id_memorando_cotacoes = db.Column(db.String(50), nullable=True)      # ID do 2º memorando (cotações)
    sei_memorando_cotacoes_formatado = db.Column(db.String(50), nullable=True)  # Número formatado

    # Autorização SCDP (PDF externo, inserido pelo CCDP no financeiro)
    sei_id_autorizacao_scdp = db.Column(db.String(50), nullable=True)
    sei_autorizacao_scdp_formatado = db.Column(db.String(50), nullable=True)

    # Nota de Empenho (documento SEI gerado pelo financeiro, idSerie 419)
    nota_empenho_codigo = db.Column(db.String(50), nullable=True)
    sei_id_nota_empenho = db.Column(db.String(50), nullable=True)
    sei_nota_empenho_formatado = db.Column(db.String(50), nullable=True)

    # Timeline / Etapa atual
    etapa_atual_id = db.Column(db.Integer, db.ForeignKey('diarias_etapas.id'), default=1)

    # Timestamps
    created_at = db.Column(db.TIMESTAMP, default=datetime.now)
    updated_at = db.Column(db.TIMESTAMP, default=datetime.now, onupdate=datetime.now)

    # Relationships
    tipo_solicitacao = db.relationship('DiariasTipoSolicitacao', lazy='joined')
    status = db.relationship('DiariasStatusViagem', lazy='joined')
    tipo = db.relationship('DiariasTipoItinerario', lazy='joined')
    etapa_atual = db.relationship('DiariasEtapa', foreign_keys=[etapa_atual_id], lazy='joined')
    escolha_voo_ida = db.relationship('DiariasCotacaoVoo', foreign_keys=[escolha_voo_ida_id], lazy='joined')
    escolha_voo_volta = db.relationship('DiariasCotacaoVoo', foreign_keys=[escolha_voo_volta_id], lazy='joined')
    itens = db.relationship('DiariasItemItinerario', backref='itinerario', lazy='dynamic',
                            cascade='all, delete-orphan')
    paradas = db.relationship('DiariasParada', backref='itinerario', lazy='dynamic',
                              cascade='all, delete-orphan')
    justificativa = db.relationship('DiariasJustificativa', backref='itinerario', uselist=False,
                                    lazy='joined', cascade='all, delete-orphan')
    cotacoes = db.relationship('DiariasCotacao', lazy='dynamic',
                               cascade='all, delete-orphan',
                               primaryjoin='DiariasItinerario.id == foreign(DiariasCotacao.itinerario_id)',
                               backref=db.backref('itinerario', lazy='joined'))
    cotacoes_voos = db.relationship('DiariasCotacaoVoo', lazy='dynamic',
                                    cascade='all, delete-orphan',
                                    primaryjoin='DiariasItinerario.id == foreign(DiariasCotacaoVoo.itinerario_id)',
                                    backref=db.backref('itinerario_ref', lazy='joined'))

    def __repr__(self):
        return f'<DiariasItinerario {self.id} - {self.usuario_gerador}>'

    @property
    def estado_origem_obj(self):
        if self.estado_origem:
            return Estado.query.get(self.estado_origem)
        return None

    @property
    def estado_destino_obj(self):
        if self.estado_destino:
            return Estado.query.get(self.estado_destino)
        return None

    @property
    def municipio_origem_obj(self):
        if self.origem:
            try:
                return Municipio.query.get(int(self.origem))
            except (ValueError, TypeError):
                return None
        return None

    @property
    def valor_total_formatado(self):
        """Retorna o valor total formatado em moeda brasileira."""
        if self.valor_total is None:
            return 'R$ 0,00'
        return f'R$ {self.valor_total:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


class DiariasItemItinerario(db.Model):
    """Pessoas incluídas na viagem."""
    __tablename__ = 'diarias_itens_itinerario'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    id_itinerario = db.Column(db.Integer, db.ForeignKey('diarias_itinerario.id'), nullable=False)
    cpf_pessoa = db.Column(db.String(20), nullable=False)
    matricula_pessoa = db.Column(db.String(20), nullable=True)
    nome_pessoa = db.Column(db.String(255), nullable=True)
    cargo_id = db.Column(db.Integer, db.ForeignKey('diarias_cargos.id'), nullable=True)
    cargo_assessorado_id = db.Column(db.Integer, db.ForeignKey('diarias_cargos.id'), nullable=True)
    natureza_id = db.Column(db.Integer, db.ForeignKey('diarias_natureza.id'))
    valor_cargo = db.Column(db.Numeric(10, 2), nullable=True, default=0)
    cotacao_id = db.Column(db.BigInteger, db.ForeignKey('diarias_cotacoes.id'), nullable=True)
    entidade_id = db.Column(db.BigInteger)

    # Campos vindos da API pessoaSGA (Gestor SEAD)
    banco_agencia = db.Column(db.String(50), nullable=True)
    banco_conta = db.Column(db.String(50), nullable=True)
    vinculo = db.Column(db.String(100), nullable=True)
    cargo_folha = db.Column(db.String(255), nullable=True)
    setor = db.Column(db.String(255), nullable=True)
    orgao = db.Column(db.String(255), nullable=True)

    cargo = db.relationship('DiariasCargo', foreign_keys=[cargo_id], lazy='joined')
    cargo_assessorado = db.relationship('DiariasCargo', foreign_keys=[cargo_assessorado_id], lazy='joined')
    natureza = db.relationship('DiariasNatureza', lazy='joined')
    cotacao = db.relationship('DiariasCotacao', lazy='joined')

    def __repr__(self):
        return f'<DiariasItemItinerario {self.id} - CPF {self.cpf_pessoa}>'

    @property
    def valor_cargo_formatado(self):
        """Retorna o valor do cargo formatado em moeda brasileira."""
        if self.valor_cargo is None:
            return 'R$ 0,00'
        return f'R$ {self.valor_cargo:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


class DiariasParada(db.Model):
    """Municípios de parada (viagens estaduais)."""
    __tablename__ = 'diarias_paradas'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    itinerario_id = db.Column(db.Integer, db.ForeignKey('diarias_itinerario.id'), nullable=False)
    municipio_id = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return f'<DiariasParada {self.id} - Municipio {self.municipio_id}>'

    @property
    def municipio(self):
        return Municipio.query.get(self.municipio_id)


class DiariasJustificativa(db.Model):
    """Justificativa da viagem."""
    __tablename__ = 'diarias_justificativa'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    itinerario_id = db.Column(db.Integer, db.ForeignKey('diarias_itinerario.id'), nullable=False)
    descricao = db.Column(db.Text, nullable=False)
    tipo_justificativa = db.Column(db.String(100))

    def __repr__(self):
        return f'<DiariasJustificativa {self.id} - Itinerario {self.itinerario_id}>'


class DiariasEtapa(db.Model):
    """Etapas do fluxo de diárias."""
    __tablename__ = 'diarias_etapas'

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(100), nullable=False)
    alias = db.Column(db.String(50), nullable=False)
    ordem = db.Column(db.Integer, nullable=False)
    cor_hex = db.Column(db.String(10))
    icone = db.Column(db.String(50))

    def __repr__(self):
        return f'<DiariasEtapa {self.id} - {self.nome}>'


class DiariasHistoricoMovimentacao(db.Model):
    """Histórico de movimentações (transições de etapa) das solicitações de diárias."""
    __tablename__ = 'diarias_historico_movimentacoes'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    id_itinerario = db.Column(db.Integer, db.ForeignKey('diarias_itinerario.id'), nullable=False)
    id_etapa_anterior = db.Column(db.Integer, nullable=True)
    id_etapa_nova = db.Column(db.Integer, db.ForeignKey('diarias_etapas.id'), nullable=False)
    id_usuario_responsavel = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)
    data_movimentacao = db.Column(db.DateTime, default=datetime.now)
    comentario = db.Column(db.Text, nullable=True)

    # Relationships
    etapa_nova = db.relationship('DiariasEtapa', foreign_keys=[id_etapa_nova])
    itinerario_ref = db.relationship('DiariasItinerario', foreign_keys=[id_itinerario],
                                     backref=db.backref('historico_movimentacoes', lazy='dynamic'))

    def __repr__(self):
        return f'<DiariasHistoricoMovimentacao {self.id} - Etapa {self.id_etapa_nova}>'


class DiariasCotacao(db.Model):
    """Cotações de agências de viagem (viagens nacionais)."""
    __tablename__ = 'diarias_cotacoes'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    itinerario_id = db.Column(db.Integer, nullable=False, index=True)
    contrato_codigo = db.Column(db.String(20), nullable=True, index=True)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    data_hora = db.Column(db.DateTime)

    contrato = db.relationship('Contrato', lazy='joined',
                               primaryjoin='foreign(DiariasCotacao.contrato_codigo) == Contrato.codigo')

    def __repr__(self):
        return f'<DiariasCotacao {self.id} - Contrato {self.contrato_codigo} - R${self.valor}>'

    @property
    def nome_agencia(self):
        """Retorna o nome da agência (do contrato vinculado)."""
        if self.contrato:
            return self.contrato.nomeContratadoResumido or self.contrato.nomeContratado or ''
        return ''

    @property
    def valor_formatado(self):
        """Retorna o valor formatado em moeda brasileira."""
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


class DiariasCotacaoVoo(db.Model):
    """Cotacoes detalhadas de voos (ida/volta, com suporte a conexao)."""
    __tablename__ = 'diarias_cotacoes_voos'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    itinerario_id = db.Column(db.BigInteger, nullable=False, index=True)
    contrato_codigo = db.Column(db.String(20), nullable=True, index=True)
    tipo_trecho = db.Column(db.String(10), nullable=False)  # 'ida' ou 'volta'

    # Trecho 1 (obrigatorio)
    cia = db.Column(db.String(50), nullable=False)
    voo = db.Column(db.String(20), nullable=False)
    saida = db.Column(db.DateTime, nullable=False)
    chegada = db.Column(db.DateTime, nullable=False)
    origem = db.Column(db.String(100), nullable=False)
    destino = db.Column(db.String(100), nullable=False)

    # Trecho 2 — conexao (opcional)
    cia_conexao = db.Column(db.String(50), nullable=True)
    voo_conexao = db.Column(db.String(20), nullable=True)
    saida_conexao = db.Column(db.DateTime, nullable=True)
    chegada_conexao = db.Column(db.DateTime, nullable=True)
    origem_conexao = db.Column(db.String(100), nullable=True)
    destino_conexao = db.Column(db.String(100), nullable=True)

    # Dados gerais da opcao
    bagagem = db.Column(db.String(50), nullable=True)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

    contrato = db.relationship('Contrato', lazy='joined',
                               primaryjoin='foreign(DiariasCotacaoVoo.contrato_codigo) == Contrato.codigo')

    def __repr__(self):
        return f'<DiariasCotacaoVoo {self.id} - {self.tipo_trecho} {self.cia} {self.voo} R${self.valor}>'

    @property
    def nome_agencia(self):
        if self.contrato:
            return self.contrato.nomeContratadoResumido or self.contrato.nomeContratado or ''
        return ''

    @property
    def valor_formatado(self):
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def tem_conexao(self):
        return bool(self.voo_conexao)

    @property
    def resumo_trecho(self):
        """Retorna resumo da rota, ex: 'Teresina > Brasilia > Congonhas'."""
        rota = f'{self.origem} > {self.destino}'
        if self.tem_conexao and self.destino_conexao:
            rota += f' > {self.destino_conexao}'
        return rota


# ── Tabelas de Controle de Diárias (acumulado + prestação de contas) ──────

class DiariasControleViagem(db.Model):
    """Viagem (nível processo) — agrupa servidores de um mesmo processo SEI."""
    __tablename__ = 'diarias_controle_viagens'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    processo = db.Column(db.String(50), unique=True, nullable=False, index=True)
    itinerario_id = db.Column(db.Integer, db.ForeignKey('diarias_itinerario.id'), nullable=True)
    setor_id = db.Column(db.BigInteger, db.ForeignKey('setor.identidade'), nullable=True)
    origem = db.Column(db.String(100))         # Nome texto (histórico/display)
    destino = db.Column(db.String(255))        # Nome texto (histórico/display)
    origem_id = db.Column(db.Integer, nullable=True)   # cod_ibge: municipio (estadual) ou estado (nacional)
    destino_id = db.Column(db.Integer, nullable=True)  # cod_ibge: municipio (estadual) ou estado (nacional)
    tipo_viagem = db.Column(db.SmallInteger, nullable=True)  # 1=Estadual, 2=Nacional
    data_inicio = db.Column(db.Date, nullable=False, index=True)
    data_termino = db.Column(db.Date, nullable=False)
    status_viagem = db.Column(db.SmallInteger, default=1)  # 1=Realizada, 2=Cancelada
    observacao = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.TIMESTAMP, default=datetime.now)

    # Relationships
    itinerario = db.relationship('DiariasItinerario', lazy='select',
                                 backref=db.backref('controle_viagem', uselist=False))
    setor = db.relationship('Setor', lazy='joined')
    servidores = db.relationship('DiariasControleServidor', back_populates='viagem',
                                 lazy='select', cascade='all, delete-orphan')

    # Status / Tipo constants
    STATUS_REALIZADA = 1
    STATUS_CANCELADA = 2
    TIPO_ESTADUAL = 1
    TIPO_NACIONAL = 2

    def __repr__(self):
        return f'<DiariasControleViagem {self.id} - {self.processo}>'

    @property
    def origem_nome(self):
        """Retorna nome da origem: resolve cod_ibge ou retorna texto histórico."""
        if self.origem_id:
            if self.tipo_viagem == self.TIPO_ESTADUAL:
                mun = Municipio.query.get(self.origem_id)
                return mun.nome if mun else self.origem
            else:
                est = Estado.query.get(self.origem_id)
                return est.nome if est else self.origem
        return self.origem

    @property
    def destino_nome(self):
        """Retorna nome do destino: resolve cod_ibge ou retorna texto histórico."""
        if self.destino_id:
            if self.tipo_viagem == self.TIPO_ESTADUAL:
                mun = Municipio.query.get(self.destino_id)
                return mun.nome if mun else self.destino
            else:
                est = Estado.query.get(self.destino_id)
                return est.nome if est else self.destino
        return self.destino


class DiariasControleServidor(db.Model):
    """Servidor em uma viagem — dados financeiros individuais."""
    __tablename__ = 'diarias_controle_servidores'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    viagem_id = db.Column(db.BigInteger, db.ForeignKey('diarias_controle_viagens.id'), nullable=False, index=True)
    cpf = db.Column(db.String(14), nullable=False, index=True)
    nome = db.Column(db.String(255))
    vinculo = db.Column(db.String(50))
    qtd_diarias = db.Column(db.Numeric(4, 1), nullable=False, default=0)
    valor_unitario = db.Column(db.Numeric(10, 2))
    valor_total = db.Column(db.Numeric(10, 2))
    natureza_despesa = db.Column(db.String(10))
    sub_item = db.Column(db.String(10))
    fonte_recursos = db.Column(db.String(20))
    baixa_np = db.Column(db.String(50), nullable=True)
    sistema_scdp = db.Column(db.String(20), nullable=True)

    # Relationships
    viagem = db.relationship('DiariasControleViagem', back_populates='servidores')
    prestacao = db.relationship('DiariasControlePrestacao', uselist=False,
                                back_populates='servidor', lazy='joined',
                                cascade='all, delete-orphan')

    def __repr__(self):
        return f'<DiariasControleServidor {self.id} - {self.cpf} - {self.nome}>'


class DiariasControlePrestacao(db.Model):
    """Prestação de contas de um servidor em uma viagem."""
    __tablename__ = 'diarias_controle_prestacao'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    servidor_id = db.Column(db.BigInteger, db.ForeignKey('diarias_controle_servidores.id'),
                            nullable=False, unique=True, index=True)
    status = db.Column(db.SmallInteger, default=2)  # 1=Entregue, 2=Pendente
    data_entrega = db.Column(db.Date, nullable=True)
    relatorio = db.Column(db.SmallInteger, nullable=True)  # 1=Aprovado, 2=Reprovado, 3=Pendente
    ano_referencia = db.Column(db.SmallInteger, nullable=True)

    # Relationships
    servidor = db.relationship('DiariasControleServidor', back_populates='prestacao')

    # Status constants
    STATUS_ENTREGUE = 1
    STATUS_PENDENTE = 2
    RELATORIO_APROVADO = 1
    RELATORIO_REPROVADO = 2
    RELATORIO_PENDENTE = 3

    def __repr__(self):
        return f'<DiariasControlePrestacao {self.id} - status={self.status}>'
