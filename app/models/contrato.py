"""
Modelo de Contrato.
"""
from app.extensions import db


class Contrato(db.Model):
    """Modelo para contratos."""

    __tablename__ = 'contratos'

    codigo = db.Column(db.String(20), primary_key=True)
    situacao = db.Column(db.String(50))
    numeroOriginal = db.Column(db.String(50))
    numProcesso = db.Column(db.String(50))
    objeto = db.Column(db.Text)
    natureza = db.Column('natureza', db.String(20))
    tipoContratante = db.Column(db.String(50))
    codigoContratante = db.Column(db.String(20))
    nomeContratante = db.Column(db.String(255))
    tipoContratado = db.Column(db.String(50))
    codigoContratado = db.Column(db.String(20))
    nomeContratado = db.Column(db.String(255))
    codigoBancoFavorecido = db.Column(db.String(10))
    codigoAgencia = db.Column(db.String(10))
    codigoConta = db.Column(db.String(20))
    valor = db.Column(db.Numeric(15, 2))
    valorTotal = db.Column(db.Numeric(15, 2))
    garantia = db.Column(db.String(20))
    valorGarantia = db.Column(db.Numeric(15, 2))
    dataProposta = db.Column(db.Date)
    dataCelebracao = db.Column(db.Date)
    dataPublicacao = db.Column(db.Date)
    dataInicioVigencia = db.Column(db.Date)
    dataFimVigencia = db.Column(db.Date)
    codigoModalidadeLicitacao = db.Column(db.String(10))
    nomeModalidadeLicitacao = db.Column(db.String(50))
    vinculoPPA = db.Column(db.String(5))
    regimeExecucao = db.Column(db.String(50))
    modalidade = db.Column(db.String(50))
    tipo_contrato = db.Column(db.String(2), nullable=True)  # S, M ou SM (fonte: CSV)
    percentualTerceiro = db.Column(db.Numeric(5, 2))
    objetivo = db.Column(db.Text)
    fundamentacaoLegal = db.Column(db.Text)
    dataConclusao = db.Column(db.Date)
    status = db.Column(db.String(50))
    responsaveisContrato = db.Column(db.Text)
    tipoRescisao = db.Column(db.String(50))
    dataRescisao = db.Column(db.Date)
    dataPublicacaoRescisao = db.Column(db.Date)
    valorMulta = db.Column(db.Numeric(15, 2))
    dataFimVigenciaTotal = db.Column(db.Date)
    vigencia_notificacao_silenciada = db.Column(db.Boolean, default=False, nullable=False)
    vigencia_silenciada_por = db.Column(db.BigInteger, nullable=True)
    vigencia_silenciada_em = db.Column(db.DateTime, nullable=True)
    vigencia_silenciada_motivo = db.Column(db.String(255), nullable=True)
    data_atualizacao = db.Column(db.TIMESTAMP)
    nomeContratadoResumido = db.Column(db.String(150))

    # Campos do módulo Prestações de Contratos
    natureza_id = db.Column(db.Integer, db.ForeignKey('natdespesas.id'), nullable=True)

    # Tipificação do contrato (classificação única)
    # CATSERV: até Classe (ou Grupo quando classe=NULL) / CATMAT: até PDM
    catserv_classe_id = db.Column(db.Integer, db.ForeignKey('catserv_classes.codigo_classe'), nullable=True)
    catserv_grupo_id = db.Column(db.Integer, db.ForeignKey('catserv_grupos.codigo_grupo'), nullable=True)
    catmat_classe_id = db.Column(db.Integer, nullable=True)  # Sem FK real (catmat é cópia local)
    catmat_pdm_id = db.Column(db.Integer, nullable=True)     # Sem FK real (catmat é cópia local)
    data_tipificacao = db.Column(db.DateTime, nullable=True)
    tipificado_por = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=True)

    # Centro de Custo e Tipo de Execução
    centro_de_custo_id = db.Column(db.Integer, db.ForeignKey('centrodecusto.id'), nullable=True)
    tipo_execucao_id = db.Column(db.Integer, db.ForeignKey('tipoexecucao.id'), nullable=True)

    # Relacionamentos
    nat_despesa = db.relationship('NatDespesa', backref='contratos', lazy=True)
    catserv_classe = db.relationship('CatservClasse', backref='contratos', lazy=True,
                                     foreign_keys=[catserv_classe_id])
    catserv_grupo = db.relationship('CatservGrupo', backref='contratos', lazy=True,
                                    foreign_keys=[catserv_grupo_id])
    usuario_tipificador = db.relationship('Usuario', foreign_keys=[tipificado_por], lazy=True)
    centro_de_custo = db.relationship('CentroDeCusto', backref='contratos', lazy=True)
    tipo_execucao = db.relationship('TipoExecucao', backref='contratos', lazy=True)
    fiscais = db.relationship('FiscalContrato', backref='contrato', lazy=True)
    prestacoes = db.relationship('Prestacao', backref='contrato_info', lazy=True,
                                 foreign_keys='Prestacao.codigo_contrato')
    saldos = db.relationship('SaldoContrato', backref='contrato', lazy=True,
                             foreign_keys='SaldoContrato.codigo_contrato')
    empenhos = db.relationship('EmpenhoContrato', backref='contrato', lazy=True,
                               foreign_keys='EmpenhoContrato.codigo_contrato')
    liquidacoes = db.relationship('LiquidacaoContrato', backref='contrato', lazy=True,
                                  foreign_keys='LiquidacaoContrato.codigo_contrato')
    pagamentos_contrato = db.relationship('PagamentoContrato', backref='contrato', lazy=True,
                                          foreign_keys='PagamentoContrato.codigo_contrato')

    def __repr__(self):
        return f'<Contrato {self.codigo}>'

    @property
    def tipo_contrato_label(self):
        """Retorna o tipo normalizado: SERVICO, MATERIAL ou MISTO.
        Lê da coluna tipo_contrato (S, M, SM) definida via CSV."""
        mapa = {'S': 'SERVICO', 'M': 'MATERIAL', 'SM': 'MISTO'}
        return mapa.get(self.tipo_contrato) if self.tipo_contrato else None

    @property
    def tipo_contrato_display(self):
        """Retorna o tipo de contrato formatado para exibição."""
        mapa = {
            'SERVICO': 'Serviço',
            'MATERIAL': 'Material',
            'MISTO': 'Serviço / Material'
        }
        return mapa.get(self.tipo_contrato_label, 'Não definido')

    @property
    def esta_tipificado(self):
        """Verifica se o contrato já foi tipificado.
        CATSERV: até Classe (ou Grupo quando serviço não tem classe) / CATMAT: até PDM."""
        tipo = self.tipo_contrato_label
        catserv_ok = self.catserv_classe_id is not None or self.catserv_grupo_id is not None
        catmat_ok = self.catmat_classe_id is not None and self.catmat_pdm_id is not None
        if tipo == 'SERVICO':
            return catserv_ok
        elif tipo == 'MATERIAL':
            return catmat_ok
        elif tipo == 'MISTO':
            return catserv_ok and catmat_ok
        return False

    @property
    def tipificacao_nivel(self):
        """Retorna o nível da tipificação CATSERV: 'classe', 'grupo' ou None."""
        if self.catserv_classe_id:
            return 'classe'
        elif self.catserv_grupo_id:
            return 'grupo'
        return None

    @property
    def saldo_atual(self):
        """Retorna o saldo atual do contrato (o mais recente) ou None."""
        if self.saldos:
            return self.saldos[0]
        return None

    @property
    def nome_exibicao(self):
        """Retorna o nome resumido ou completo do contratado."""
        return self.nomeContratadoResumido or self.nomeContratado

    @property
    def valor_formatado(self):
        """Retorna o valor formatado em moeda brasileira."""
        if self.valor is None:
            return 'R$ 0,00'
        return f'R$ {self.valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def valor_total_formatado(self):
        """Retorna o valor total formatado em moeda brasileira."""
        if self.valorTotal is None:
            return 'R$ 0,00'
        return f'R$ {self.valorTotal:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    @property
    def dados_bancarios(self):
        """Retorna dados bancários formatados."""
        if not self.codigoBancoFavorecido:
            return None
        partes = []
        if self.codigoBancoFavorecido:
            partes.append(f'Banco: {self.codigoBancoFavorecido}')
        if self.codigoAgencia:
            partes.append(f'Ag: {self.codigoAgencia}')
        if self.codigoConta:
            partes.append(f'Conta: {self.codigoConta}')
        return ' | '.join(partes)

    @property
    def fiscal_principal(self):
        """Retorna o fiscal principal (tipo 'Fiscal') ou o primeiro fiscal disponível."""
        if not self.fiscais:
            return None
        for fiscal in self.fiscais:
            if fiscal.tipo and fiscal.tipo.lower() == 'fiscal':
                return fiscal
        return self.fiscais[0]

    @property
    def natureza_display(self):
        """Retorna a natureza formatada (id_titulo da natdespesas ou fallback para coluna natureza)."""
        if self.nat_despesa:
            return self.nat_despesa.id_titulo or self.nat_despesa.titulo
        return self.natureza

    def _formatar_data(self, data):
        """Formata uma data para padrão brasileiro."""
        if data is None:
            return None
        return data.strftime('%d/%m/%Y')

    @property
    def data_celebracao_fmt(self):
        return self._formatar_data(self.dataCelebracao)

    @property
    def data_publicacao_fmt(self):
        return self._formatar_data(self.dataPublicacao)

    @property
    def data_inicio_vigencia_fmt(self):
        return self._formatar_data(self.dataInicioVigencia)

    @property
    def data_fim_vigencia_fmt(self):
        return self._formatar_data(self.dataFimVigencia)

    @property
    def data_fim_vigencia_total_fmt(self):
        return self._formatar_data(self.dataFimVigenciaTotal)
