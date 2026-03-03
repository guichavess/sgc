"""
Modelo de Solicitação de Pagamento.
"""
from datetime import datetime
from app.extensions import db


class Solicitacao(db.Model):
    """Modelo principal para solicitações de pagamento."""

    __tablename__ = 'sis_solicitacoes'

    id = db.Column(db.BigInteger, primary_key=True)
    codigo_contrato = db.Column(db.String(20), db.ForeignKey('contratos.codigo'), nullable=False)
    id_usuario_solicitante = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    etapa_atual_id = db.Column(db.Integer, db.ForeignKey('sis_etapas_fluxo.id'), default=1)
    status_empenho_id = db.Column(db.Integer, db.ForeignKey('sis_status_empenho.id'), nullable=True, default=3)
    id_tipo_pagamento = db.Column(db.Integer, db.ForeignKey('tipo_pagamento.id'), nullable=True)
    data_solicitacao = db.Column(db.DateTime, default=datetime.now)
    protocolo_gerado_sei = db.Column(db.String(50))
    id_procedimento_sei = db.Column(db.String(50))
    link_processo_sei = db.Column(db.Text)
    competencia = db.Column(db.String(25))
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
    tipo_pagamento = db.relationship('TipoPagamento', backref='solicitacoes')

    def __repr__(self):
        return f'<Solicitacao {self.id} - {self.protocolo_gerado_sei}>'

    @property
    def saldo_atual_contrato(self):
        """Busca o saldo mais recente para o contrato e competência."""
        from app.models.saldo import SaldoEmpenho
        return SaldoEmpenho.get_saldo_atual(self.codigo_contrato, self.competencia)

    @property
    def valor_empenho_solicitado(self):
        """Retorna o valor da última solicitação de empenho vinculada."""
        sol = SolicitacaoEmpenho.query.filter_by(id_solicitacao=self.id).first()
        return sol.valor if sol else None

    @property
    def tempo_decorrido_visual(self):
        """
        Retorna o tempo total do processo.
        - Se finalizado: Retorna o valor gravado em 'tempo_total'.
        - Se em aberto: Calcula a diferença entre AGORA e a data da PRIMEIRA movimentação histórica.
        """
        if self.tempo_total:
            return self.tempo_total

        # Para processos em aberto, usa a primeira movimentação como data real de início
        from app.models.historico import HistoricoMovimentacao
        primeiro_hist = HistoricoMovimentacao.query.filter_by(
            id_solicitacao=self.id
        ).order_by(HistoricoMovimentacao.data_movimentacao.asc()).first()

        if primeiro_hist and primeiro_hist.data_movimentacao:
            data_inicio = primeiro_hist.data_movimentacao
        else:
            data_inicio = self.data_solicitacao

        if not data_inicio:
            return "--"

        diff = datetime.now() - data_inicio
        total_seconds = int(diff.total_seconds())

        if total_seconds < 0:
            return "recentemente"

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
        Busca o código da natureza de despesa na tabela Empenho
        baseado no contrato e no ano da competência.
        """
        from app.models.empenho import Empenho

        try:
            ano = datetime.now().year
            if self.competencia and '/' in self.competencia:
                ano = int(self.competencia.split('/')[-1])

            cod_limpo = "".join(filter(str.isdigit, str(self.codigo_contrato)))

            empenho = Empenho.query.filter(
                Empenho.codContrato == cod_limpo,
                Empenho.anoProcesso == ano
            ).first()

            return empenho.codNatureza if empenho else "N/D"
        except (ValueError, AttributeError):
            return "Erro"


class SolicitacaoEmpenho(db.Model):
    """Modelo para solicitações de empenho vinculadas."""

    __tablename__ = 'solicitacaoempenho'

    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.DateTime, default=datetime.now)
    id_solicitacao = db.Column(db.BigInteger, db.ForeignKey('sis_solicitacoes.id'), nullable=False)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    competencia = db.Column(db.String(30))
    id_user = db.Column(db.BigInteger, db.ForeignKey('sis_usuarios.id'), nullable=False)
    ne = db.Column(db.String(50), nullable=True)
    saldo_momento = db.Column(db.Numeric(20, 2))

    def __repr__(self):
        return f'<SolicitacaoEmpenho {self.id}>'
