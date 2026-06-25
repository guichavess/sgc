"""
Modelo de Log de Sincronização do módulo de Pagamentos.

Registra cada execução da rotina "Sincronizar Tudo (SEI + Etapas + Saldos)",
seja disparada manualmente pelo botão do dashboard ou pelo job agendado.

É a fonte única e confiável da data de "última atualização" exibida no dashboard
de solicitações — independente de paginação ou de existência de saldo por item.
"""
from datetime import datetime

from app.extensions import db


class SincronizacaoLog(db.Model):
    """Registro de uma execução da sincronização do módulo de Pagamentos."""

    __tablename__ = 'sis_sincronizacao_log'

    id = db.Column(db.Integer, primary_key=True)
    iniciado_em = db.Column(db.DateTime, default=datetime.now, nullable=False)
    finalizado_em = db.Column(db.DateTime, nullable=True, index=True)

    # sucesso | parcial | erro | em_andamento
    status = db.Column(db.String(20), default='em_andamento', nullable=False)
    # manual | agendada
    origem = db.Column(db.String(20), default='manual', nullable=False)

    docs_atualizados = db.Column(db.Integer, default=0)
    etapas_avancadas = db.Column(db.Integer, default=0)
    saldos_atualizados = db.Column(db.Integer, default=0)
    erros = db.Column(db.Integer, default=0)

    # None = execução automática (job agendado, sem autor humano)
    usuario_id = db.Column(db.Integer, nullable=True, index=True)

    def __repr__(self):
        return (
            f'<SincronizacaoLog {self.origem} {self.status} '
            f'{self.finalizado_em}>'
        )
