"""
Helper de notificacoes para o modulo de Diarias.

Centraliza a logica de quem notificar em cada etapa do fluxo.
Usa o NotificationEngine como backend.

Uso:
    from app.services.diarias_notification import DiariasNotifier
    DiariasNotifier.notificar_etapa(itinerario, 'ob_inserida', usuario_responsavel_id)
"""
from typing import Optional
from flask import current_app

from app.services.notification_engine import NotificationEngine
from app.models.usuario import Usuario
from app.models.perfil import PerfilPermissao, Perfil
from app.extensions import db


class DiariasNotifier:
    """Gerencia notificacoes do fluxo de diarias."""

    @staticmethod
    def _resolver_solicitante_id(itinerario) -> Optional[int]:
        """Retorna o ID do usuario solicitante (quem criou a diaria)."""
        if not itinerario.usuario_gerador:
            return None
        usuario = Usuario.query.filter_by(
            sigla_login=itinerario.usuario_gerador, ativo=True
        ).first()
        return usuario.id if usuario else None

    @staticmethod
    def _resolver_usuarios_financeiro() -> list:
        """Retorna IDs de usuarios com permissao financeiro.visualizar."""
        try:
            usuarios = Usuario.query.join(
                Perfil, Usuario.perfil_id == Perfil.id
            ).join(
                PerfilPermissao, PerfilPermissao.perfil_id == Perfil.id
            ).filter(
                PerfilPermissao.modulo == 'financeiro',
                PerfilPermissao.acao == 'visualizar',
                Usuario.ativo == True,
            ).all()
            return [u.id for u in usuarios]
        except Exception:
            return []

    @staticmethod
    def _resolver_usuarios_diarias() -> list:
        """Retorna IDs de usuarios com permissao diarias.visualizar."""
        try:
            usuarios = Usuario.query.join(
                Perfil, Usuario.perfil_id == Perfil.id
            ).join(
                PerfilPermissao, PerfilPermissao.perfil_id == Perfil.id
            ).filter(
                PerfilPermissao.modulo == 'diarias',
                PerfilPermissao.acao == 'visualizar',
                Usuario.ativo == True,
            ).all()
            return [u.id for u in usuarios]
        except Exception:
            return []

    @staticmethod
    def _resolver_admins() -> list:
        """Retorna IDs dos administradores."""
        try:
            return [u.id for u in Usuario.query.filter_by(is_admin=True, ativo=True).all()]
        except Exception:
            return []

    @staticmethod
    def _resolver_superintendente() -> list:
        """Retorna IDs dos usuarios com cargo de Superintendente."""
        try:
            usuarios = Usuario.query.filter(
                Usuario.ativo == True,
                db.or_(
                    Usuario.cargo.ilike('%superintendente%'),
                )
            ).all()
            return [u.id for u in usuarios]
        except Exception:
            return []

    @staticmethod
    def _url_detalhes_cliente(itinerario) -> str:
        """URL da pagina de detalhes do lado do cliente."""
        return f'/diarias/detalhes/{itinerario.id}'

    @staticmethod
    def _url_detalhes_financeiro(itinerario) -> str:
        """URL da pagina de detalhes do lado financeiro."""
        return f'/financeiro/diarias/{itinerario.id}'

    @staticmethod
    def _processo_label(itinerario) -> str:
        """Retorna label do processo para mensagens."""
        return itinerario.sei_protocolo or itinerario.n_processo or f'Diaria #{itinerario.id}'

    @staticmethod
    def notificar_etapa(itinerario, tipo_sufixo: str, usuario_responsavel_id: Optional[int] = None):
        """
        Notifica os usuarios relevantes sobre uma etapa do fluxo de diarias.

        Args:
            itinerario: DiariasItinerario instance
            tipo_sufixo: Sufixo do tipo de notificacao (ex: 'ob_inserida', 'relatorio_viagem')
            usuario_responsavel_id: ID do usuario que executou a acao (excluido dos destinatarios)
        """
        tipo_codigo = f'diarias.{tipo_sufixo}'
        processo = DiariasNotifier._processo_label(itinerario)
        solicitante_id = DiariasNotifier._resolver_solicitante_id(itinerario)

        # Mapeia tipo → config de notificacao
        config = NOTIFICACAO_CONFIG.get(tipo_sufixo)
        if not config:
            current_app.logger.warning(
                f'DiariasNotifier: tipo_sufixo "{tipo_sufixo}" nao configurado'
            )
            return

        # Resolve destinatarios
        destinatarios = set()

        if config.get('solicitante') and solicitante_id:
            destinatarios.add(solicitante_id)

        if config.get('financeiro'):
            destinatarios.update(DiariasNotifier._resolver_usuarios_financeiro())

        if config.get('diarias'):
            destinatarios.update(DiariasNotifier._resolver_usuarios_diarias())

        if config.get('admins'):
            destinatarios.update(DiariasNotifier._resolver_admins())

        if config.get('superintendente'):
            destinatarios.update(DiariasNotifier._resolver_superintendente())

        # Remove o usuario que executou a acao (ele ja sabe)
        if usuario_responsavel_id and usuario_responsavel_id in destinatarios:
            destinatarios.discard(usuario_responsavel_id)

        if not destinatarios:
            return

        # Monta titulo e mensagem
        titulo = config['titulo'].format(processo=processo)
        mensagem = config['mensagem'].format(processo=processo)

        # URL de referencia
        ref_url = config.get('url_fn', lambda it: '')(itinerario)

        try:
            resultado = NotificationEngine.notificar(
                tipo_codigo=tipo_codigo,
                destinatarios=list(destinatarios),
                titulo=titulo,
                mensagem=mensagem,
                ref_modulo='diarias',
                ref_id=str(itinerario.id),
                ref_url=ref_url,
            )
            if resultado.get('erros'):
                current_app.logger.warning(
                    f'DiariasNotifier: erros ao notificar {tipo_codigo}: {resultado["erros"]}'
                )
        except Exception as e:
            current_app.logger.error(f'DiariasNotifier: erro ao notificar {tipo_codigo}: {e}')


# ── Configuracao de cada tipo de notificacao ──
# Chave = tipo_sufixo (diarias.{chave})
# solicitante: notifica quem criou a diaria
# financeiro: notifica usuarios com permissao financeiro
# diarias: notifica usuarios com permissao diarias
# admins: notifica administradores
# url_fn: funcao(itinerario) → URL

_url_cliente = DiariasNotifier._url_detalhes_cliente
_url_financeiro = DiariasNotifier._url_detalhes_financeiro

NOTIFICACAO_CONFIG = {
    # Etapa 1: Solicitacao criada — notifica financeiro, diárias e superintendente
    'nova_solicitacao': {
        'titulo': 'Nova Solicitacao de Diaria',
        'mensagem': 'Uma nova solicitacao de diaria foi criada: {processo}. Aguardando assinatura do Superintendente e autorizacao do Secretario.',
        'solicitante': False, 'financeiro': True, 'diarias': True, 'admins': False,
        'superintendente': True,
        'url_fn': _url_financeiro,
    },

    # Superintendente assinou requisições
    'assinatura_superintendente': {
        'titulo': 'Superintendente Assinou Requisicoes',
        'mensagem': 'O Superintendente assinou as requisicoes do processo {processo}. Aguardando autorizacao do Secretario.',
        'solicitante': False, 'financeiro': False, 'diarias': True, 'admins': True,
        'superintendente': False,
        'url_fn': _url_financeiro,
    },

    # NR inserida
    'nota_reserva': {
        'titulo': 'Nota de Reserva Inserida',
        'mensagem': 'A Nota de Reserva e Quadro Orcamentario foram inseridos no processo {processo}.',
        'solicitante': True, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # NE inserida
    'nota_empenho': {
        'titulo': 'Nota de Empenho Inserida',
        'mensagem': 'A Nota de Empenho foi inserida no processo {processo}.',
        'solicitante': True, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Despacho CCDP
    'despacho_ccdp': {
        'titulo': 'Despacho CCDP Gerado',
        'mensagem': 'O Despacho CCDP foi gerado para o processo {processo}. Aguardando ciencia da SGA.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Ciencia SGA
    'ciencia_sga': {
        'titulo': 'Ciencia SGA Registrada',
        'mensagem': 'O Superintendente registrou ciencia no processo {processo}. Encaminhado ao NCI.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Analise NCI
    'analise_nci': {
        'titulo': 'Analise NCI Concluida',
        'mensagem': 'O NCI concluiu a analise de pagamento do processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Despacho APOIO
    'despacho_apoio': {
        'titulo': 'Despacho APOIO/DFIN Gerado',
        'mensagem': 'Despacho APOIO/DFIN gerado para o processo {processo}. Aguardando Diretor DFIN.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Despacho Diretor
    'despacho_diretor': {
        'titulo': 'Despacho Diretor DFIN Gerado',
        'mensagem': 'O Diretor DFIN gerou o despacho para o processo {processo}. Encaminhado a GEO.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Despacho GEO
    'despacho_geo': {
        'titulo': 'Despacho GEO Gerado',
        'mensagem': 'O GEO gerou o despacho para o processo {processo}. Encaminhado a CCDP para liquidacao.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # NL
    'nl_inserida': {
        'titulo': 'NL Inserida',
        'mensagem': 'Nota de Liquidacao inserida no processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # PD
    'pd_inserida': {
        'titulo': 'PD Inserida',
        'mensagem': 'Programacao de Desembolso inserida no processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # OB (alerta para solicitante)
    'ob_inserida': {
        'titulo': 'Pagamento Realizado - OB Inserida',
        'mensagem': 'A Ordem Bancaria foi inserida no processo {processo}. Voce deve gerar o Relatorio de Viagem.',
        'solicitante': True, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_cliente,
    },

    # Relatorio de Viagem
    'relatorio_viagem': {
        'titulo': 'Relatorio de Viagem Gerado',
        'mensagem': 'O Relatorio de Viagem foi gerado para o processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Comprovante de Viagem (alerta para financeiro/CCDP)
    'comprovante_viagem': {
        'titulo': 'Comprovante de Viagem Enviado',
        'mensagem': 'O comprovante de viagem foi enviado para o processo {processo}. CCDP deve finalizar a prestacao de contas.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # NP
    'np_inserida': {
        'titulo': 'Nota Patrimonial Inserida',
        'mensagem': 'NP inserida no processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Prestacao SCDP
    'prestacao_scdp': {
        'titulo': 'Prestacao SCDP Enviada',
        'mensagem': 'Documento de Prestacao SCDP enviado para o processo {processo}.',
        'solicitante': False, 'financeiro': True, 'diarias': False, 'admins': False,
        'url_fn': _url_financeiro,
    },

    # Processo concluido (alerta para todos)
    'processo_concluido': {
        'titulo': 'Processo de Diaria Concluido',
        'mensagem': 'O processo {processo} foi pago e concluido pela CCDP.',
        'solicitante': True, 'financeiro': True, 'diarias': True, 'admins': True,
        'url_fn': _url_cliente,
    },
}
