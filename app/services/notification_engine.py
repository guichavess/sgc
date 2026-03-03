"""
Motor central de notificacoes do SGC.

Ponto de entrada unico para criar e despachar notificacoes.
Substitui e expande o notification_service.py original.

Uso:
    from app.services.notification_engine import NotificationEngine

    NotificationEngine.notificar(
        tipo_codigo='solicitacao.etapa_avancou',
        destinatarios=[1, 2, 3],
        titulo='Etapa atualizada',
        mensagem='Solicitacao X avancou para Y',
        ref_modulo='solicitacoes',
        ref_id='123',
        ref_url='/solicitacoes/solicitacao/123'
    )
"""
from typing import List, Optional
from flask import current_app

from app.extensions import db
from app.models.notificacao import NotificacaoTipo, Notificacao, NotificacaoPreferencia
from app.models.usuario import Usuario
from app.models.perfil import PerfilPermissao
from app.repositories.notificacao_repository import NotificacaoRepository


class NotificationEngine:
    """Motor central de notificacoes."""

    # Canais externos em standby - alterar para True quando ativar
    EMAIL_ATIVO = False
    TELEGRAM_ATIVO = False

    @staticmethod
    def notificar(
        tipo_codigo: str,
        destinatarios: List[int],
        titulo: str,
        mensagem: str,
        ref_modulo: Optional[str] = None,
        ref_id: Optional[str] = None,
        ref_url: Optional[str] = None,
        nivel_override: Optional[str] = None,
    ) -> dict:
        """
        Ponto de entrada unico para criar notificacoes.

        Args:
            tipo_codigo: Codigo do tipo (ex: 'solicitacao.etapa_avancou')
            destinatarios: Lista de usuario_ids
            titulo: Titulo da notificacao
            mensagem: Corpo da notificacao
            ref_modulo: Modulo de referencia (solicitacoes, financeiro, etc.)
            ref_id: ID do objeto referenciado
            ref_url: URL para navegacao direta
            nivel_override: Nivel forçado (ignora o nivel do tipo)

        Returns:
            dict com contagens: criadas, email_enviados, telegram_enviados, erros
        """
        resultado = {
            'criadas': 0,
            'email_enviados': 0,
            'telegram_enviados': 0,
            'erros': [],
        }

        try:
            tipo = NotificacaoTipo.query.filter_by(codigo=tipo_codigo, ativo=True).first()
            if not tipo:
                resultado['erros'].append(f'Tipo "{tipo_codigo}" nao encontrado ou inativo')
                return resultado

            nivel = nivel_override or tipo.nivel

            # Remover duplicatas
            destinatarios_unicos = list(set(destinatarios))

            for usuario_id in destinatarios_unicos:
                try:
                    _criou = NotificationEngine._processar_destinatario(
                        tipo=tipo,
                        usuario_id=usuario_id,
                        titulo=titulo,
                        mensagem=mensagem,
                        nivel=nivel,
                        ref_modulo=ref_modulo,
                        ref_id=ref_id,
                        ref_url=ref_url,
                        resultado=resultado,
                    )
                except Exception as e:
                    resultado['erros'].append(
                        f'Erro para usuario {usuario_id}: {str(e)}'
                    )

            db.session.commit()

        except Exception as e:
            db.session.rollback()
            resultado['erros'].append(f'Erro geral: {str(e)}')
            current_app.logger.error(f'NotificationEngine.notificar erro: {e}')

        return resultado

    @staticmethod
    def _processar_destinatario(
        tipo: NotificacaoTipo,
        usuario_id: int,
        titulo: str,
        mensagem: str,
        nivel: str,
        ref_modulo: Optional[str],
        ref_id: Optional[str],
        ref_url: Optional[str],
        resultado: dict,
    ) -> bool:
        """Processa notificacao para um destinatario."""
        usuario = Usuario.query.get(usuario_id)
        if not usuario or not usuario.ativo:
            return False

        # Verificar preferencias do usuario
        pref = NotificacaoRepository.obter_preferencia(usuario_id, tipo.id)
        if pref and pref.silenciado:
            return False

        # Deduplicacao
        if ref_id and NotificacaoRepository.existe_recente(
            tipo.codigo, usuario_id, ref_id, dias=1
        ):
            return False

        # Criar notificacao in-app
        canal_in_app = tipo.canal_in_app
        if pref:
            canal_in_app = pref.canal_in_app

        if canal_in_app:
            NotificacaoRepository.criar_notificacao(
                tipo=tipo,
                usuario_id=usuario_id,
                titulo=titulo,
                mensagem=mensagem,
                nivel=nivel,
                ref_modulo=ref_modulo,
                ref_id=ref_id,
                ref_url=ref_url,
            )
            resultado['criadas'] += 1

        # Despachar email (standby: controlado por EMAIL_ATIVO)
        if NotificationEngine.EMAIL_ATIVO:
            enviar_email = tipo.canal_email
            if pref:
                enviar_email = pref.canal_email
            if enviar_email and usuario.notificacoes_email and usuario.email:
                if nivel in ('alerta', 'critica') or enviar_email:
                    try:
                        sucesso = NotificationEngine._enviar_email(
                            usuario=usuario,
                            titulo=titulo,
                            mensagem=mensagem,
                            nivel=nivel,
                        )
                        if sucesso:
                            resultado['email_enviados'] += 1
                    except Exception as e:
                        resultado['erros'].append(f'Email erro ({usuario.email}): {e}')

        # Despachar telegram (standby: controlado por TELEGRAM_ATIVO)
        if NotificationEngine.TELEGRAM_ATIVO:
            enviar_telegram = tipo.canal_telegram
            if pref:
                enviar_telegram = pref.canal_telegram
            if enviar_telegram and usuario.notificacoes_telegram and usuario.telegram_chat_id:
                if nivel in ('alerta', 'critica') or enviar_telegram:
                    try:
                        sucesso = NotificationEngine._enviar_telegram(
                            chat_id=usuario.telegram_chat_id,
                            titulo=titulo,
                            mensagem=mensagem,
                            nivel=nivel,
                        )
                        if sucesso:
                            resultado['telegram_enviados'] += 1
                    except Exception as e:
                        resultado['erros'].append(
                            f'Telegram erro ({usuario.telegram_chat_id}): {e}'
                        )

        return True

    @staticmethod
    def _enviar_email(
        usuario: Usuario,
        titulo: str,
        mensagem: str,
        nivel: str,
    ) -> bool:
        """Envia email de notificacao."""
        from app.services.email_service import enviar_email_teste

        assunto = f'SGC - {titulo}'
        if nivel == 'critica':
            assunto = f'[URGENTE] SGC - {titulo}'

        corpo_html = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: #343990; color: white; padding: 16px 24px; border-radius: 8px 8px 0 0;">
                <h2 style="margin: 0; font-size: 18px;">SGC - Gestao de Contratos</h2>
            </div>
            <div style="padding: 24px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 8px 8px;">
                <h3 style="margin: 0 0 12px 0; color: #333;">{titulo}</h3>
                <p style="color: #555; line-height: 1.6;">{mensagem}</p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="color: #999; font-size: 12px;">
                    Esta e uma notificacao automatica do SGC. Nao responda este email.
                </p>
            </div>
        </div>
        """

        try:
            return enviar_email_teste(
                assunto=assunto,
                corpo_html=corpo_html,
                lista_destinatarios=[usuario.email],
            )
        except Exception as e:
            current_app.logger.error(f'Erro ao enviar email para {usuario.email}: {e}')
            return False

    @staticmethod
    def _enviar_telegram(
        chat_id: str,
        titulo: str,
        mensagem: str,
        nivel: str,
    ) -> bool:
        """Envia notificacao via Telegram Bot API."""
        from app.services.telegram_bot_service import TelegramBotService

        icone = {
            'silenciosa': '',
            'lembrete': '\U0001f4cb',
            'alerta': '\u26a0\ufe0f',
            'critica': '\U0001f6a8',
        }.get(nivel, '')

        texto = f"{icone} <b>{titulo}</b>\n\n{mensagem}"

        return TelegramBotService.enviar_mensagem(chat_id, texto)

    @staticmethod
    def resolver_destinatarios(
        tipo_codigo: str,
        codigo_contrato: Optional[str] = None,
        usuario_criador_id: Optional[int] = None,
    ) -> List[int]:
        """
        Resolve quem deve receber a notificacao baseado no tipo.

        Returns:
            Lista de usuario_ids unicos.
        """
        from sqlalchemy import text
        from app.models.perfil import Perfil

        ids = set()
        tipo = NotificacaoTipo.query.filter_by(codigo=tipo_codigo).first()
        if not tipo:
            return []

        modulo = tipo.modulo

        # 1. Incluir usuario criador (se fornecido)
        if usuario_criador_id:
            ids.add(usuario_criador_id)

        # 2. Fiscais do contrato (se fornecido)
        if codigo_contrato:
            try:
                sql = text("""
                    SELECT u.id
                    FROM sgc.fiscais_contrato fc
                    JOIN sgc.sis_usuarios u ON (
                        u.email = fc.email OR u.telefone = fc.telefone
                    )
                    WHERE fc.codigo_contrato = :cod
                    AND u.ativo = 1
                """)
                result = db.session.execute(sql, {'cod': codigo_contrato}).fetchall()
                for row in result:
                    ids.add(row[0])
            except Exception as e:
                current_app.logger.warning(f'Erro ao buscar fiscais: {e}')

        # 3. Usuarios com permissao no modulo
        try:
            usuarios_modulo = Usuario.query.join(
                Perfil, Usuario.perfil_id == Perfil.id
            ).join(
                PerfilPermissao, PerfilPermissao.perfil_id == Perfil.id
            ).filter(
                PerfilPermissao.modulo == modulo,
                PerfilPermissao.acao == 'visualizar',
                Usuario.ativo == True,
            ).all()

            for u in usuarios_modulo:
                ids.add(u.id)
        except Exception as e:
            current_app.logger.warning(f'Erro ao buscar usuarios do modulo: {e}')

        # 4. Admins (para notificacoes criticas)
        if tipo.nivel == 'critica':
            try:
                admins = Usuario.query.filter_by(is_admin=True, ativo=True).all()
                for a in admins:
                    ids.add(a.id)
            except Exception as e:
                current_app.logger.warning(f'Erro ao buscar admins: {e}')

        return list(ids)
