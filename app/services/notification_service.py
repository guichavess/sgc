"""
Serviço de Notificações - Email e Telegram.
"""
import os
import asyncio
from typing import List, Optional
from flask import current_app

from app.models import Solicitacao
from app.extensions import db


class NotificationService:
    """Serviço para envio de notificações."""

    @staticmethod
    def enviar_telegram(
        destinatarios: List[str],
        mensagem: str
    ) -> bool:
        """
        Envia mensagem via Telegram.

        Args:
            destinatarios: Lista de usernames ou IDs do Telegram
            mensagem: Mensagem a enviar

        Returns:
            True se enviou com sucesso
        """
        try:
            from telethon import TelegramClient

            api_id = int(os.getenv('TELEGRAM_API_ID', '0'))
            api_hash = os.getenv('TELEGRAM_API_HASH', '')
            session_path = os.getenv(
                'TELEGRAM_SESSION_PATH',
                os.path.join(os.getcwd(), 'api_telegram', 'minha_sessao')
            )

            if not api_id or not api_hash:
                current_app.logger.warning("Credenciais Telegram não configuradas")
                return False

            async def _envio():
                async with TelegramClient(session_path, api_id, api_hash) as client:
                    for dest in destinatarios:
                        try:
                            await client.send_message(dest, mensagem)
                        except Exception as e:
                            current_app.logger.error(
                                f"Erro ao enviar Telegram para {dest}: {e}"
                            )

            asyncio.run(_envio())
            return True

        except Exception as e:
            current_app.logger.error(f"Erro no serviço Telegram: {e}")
            return False

    @staticmethod
    def enviar_email(
        destinatario: str,
        assunto: str,
        corpo: str,
        html: bool = False
    ) -> bool:
        """
        Envia email.

        Args:
            destinatario: Email do destinatário
            assunto: Assunto do email
            corpo: Corpo do email
            html: Se True, envia como HTML

        Returns:
            True se enviou com sucesso
        """
        try:
            from app.services.email_service import enviar_email_teste

            # Usa o serviço de email existente
            resultado = enviar_email_teste(
                destinatario=destinatario,
                assunto=assunto,
                corpo=corpo
            )

            return resultado.get('sucesso', False)

        except Exception as e:
            current_app.logger.error(f"Erro ao enviar email: {e}")
            return False

    @staticmethod
    def notificar_fiscais(
        solicitacao: Solicitacao,
        tipo_notificacao: str = 'nova_solicitacao'
    ) -> dict:
        """
        Notifica os fiscais do contrato sobre uma solicitação.

        Args:
            solicitacao: Objeto da solicitação
            tipo_notificacao: Tipo de notificação

        Returns:
            Resultado das notificações
        """
        from sqlalchemy import text

        resultados = {
            'telegram': {'enviados': 0, 'erros': 0},
            'email': {'enviados': 0, 'erros': 0}
        }

        try:
            # Busca fiscais do contrato
            sql = text("""
                SELECT nome, telefone, email
                FROM sgc.fiscais_contrato
                WHERE codigo_contrato = :cod
                AND (telefone IS NOT NULL OR email IS NOT NULL)
            """)

            fiscais = db.session.execute(
                sql,
                {'cod': solicitacao.codigo_contrato}
            ).fetchall()

            if not fiscais:
                return resultados

            # Monta mensagem
            mensagens = {
                'nova_solicitacao': f"""
📋 *Nova Solicitação de Pagamento*

Contrato: {solicitacao.codigo_contrato}
Competência: {solicitacao.competencia}
Protocolo: {solicitacao.protocolo_gerado_sei or 'Pendente'}

Por favor, verifique a documentação necessária.
                """.strip(),

                'documentacao_pendente': f"""
⚠️ *Documentação Pendente*

A solicitação do contrato {solicitacao.codigo_contrato}
está aguardando documentação.

Competência: {solicitacao.competencia}
                """.strip()
            }

            mensagem = mensagens.get(
                tipo_notificacao,
                mensagens['nova_solicitacao']
            )

            # Envia notificações
            for fiscal in fiscais:
                nome, telefone, email = fiscal

                # Telegram
                if telefone:
                    try:
                        if NotificationService.enviar_telegram([telefone], mensagem):
                            resultados['telegram']['enviados'] += 1
                        else:
                            resultados['telegram']['erros'] += 1
                    except Exception:
                        resultados['telegram']['erros'] += 1

                # Email
                if email and '@' in email:
                    try:
                        assunto = f"SGC - {tipo_notificacao.replace('_', ' ').title()}"
                        if NotificationService.enviar_email(email, assunto, mensagem):
                            resultados['email']['enviados'] += 1
                        else:
                            resultados['email']['erros'] += 1
                    except Exception:
                        resultados['email']['erros'] += 1

        except Exception as e:
            current_app.logger.error(f"Erro ao notificar fiscais: {e}")

        return resultados

    @staticmethod
    def notificar_mudanca_etapa(
        solicitacao: Solicitacao,
        etapa_anterior: str,
        etapa_nova: str
    ) -> bool:
        """
        Notifica sobre mudança de etapa.

        Args:
            solicitacao: Objeto da solicitação
            etapa_anterior: Nome da etapa anterior
            etapa_nova: Nome da nova etapa

        Returns:
            True se notificou com sucesso
        """
        mensagem = f"""
🔄 *Atualização de Etapa*

Contrato: {solicitacao.codigo_contrato}
Competência: {solicitacao.competencia}

Etapa anterior: {etapa_anterior}
Nova etapa: {etapa_nova}
        """.strip()

        try:
            NotificationService.notificar_fiscais(
                solicitacao,
                tipo_notificacao='atualizacao_etapa'
            )
            return True
        except Exception as e:
            current_app.logger.error(f"Erro ao notificar mudança de etapa: {e}")
            return False
