"""
Servico de envio de mensagens via Telegram Bot API.
Substitui o Telethon (conta pessoal) por Bot API (mais adequado para notificacoes).

Configuracao:
    1. Criar bot via @BotFather no Telegram
    2. Definir TELEGRAM_BOT_TOKEN no .env
    3. Usuarios enviam /start ao bot para obter seu chat_id
"""
import requests
from flask import current_app


class TelegramBotService:
    """Servico de envio via Telegram Bot API."""

    BOT_TOKEN = None
    BASE_URL = 'https://api.telegram.org/bot{token}'

    @classmethod
    def init_app(cls, app):
        """Inicializa com token do config."""
        cls.BOT_TOKEN = app.config.get('TELEGRAM_BOT_TOKEN', '')

    @classmethod
    def _get_url(cls, method: str) -> str:
        """Monta URL da API do Telegram."""
        token = cls.BOT_TOKEN or current_app.config.get('TELEGRAM_BOT_TOKEN', '')
        return f'https://api.telegram.org/bot{token}/{method}'

    @classmethod
    def enviar_mensagem(
        cls,
        chat_id: str,
        texto: str,
        parse_mode: str = 'HTML'
    ) -> bool:
        """
        Envia mensagem via Telegram Bot API.

        Args:
            chat_id: ID do chat do destinatario
            texto: Mensagem a enviar (suporta HTML)
            parse_mode: Formato da mensagem (HTML ou MarkdownV2)

        Returns:
            True se enviou com sucesso
        """
        token = cls.BOT_TOKEN or current_app.config.get('TELEGRAM_BOT_TOKEN', '')
        if not token:
            current_app.logger.warning('TELEGRAM_BOT_TOKEN nao configurado')
            return False

        if not chat_id:
            return False

        try:
            response = requests.post(
                cls._get_url('sendMessage'),
                json={
                    'chat_id': chat_id,
                    'text': texto,
                    'parse_mode': parse_mode,
                },
                timeout=10,
            )
            data = response.json()

            if not data.get('ok'):
                current_app.logger.error(
                    f'Erro Telegram para chat_id={chat_id}: {data.get("description")}'
                )
                return False

            return True

        except requests.exceptions.Timeout:
            current_app.logger.error(f'Timeout ao enviar Telegram para {chat_id}')
            return False
        except Exception as e:
            current_app.logger.error(f'Erro ao enviar Telegram: {e}')
            return False

    @classmethod
    def verificar_chat_id(cls, chat_id: str) -> bool:
        """Verifica se um chat_id e valido tentando getChat."""
        token = cls.BOT_TOKEN or current_app.config.get('TELEGRAM_BOT_TOKEN', '')
        if not token or not chat_id:
            return False

        try:
            response = requests.post(
                cls._get_url('getChat'),
                json={'chat_id': chat_id},
                timeout=10,
            )
            data = response.json()
            return data.get('ok', False)
        except Exception:
            return False
