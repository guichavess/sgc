"""
Autenticacao SEI — token admin com cache e autenticacao de usuario.

O token SEI expira a meia-noite (00:00 Brasilia) do dia do login.

gerar_token_sei_admin() e chamada em ~40+ pontos do codigo (diarias,
financeiro, CGFR). Sem cache, cada chamada fazia um POST sincrono a
API SEI. Com o cache abaixo, o token e obtido 1 vez por dia e servido
da memoria ate meia-noite de Brasilia.

Mecanismos de protecao (mesmos do sei_token.py para o token de usuario):
- **Cache**: token armazenado em variavel de modulo, por processo/worker.
- **Coalescencia**: threads concorrentes esperam e reusam o resultado
  de uma unica chamada a API (double-check locking).
- **Backoff**: apos uma falha, novas tentativas sao suprimidas por
  SEI_ADMIN_RETRY_BACKOFF segundos.
"""
import os
import threading
import time
from datetime import datetime, timedelta

import requests
import urllib3
from flask import current_app

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_SEI_LOGIN_URL = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"

# Janela de silencio apos uma falha na renovacao do token admin (segundos).
SEI_ADMIN_RETRY_BACKOFF = int(os.getenv('SEI_TOKEN_RETRY_BACKOFF', '60'))

# Margem de seguranca antes da meia-noite (segundos).
# Token expira a meia-noite; invalidamos o cache 5 min antes.
SEI_ADMIN_MARGEM = int(os.getenv('SEI_TOKEN_MARGEM', '300'))

# ── Estado compartilhado entre threads do worker ──────────────────────────────
_admin_lock = threading.Lock()
_admin_token = None
_admin_obtido_em = 0       # timestamp UNIX de quando o token foi obtido
_admin_falha_em = 0        # timestamp UNIX da ultima falha


def limpar_cache_admin():
    """Zera cache e backoff do token admin. Usado pelos testes."""
    global _admin_token, _admin_obtido_em, _admin_falha_em
    with _admin_lock:
        _admin_token = None
        _admin_obtido_em = 0
        _admin_falha_em = 0


def _meia_noite_brasilia(referencia_ts=None):
    """Retorna o timestamp UNIX da meia-noite de Brasilia do dia de referencia.

    Se referencia_ts nao for fornecido, usa o momento atual.
    A meia-noite retornada e a do *fim* do dia (00:00 do dia seguinte).
    """
    tz = ZoneInfo("America/Sao_Paulo")
    if referencia_ts:
        dt = datetime.fromtimestamp(referencia_ts, tz=tz)
    else:
        dt = datetime.now(tz)
    meia_noite = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return meia_noite.timestamp()


def _admin_token_valido():
    """True se o token admin em cache ainda e valido (antes da meia-noite)."""
    if not _admin_token or not _admin_obtido_em:
        return False
    expira_em = _meia_noite_brasilia(_admin_obtido_em) - SEI_ADMIN_MARGEM
    return time.time() < expira_em


def _admin_em_backoff():
    """True se a ultima renovacao falhou dentro da janela de backoff."""
    if not _admin_falha_em:
        return False
    return (time.time() - _admin_falha_em) < SEI_ADMIN_RETRY_BACKOFF


def gerar_token_sei_admin():
    """
    Gera um token de acesso ao SEI utilizando credenciais administrativas.

    O token e cacheado em memoria ate meia-noite de Brasilia (menos margem
    de seguranca). Chamadas concorrentes de threads diferentes esperam e
    reusam o resultado de uma unica chamada a API (coalescencia).

    Apos uma falha, novas tentativas sao suprimidas por
    SEI_ADMIN_RETRY_BACKOFF segundos (backoff).

    Returns:
        str: Token SEI ou None em caso de falha.
    """
    global _admin_token, _admin_obtido_em, _admin_falha_em

    # 1. Cache valido? Retorna direto (leitura sem lock — otimista)
    if _admin_token_valido():
        return _admin_token

    # 2. Em backoff apos falha? Retorna None sem tocar a rede
    if _admin_em_backoff():
        current_app.logger.debug(
            "[SEI_AUTH] Token admin em backoff — suprimindo nova tentativa."
        )
        return None

    # 3. Credenciais disponiveis?
    usuario = os.getenv("SEI_USER") or current_app.config.get("SEI_USER")
    senha = os.getenv("SEI_PASSWORD") or current_app.config.get("SEI_PASSWORD")
    orgao = os.getenv("SEI_ORGAO", "SEAD-PI")

    if not usuario or not senha:
        current_app.logger.error(
            "[SEI_AUTH] Credenciais (SEI_USER/SEI_PASSWORD) nao encontradas."
        )
        return None

    # 4. Lock + double-check + chamada API
    with _admin_lock:
        # Re-check: outra thread pode ter renovado enquanto esperavamos o lock
        if _admin_token_valido():
            return _admin_token

        if _admin_em_backoff():
            return None

        return _renovar_token_admin(usuario, senha, orgao)


def _renovar_token_admin(usuario, senha, orgao):
    """Chamada efetiva ao SEI. Sempre executada sob _admin_lock."""
    global _admin_token, _admin_obtido_em, _admin_falha_em

    payload = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        response = requests.post(
            _SEI_LOGIN_URL, json=payload, headers=headers,
            timeout=30, verify=False,
        )

        if response.status_code == 200:
            dados = response.json()

            token = (
                dados.get('IdSession')
                or dados.get('token')
                or dados.get('Token')
            )

            if not token:
                token = (
                    response.headers.get('token')
                    or response.headers.get('Token')
                )

            if token:
                _admin_token = token
                _admin_obtido_em = time.time()
                _admin_falha_em = 0
                current_app.logger.info(
                    "[SEI_AUTH] Token admin renovado com sucesso. "
                    f"Valido ate meia-noite de Brasilia."
                )
                return token
            else:
                _admin_falha_em = time.time()
                current_app.logger.error(
                    f"[SEI_AUTH] Token nao encontrado na resposta. Dados: {dados}"
                )
                return None
        else:
            _admin_falha_em = time.time()
            current_app.logger.error(
                f"[SEI_AUTH] Renovacao falhou — status {response.status_code}. "
                f"Novas tentativas suspensas por {SEI_ADMIN_RETRY_BACKOFF}s."
            )
            return None

    except requests.exceptions.Timeout:
        _admin_falha_em = time.time()
        current_app.logger.warning(
            "[SEI_AUTH] Timeout na renovacao do token admin."
        )
        return None
    except Exception as e:
        _admin_falha_em = time.time()
        current_app.logger.error(f"[SEI_AUTH] Erro na renovacao: {e}")
        return None


def autenticar_usuario_sei(usuario, senha, protocolo_bypass=None):
    """
    Autentica um usuario especifico no SEI e retorna dados completos.

    Diferente de gerar_token_sei_admin() que so retorna o token,
    esta funcao retorna o dict completo com token, IdUsuario, IdLogin, etc.
    Usado para operacoes que precisam identificar o usuario (ex: assinatura).

    Bypass:
    - Se DIARIAS_BYPASS_ASSINATURAS=True (global), simula.
    - Se `protocolo_bypass` está em DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS,
      simula apenas para esse processo.

    Args:
        usuario: Login do usuario no SEI
        senha: Senha do usuario no SEI
        protocolo_bypass: protocolo SEI do processo (ex: '00002.003853/2026-21').
            Quando fornecido e na lista de bypass, retorna token fake
            sem chamar a API real do SEI.

    Returns:
        dict com {token, id_usuario, id_login, dados_completos} ou None em caso de erro
    """
    # ── Bypass para testes (global OU por protocolo específico) ──
    from app.constants import protocolo_tem_bypass_assinatura
    if protocolo_tem_bypass_assinatura(protocolo_bypass):
        current_app.logger.info(
            f"[BYPASS] Autenticação SEI simulada para usuario={usuario} "
            f"(processo={protocolo_bypass!r})"
        )
        return {
            'token': 'bypass-token',
            'id_usuario': 'bypass-user',
            'id_login': 'bypass-login',
            'nome': usuario,
            'cargo': '',
            'unidades': [],
            'dados_completos': {},
        }
    url_auth = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"
    orgao = os.getenv("SEI_ORGAO", "SEAD-PI")

    payload = {
        "Usuario": usuario,
        "Senha": senha,
        "Orgao": orgao,
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    try:
        response = requests.post(url_auth, json=payload, headers=headers, timeout=120, verify=False)

        if response.status_code == 200:
            dados = response.json()

            # Estrutura SEI: {"Token": "...", "Login": {"IdUsuario": ..., "IdLogin": ...}, ...}
            # Token fica na RAIZ do JSON (igual ao que o modulo pagamentos usa em auth/routes.py)
            login_data = dados.get('Login', dados)

            token = dados.get('Token') or dados.get('token') or dados.get('IdSession')
            if not token:
                token = login_data.get('IdSession') or login_data.get('token') or login_data.get('Token')
            if not token:
                token = response.headers.get('token') or response.headers.get('Token')

            if not token:
                current_app.logger.error(
                    f"SEI AUTH Usuario: Token nao encontrado. Dados: {dados}"
                )
                return None

            return {
                'token': token,
                'id_usuario': str(login_data.get('IdUsuario', '')),
                'id_login': str(login_data.get('IdLogin', '') or token),
                'nome': login_data.get('Nome', ''),
                'cargo': login_data.get('UltimoCargoAssinatura', ''),
                'id_unidade_atual': str(login_data.get('IdUnidadeAtual', '')),
                'unidades': dados.get('Unidades', []),
                'dados_completos': dados,
            }
        else:
            current_app.logger.error(
                f"SEI AUTH Usuario: Status {response.status_code}. Resposta: {response.text[:300]}"
            )
            return None

    except Exception as e:
        current_app.logger.error(f"SEI AUTH Usuario: Excecao: {str(e)}")
        return None