"""
Renovacao automatica do token SEI do usuario.

O token SEI expira a meia-noite (00:00 Brasilia) do dia do login.
A sessao Flask (filesystem) persiste independentemente, entao o usuario
continua autenticado no sistema mas as operacoes SEI falham com 401.

Solucao: durante o login, as credenciais sao armazenadas na sessao
servidor-side (nunca vao para o navegador — so o cookie de sessao ID).
Um before_request hook verifica se o token ja expirou (passou da
meia-noite) e renova transparentemente.

Na pratica, o hook roda em TODA requisicao autenticada mas a checagem
e barata (~1 microsegundo comparando timestamps). A renovacao so
acontece 1 vez por dia (apos meia-noite de Brasilia).

Mecanismos de protecao (ver tests/services/test_sei_token.py):
- **Backoff**: apos uma falha, novas tentativas do mesmo usuario sao
  puladas por SEI_TOKEN_RETRY_BACKOFF segundos.
- **Coalescencia**: requisicoes concorrentes do mesmo usuario compartilham
  uma unica chamada a API, em vez de uma por requisicao.

Configuracao via .env:
    SEI_TOKEN_MARGEM=300          (segundos antes da meia-noite, default 5 min)
    SEI_TOKEN_RETRY_BACKOFF=60    (segundos, default 60s)
"""
import os
import threading
import time
from datetime import datetime, timedelta

import requests
import urllib3
from flask import session, current_app

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Margem de seguranca antes da meia-noite (segundos).
# O token expira a meia-noite; renovamos com essa margem antes.
SEI_TOKEN_MARGEM = int(os.getenv('SEI_TOKEN_MARGEM', '300'))

# Janela de silencio apos uma renovacao falha, por usuario.
SEI_TOKEN_RETRY_BACKOFF = int(os.getenv('SEI_TOKEN_RETRY_BACKOFF', '60'))

_SEI_LOGIN_URL = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"

# Timezone de Brasilia (usado para calcular meia-noite)
_TZ_BRASILIA = ZoneInfo("America/Sao_Paulo")

# ── Estado compartilhado entre threads do worker ──────────────────────────────
# Chaveado pelo login SEI. Vive no processo: com varios workers gunicorn cada
# um mantem o seu, o que apenas reduz o ganho — nao afeta a correcao.
_estado_lock = threading.Lock()
_locks_usuario: dict = {}   # login -> threading.Lock (serializa renovacoes)
_cache_token: dict = {}     # login -> (token, dados, obtido_em)
_ultima_falha: dict = {}    # login -> timestamp da ultima falha


def limpar_estado():
    """Zera cache/backoff/locks. Usado pelos testes para isolar cada caso."""
    with _estado_lock:
        _locks_usuario.clear()
        _cache_token.clear()
        _ultima_falha.clear()


def _meia_noite_brasilia(referencia_ts=None):
    """Retorna o timestamp UNIX da meia-noite de Brasilia do dia de referencia.

    Se referencia_ts nao for fornecido, usa o momento atual.
    A meia-noite retornada e a do *fim* do dia (00:00 do dia seguinte).
    """
    if referencia_ts:
        dt = datetime.fromtimestamp(referencia_ts, tz=_TZ_BRASILIA)
    else:
        dt = datetime.now(_TZ_BRASILIA)
    meia_noite = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return meia_noite.timestamp()


def _token_expirou_meia_noite(obtido_em):
    """Retorna True se o token obtido em obtido_em ja expirou (passou da meia-noite)."""
    if not obtido_em:
        return True
    expira_em = _meia_noite_brasilia(obtido_em) - SEI_TOKEN_MARGEM
    return time.time() >= expira_em


def esta_em_backoff(login):
    """True se a ultima renovacao desse login falhou dentro da janela de backoff."""
    with _estado_lock:
        falha_em = _ultima_falha.get(login)
    if not falha_em:
        return False
    return (time.time() - falha_em) < SEI_TOKEN_RETRY_BACKOFF


def _lock_do_usuario(login):
    """Lock dedicado ao login — renovacoes de usuarios distintos nao se bloqueiam."""
    with _estado_lock:
        lock = _locks_usuario.get(login)
        if lock is None:
            lock = threading.Lock()
            _locks_usuario[login] = lock
        return lock


def _token_em_cache(login):
    """Retorna (token, dados) se houver token valido para o login, senao None."""
    with _estado_lock:
        entrada = _cache_token.get(login)
    if not entrada:
        return None
    token, dados, obtido_em = entrada
    if _token_expirou_meia_noite(obtido_em):
        return None
    return token, dados


def _registrar_sucesso(login, token, dados):
    with _estado_lock:
        _cache_token[login] = (token, dados, time.time())
        _ultima_falha.pop(login, None)


def _registrar_falha(login):
    with _estado_lock:
        _ultima_falha[login] = time.time()


def _aplicar_na_sessao(token, dados):
    """Grava token e dados derivados na sessao da requisicao atual."""
    session['sei_token'] = token
    session['sei_token_time'] = time.time()

    # Atualiza unidades (podem mudar entre sessoes)
    if 'Unidades' in dados:
        session['unidades'] = [
            {'id': u.get('Id'), 'sigla': u.get('Sigla')}
            for u in dados['Unidades']
        ]

    # Atualiza unidade atual se veio no Login
    login_data = dados.get('Login')
    if login_data:
        id_unidade = login_data.get('IdUnidadeAtual')
        if id_unidade:
            session['unidade_atual_id'] = str(id_unidade)


def token_precisa_renovar():
    """Retorna True se o token SEI da sessao esta expirado ou ausente.

    O token expira a meia-noite de Brasilia do dia do login.
    """
    token = session.get('sei_token')
    if not token:
        return True
    token_time = session.get('sei_token_time', 0)
    return _token_expirou_meia_noite(token_time)


def renovar_token_sessao():
    """
    Renova o token SEI usando credenciais armazenadas na sessao.

    As credenciais (_sei_user, _sei_pwd, _sei_orgao) sao gravadas durante
    o login em auth/routes.py e ficam apenas no servidor (filesystem session).

    Nao chama a API quando ja existe token recente para o login (cache) ou
    quando a ultima tentativa falhou ha pouco (backoff).

    Returns:
        str: Novo token ou None se falhar.
    """
    usuario = session.get('_sei_user')
    senha = session.get('_sei_pwd')
    orgao = session.get('_sei_orgao', 'SEAD-PI')

    if not usuario or not senha:
        return None

    # Outra requisicao ja renovou ha pouco — aproveita sem tocar a rede.
    em_cache = _token_em_cache(usuario)
    if em_cache:
        token, dados = em_cache
        _aplicar_na_sessao(token, dados)
        return token

    if esta_em_backoff(usuario):
        return None

    # Serializa por usuario: quem chegar depois espera e reaproveita o resultado.
    with _lock_do_usuario(usuario):
        em_cache = _token_em_cache(usuario)
        if em_cache:
            token, dados = em_cache
            _aplicar_na_sessao(token, dados)
            return token

        # A thread que segurava o lock pode ter acabado de falhar.
        if esta_em_backoff(usuario):
            return None

        return _renovar_via_api(usuario, senha, orgao)


def _renovar_via_api(usuario, senha, orgao):
    """Chamada efetiva ao SEI. Sempre executada sob o lock do usuario."""
    payload = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        resp = requests.post(
            _SEI_LOGIN_URL, json=payload, headers=headers,
            timeout=30, verify=False,
        )

        if resp.status_code != 200:
            _registrar_falha(usuario)
            current_app.logger.warning(
                f"[SEI_TOKEN] Renovacao falhou — status {resp.status_code}. "
                f"Novas tentativas suspensas por {SEI_TOKEN_RETRY_BACKOFF}s."
            )
            return None

        dados = resp.json()
        token = (
            dados.get('Token')
            or dados.get('token')
            or dados.get('IdSession')
        )

        if not token:
            _registrar_falha(usuario)
            current_app.logger.warning(
                "[SEI_TOKEN] Resposta SEI sem token na renovacao."
            )
            return None

        _registrar_sucesso(usuario, token, dados)
        _aplicar_na_sessao(token, dados)

        current_app.logger.info(
            "[SEI_TOKEN] Token do usuario renovado com sucesso. "
            "Valido ate meia-noite de Brasilia."
        )
        return token

    except requests.exceptions.Timeout:
        _registrar_falha(usuario)
        current_app.logger.warning("[SEI_TOKEN] Timeout na renovacao do token.")
        return None
    except Exception as e:
        _registrar_falha(usuario)
        current_app.logger.error(f"[SEI_TOKEN] Erro na renovacao: {e}")
        return None


def obter_token_sei():
    """
    Retorna um token SEI valido para o usuario logado.

    1. Se o token na sessao e recente, retorna direto.
    2. Se expirou, renova usando credenciais da sessao.
    3. Se renovacao falhar, usa token admin como fallback.

    Returns:
        str: Token SEI valido ou None.
    """
    if not token_precisa_renovar():
        return session.get('sei_token')

    novo = renovar_token_sessao()
    if novo:
        return novo

    # Fallback: token admin (perde contexto do usuario, mas operacao nao falha)
    from app.services.sei_auth import gerar_token_sei_admin
    current_app.logger.warning(
        "[SEI_TOKEN] Usando token admin como fallback."
    )
    return gerar_token_sei_admin()
