"""
Renovacao automatica do token SEI do usuario.

O token SEI expira apos ~30 minutos. A sessao Flask (filesystem) persiste
independentemente, entao o usuario continua autenticado no sistema mas as
operacoes SEI falham com 401.

Solucao: durante o login, as credenciais sao armazenadas na sessao
servidor-side (nunca vao para o navegador — so o cookie de sessao ID).
Um before_request hook verifica a idade do token e renova
transparentemente antes que expire.

Configuracao via .env:
    SEI_TOKEN_MAX_AGE=1500   (segundos, default 25 min)
"""
import os
import time

import requests
import urllib3
from flask import session, current_app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Idade maxima do token em segundos antes de renovar.
# O SEI geralmente expira em ~30 min; renovamos com margem de seguranca.
SEI_TOKEN_MAX_AGE = int(os.getenv('SEI_TOKEN_MAX_AGE', '1500'))

_SEI_LOGIN_URL = "https://api.sei.pi.gov.br/v1/orgaos/usuarios/login"


def token_precisa_renovar():
    """Retorna True se o token SEI da sessao esta expirado ou ausente."""
    token = session.get('sei_token')
    if not token:
        return True
    token_time = session.get('sei_token_time', 0)
    return (time.time() - token_time) >= SEI_TOKEN_MAX_AGE


def renovar_token_sessao():
    """
    Renova o token SEI usando credenciais armazenadas na sessao.

    As credenciais (_sei_user, _sei_pwd, _sei_orgao) sao gravadas durante
    o login em auth/routes.py e ficam apenas no servidor (filesystem session).

    Returns:
        str: Novo token ou None se falhar.
    """
    usuario = session.get('_sei_user')
    senha = session.get('_sei_pwd')
    orgao = session.get('_sei_orgao', 'SEAD-PI')

    if not usuario or not senha:
        return None

    payload = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        resp = requests.post(
            _SEI_LOGIN_URL, json=payload, headers=headers,
            timeout=30, verify=False,
        )

        if resp.status_code != 200:
            current_app.logger.warning(
                f"[SEI_TOKEN] Renovacao falhou — status {resp.status_code}"
            )
            return None

        dados = resp.json()
        token = (
            dados.get('Token')
            or dados.get('token')
            or dados.get('IdSession')
        )

        if not token:
            current_app.logger.warning(
                "[SEI_TOKEN] Resposta SEI sem token na renovacao."
            )
            return None

        # Atualiza sessao
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

        current_app.logger.info("[SEI_TOKEN] Token do usuario renovado com sucesso.")
        return token

    except requests.exceptions.Timeout:
        current_app.logger.warning("[SEI_TOKEN] Timeout na renovacao do token.")
        return None
    except Exception as e:
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
