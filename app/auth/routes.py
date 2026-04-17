"""
Rotas de autenticação.
"""
import time
from datetime import datetime
from flask import Blueprint, request, render_template, redirect, url_for, session, flash
from flask_login import current_user, login_user

from app.extensions import db
from app.models import Usuario
from app.models.usuario import UsuarioUnidadeSei
from app.auth.services import realizar_login_sei


auth_bp = Blueprint('auth', __name__)

def sincronizar_usuario_local(dados_login, unidades=None):
    """
    Verifica se o usuário existe no banco local.
    Se não, cria. Se sim, atualiza timestamp, nome, cargo e unidade SEI.

    Args:
        dados_login: dict do objeto 'Login' da resposta SEI (contém IdUnidadeAtual).
        unidades: lista opcional de dicts da chave 'Unidades' (raíz da resposta SEI).
                  Usada para resolver a Sigla a partir do IdUnidadeAtual.
    """
    from app.utils.unidade_sei import extrair_superintendencia

    try:
        id_sei = dados_login.get('IdUsuario')
        nome = dados_login.get('Nome')
        sigla = dados_login.get('Sigla')
        # Captura o cargo do JSON de resposta (pode vir vazio, então usamos get)
        cargo_sei = dados_login.get('UltimoCargoAssinatura')

        # Unidade atual do usuário (fonte: Login.IdUnidadeAtual)
        id_unidade_atual = str(dados_login.get('IdUnidadeAtual', '') or '')
        sigla_unidade_atual = None
        super_sigla = None

        if unidades:
            # Tenta resolver pela IdUnidadeAtual; se não vier, cai para a 1ª unidade
            unidade_match = None
            if id_unidade_atual:
                unidade_match = next(
                    (u for u in unidades if str(u.get('Id', '')) == id_unidade_atual),
                    None,
                )
            if not unidade_match and unidades:
                unidade_match = unidades[0]
                id_unidade_atual = str(unidade_match.get('Id', '') or '')

            if unidade_match:
                sigla_unidade_atual = unidade_match.get('Sigla') or None
                super_sigla, _tipo = extrair_superintendencia(sigla_unidade_atual)

        if not id_sei:
            return None

        # Busca no banco
        usuario = Usuario.query.filter_by(id_usuario_sei=id_sei).first()

        if usuario:
            # Atualiza dados existentes
            usuario.ultimo_login = datetime.now()
            usuario.nome = nome
            usuario.sigla_login = sigla
            usuario.cargo = cargo_sei  # Atualiza o cargo
            if id_unidade_atual:
                usuario.unidade_sei_id = id_unidade_atual
            if sigla_unidade_atual:
                usuario.unidade_sei_sigla = sigla_unidade_atual
            if super_sigla:
                usuario.superintendencia_sigla = super_sigla
        else:
            # Cria novo usuário com cargo + unidade SEI
            usuario = Usuario(
                id_usuario_sei=id_sei,
                nome=nome,
                sigla_login=sigla,
                cargo=cargo_sei,
                ultimo_login=datetime.now(),
                unidade_sei_id=id_unidade_atual or None,
                unidade_sei_sigla=sigla_unidade_atual,
                superintendencia_sigla=super_sigla,
            )
            db.session.add(usuario)

        db.session.commit()
        return usuario.id
    except Exception as e:
        print(f"Erro ao sincronizar usuário: {e}")
        db.session.rollback()
        return None


def _sincronizar_unidades_sei(usuario_id, unidades_api):
    """Sincroniza as unidades SEI do usuário no banco (delete+insert)."""
    try:
        UsuarioUnidadeSei.query.filter_by(usuario_id=usuario_id).delete()
        for u in unidades_api:
            db.session.add(UsuarioUnidadeSei(
                usuario_id=usuario_id,
                unidade_sei_id=str(u.get('Id', '')),
                sigla=u.get('Sigla', ''),
                descricao=u.get('Descricao', ''),
            ))
        db.session.commit()
    except Exception as e:
        from flask import current_app
        current_app.logger.error(f"[AUTH] Erro ao sincronizar unidades SEI: {e}")
        db.session.rollback()


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('hub'))

    if request.method == 'POST':
        user = request.form.get('usuario')
        pwd = request.form.get('senha')
        orgao_escolha = request.form.get('orgao', 'SEAD')
        orgao = f"{orgao_escolha}-PI"

        dados_api = realizar_login_sei(user, pwd, orgao)

        if dados_api and "Login" in dados_api:
            login_data = dados_api['Login']
            unidades_api = dados_api.get('Unidades', [])
            id_interno = sincronizar_usuario_local(login_data, unidades=unidades_api)

            # Sincroniza unidades SEI do usuário no banco (1:N)
            if id_interno and unidades_api:
                _sincronizar_unidades_sei(id_interno, unidades_api)
            
            if not id_interno:
                flash('Erro ao registrar usuário no sistema local.', 'warning')
                return redirect(url_for('auth.login'))
            
            usuario_obj = Usuario.query.get(id_interno)
            login_user(usuario_obj)

            # Configuração da Sessão
            session['usuario_nome'] = login_data.get('Nome')
            session['usuario_sei_id'] = login_data.get('IdUsuario')
            session['usuario_db_id'] = id_interno
            session['sei_token'] = dados_api.get('Token')
            session['sei_token_time'] = time.time()
            session['usuario_sei_id_login'] = login_data.get('IdLogin')
            session['usuario_orgao'] = orgao
            session['usuario_cargo'] = login_data.get('UltimoCargoAssinatura')

            # Credenciais para renovacao automatica do token SEI.
            # Ficam apenas no servidor (filesystem session), nunca no navegador.
            session['_sei_user'] = user
            session['_sei_pwd'] = pwd
            session['_sei_orgao'] = orgao

            # Processamento de Unidades
            lista_unidades = []
            if "Unidades" in dados_api:
                for unidade in dados_api['Unidades']:
                    lista_unidades.append({'id': unidade.get('Id'), 'sigla': unidade.get('Sigla')})
            session['unidades'] = lista_unidades
            # Unidade atual: prioriza Login.IdUnidadeAtual (SEI); fallback para a 1ª
            id_unidade_atual_sei = login_data.get('IdUnidadeAtual')
            if id_unidade_atual_sei:
                session['unidade_atual_id'] = str(id_unidade_atual_sei)
            elif lista_unidades:
                session['unidade_atual_id'] = lista_unidades[0]['id']

            return redirect(url_for('hub'))
        else:
            flash('Falha no login. Verifique suas credenciais.', 'danger')
        
    return render_template('auth/login.html')



@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))