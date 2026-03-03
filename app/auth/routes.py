"""
Rotas de autenticação.
"""
from datetime import datetime
from flask import Blueprint, request, render_template, redirect, url_for, session, flash
from flask_login import current_user, login_user

from app.extensions import db
from app.models import Usuario
from app.auth.services import realizar_login_sei


auth_bp = Blueprint('auth', __name__)

def sincronizar_usuario_local(dados_login):
    """
    Verifica se o usuário existe no banco local.
    Se não, cria. Se sim, atualiza timestamp, nome e CARGO.
    """
    try:
        id_sei = dados_login.get('IdUsuario')
        nome = dados_login.get('Nome')
        sigla = dados_login.get('Sigla') 
        # Captura o cargo do JSON de resposta (pode vir vazio, então usamos get)
        cargo_sei = dados_login.get('UltimoCargoAssinatura') 
        
        if not id_sei:
            return None

        # Busca no banco
        usuario = Usuario.query.filter_by(id_usuario_sei=id_sei).first()

        if usuario:
            # Atualiza dados existentes
            usuario.ultimo_login = datetime.now()
            usuario.nome = nome 
            usuario.sigla_login = sigla
            usuario.cargo = cargo_sei # Atualiza o cargo
        else:
            # Cria novo usuário com cargo
            usuario = Usuario(
                id_usuario_sei=id_sei,
                nome=nome,
                sigla_login=sigla,
                cargo=cargo_sei, # Insere o cargo
                ultimo_login=datetime.now()
            )
            db.session.add(usuario)
        
        db.session.commit()
        return usuario.id
    except Exception as e:
        print(f"Erro ao sincronizar usuário: {e}")
        db.session.rollback()
        return None


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('hub'))

    if request.method == 'POST':
        user = request.form.get('usuario')
        pwd = request.form.get('senha')
        orgao = "SEAD-PI" 

        dados_api = realizar_login_sei(user, pwd, orgao)

        if dados_api and "Login" in dados_api:
            login_data = dados_api['Login']
            id_interno = sincronizar_usuario_local(login_data)
            
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
            session['usuario_sei_id_login'] = login_data.get('IdLogin')
            session['usuario_orgao'] = orgao
            session['usuario_cargo'] = login_data.get('UltimoCargoAssinatura') 

            # Processamento de Unidades
            lista_unidades = []
            if "Unidades" in dados_api:
                for unidade in dados_api['Unidades']:
                    lista_unidades.append({'id': unidade.get('Id'), 'sigla': unidade.get('Sigla')})
            session['unidades'] = lista_unidades
            if lista_unidades:
                session['unidade_atual_id'] = lista_unidades[0]['id']

            return redirect(url_for('hub'))
        else:
            flash('Falha no login. Verifique suas credenciais.', 'danger')
        
    return render_template('auth/login.html')



@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))