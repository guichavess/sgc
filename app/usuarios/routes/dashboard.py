"""
Rotas de Usuários - Listagem e edição de usuários.
Acesso restrito a administradores (is_admin=True).
"""
from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required

from app.usuarios.routes import usuarios_bp
from app.services.usuario_service import UsuarioService
from app.utils.permissions import requires_admin


@usuarios_bp.route('/')
@login_required
@requires_admin
def dashboard():
    """Página principal - Lista de usuários com filtros e paginação."""
    filtro_nome = request.args.get('nome', '').strip()
    filtro_perfil = request.args.get('perfil_id', type=int) or None
    filtro_ativo = request.args.get('ativo', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 20

    # Converte filtro_ativo para bool
    ativo = None
    if filtro_ativo == '1':
        ativo = True
    elif filtro_ativo == '0':
        ativo = False

    pagination = UsuarioService.listar_usuarios_paginado(
        nome=filtro_nome or None,
        perfil_id=filtro_perfil,
        ativo=ativo,
        page=page,
        per_page=per_page
    )

    perfis = UsuarioService.listar_perfis()

    tem_filtro = any([filtro_nome, filtro_perfil, filtro_ativo])

    # Pré-carrega permissões de cada usuário para exibir badges na tabela
    modulos = UsuarioService.get_modulos()
    permissoes_por_usuario = {}
    for usuario in pagination.items:
        permissoes_por_usuario[usuario.id] = UsuarioService.obter_permissoes_usuario(usuario)

    return render_template(
        'usuarios/dashboard.html',
        usuarios=pagination.items,
        pagination=pagination,
        perfis=perfis,
        modulos=modulos,
        permissoes_por_usuario=permissoes_por_usuario,
        filtro_nome=filtro_nome,
        filtro_perfil=filtro_perfil,
        filtro_ativo=filtro_ativo,
        tem_filtro=tem_filtro
    )


@usuarios_bp.route('/<int:usuario_id>/editar', methods=['GET', 'POST'])
@login_required
@requires_admin
def editar_usuario(usuario_id):
    """Edição de status, permissões e preferencias de notificacao de um usuário."""
    from app.models.notificacao import NotificacaoTipo, NotificacaoPreferencia
    from app.extensions import db

    usuario = UsuarioService.buscar_usuario(usuario_id)
    if not usuario:
        flash('Usuário não encontrado.', 'danger')
        return redirect(url_for('usuarios.dashboard'))

    if request.method == 'POST':
        ativo = request.form.get('ativo') == '1'

        # Extrai permissões dos checkboxes (perm_modulo_acao)
        permissoes = _extrair_permissoes_form(request.form)

        try:
            UsuarioService.atualizar_usuario(
                usuario_id=usuario_id,
                ativo=ativo,
                permissoes=permissoes
            )

            # Salva preferencias de notificacao
            _salvar_preferencias_notificacao(usuario_id, request.form, db)

            flash('Usuário atualizado com sucesso!', 'success')
            return redirect(url_for('usuarios.dashboard'))
        except ValueError as e:
            flash(str(e), 'danger')

    modulos = UsuarioService.get_modulos()
    acoes = UsuarioService.get_acoes()
    permissoes_atuais = UsuarioService.obter_permissoes_usuario(usuario)

    # Carregar tipos de notificacao e preferencias do usuario
    notif_tipos = _carregar_preferencias_notificacao(usuario_id)

    return render_template(
        'usuarios/editar_usuario.html',
        usuario=usuario,
        modulos=modulos,
        acoes=acoes,
        permissoes_atuais=permissoes_atuais,
        notif_tipos=notif_tipos,
    )


def _extrair_permissoes_form(form):
    """Extrai permissões dos checkboxes do formulário.

    Formato dos checkboxes: name="perm_{modulo}_{acao}" value="1"
    """
    permissoes = []
    for key in form:
        if key.startswith('perm_'):
            partes = key[5:].rsplit('_', 1)
            if len(partes) == 2:
                modulo, acao = partes
                permissoes.append({'modulo': modulo, 'acao': acao})
    return permissoes


def _carregar_preferencias_notificacao(usuario_id):
    """Carrega tipos de notificacao com preferencias do usuario para exibicao."""
    from app.models.notificacao import NotificacaoTipo, NotificacaoPreferencia

    tipos = NotificacaoTipo.query.filter_by(ativo=True).order_by(
        NotificacaoTipo.modulo, NotificacaoTipo.nome
    ).all()

    prefs = NotificacaoPreferencia.query.filter_by(
        usuario_id=usuario_id
    ).all()
    prefs_map = {p.tipo_id: p for p in prefs}

    resultado = []
    for t in tipos:
        pref = prefs_map.get(t.id)
        resultado.append({
            'tipo': t,
            'canal_in_app': pref.canal_in_app if pref else t.canal_in_app,
            'silenciado': pref.silenciado if pref else False,
        })
    return resultado


def _salvar_preferencias_notificacao(usuario_id, form, db):
    """Salva preferencias de notificacao a partir do formulario.

    Formato dos checkboxes:
        name="notif_{tipo_id}_in_app" value="1"
        name="notif_{tipo_id}_silenciado" value="1"
    """
    from app.models.notificacao import NotificacaoTipo, NotificacaoPreferencia

    tipos = NotificacaoTipo.query.filter_by(ativo=True).all()

    for tipo in tipos:
        in_app = form.get(f'notif_{tipo.id}_in_app') == '1'
        silenciado = form.get(f'notif_{tipo.id}_silenciado') == '1'

        pref = NotificacaoPreferencia.query.filter_by(
            usuario_id=usuario_id, tipo_id=tipo.id
        ).first()

        # So cria/atualiza se diferente do padrao do tipo
        if in_app != tipo.canal_in_app or silenciado:
            if not pref:
                pref = NotificacaoPreferencia(
                    usuario_id=usuario_id,
                    tipo_id=tipo.id,
                )
                db.session.add(pref)
            pref.canal_in_app = in_app
            pref.silenciado = silenciado
        elif pref:
            # Volta ao padrao: remove a preferencia customizada
            db.session.delete(pref)

    db.session.commit()
