"""
Rotas de Perfis - CRUD de perfis e permissões.
Acesso restrito a administradores (is_admin=True).
"""
from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required

from app.usuarios.routes import usuarios_bp
from app.services.usuario_service import UsuarioService
from app.utils.permissions import requires_admin


@usuarios_bp.route('/perfis')
@login_required
@requires_admin
def perfis_index():
    """Lista de perfis."""
    perfis = UsuarioService.listar_perfis()

    # Contar usuários por perfil
    contagem = {}
    for perfil in perfis:
        contagem[perfil.id] = UsuarioService.contar_usuarios_por_perfil(perfil.id)

    return render_template(
        'usuarios/perfis/index.html',
        perfis=perfis,
        contagem=contagem
    )


@usuarios_bp.route('/perfis/novo', methods=['GET', 'POST'])
@login_required
@requires_admin
def perfil_novo():
    """Criar novo perfil."""
    if request.method == 'POST':
        nome = request.form.get('nome', '').strip()
        descricao = request.form.get('descricao', '').strip()

        # Coleta permissões do form (checkboxes: perm:modulo:pagina:acao)
        permissoes = UsuarioService.extrair_permissoes_form(request.form)

        if not nome:
            flash('O nome do perfil é obrigatório.', 'danger')
        else:
            try:
                UsuarioService.criar_perfil(
                    nome=nome,
                    descricao=descricao or None,
                    permissoes=permissoes
                )
                flash('Perfil criado com sucesso!', 'success')
                return redirect(url_for('usuarios.perfis_index'))
            except ValueError as e:
                flash(str(e), 'danger')

    return render_template(
        'usuarios/perfis/form.html',
        perfil=None,
        permissoes_atuais={},
        **_contexto_matriz()
    )


@usuarios_bp.route('/perfis/<int:perfil_id>/editar', methods=['GET', 'POST'])
@login_required
@requires_admin
def perfil_editar(perfil_id):
    """Editar perfil existente."""
    perfil = UsuarioService.buscar_perfil(perfil_id)
    if not perfil:
        flash('Perfil não encontrado.', 'danger')
        return redirect(url_for('usuarios.perfis_index'))

    if request.method == 'POST':
        nome = request.form.get('nome', '').strip()
        descricao = request.form.get('descricao', '').strip()
        ativo = request.form.get('ativo') == '1'

        permissoes = UsuarioService.extrair_permissoes_form(request.form)

        if not nome:
            flash('O nome do perfil é obrigatório.', 'danger')
        else:
            try:
                UsuarioService.atualizar_perfil(
                    perfil_id=perfil_id,
                    nome=nome,
                    descricao=descricao or None,
                    ativo=ativo,
                    permissoes=permissoes
                )
                flash('Perfil atualizado com sucesso!', 'success')
                return redirect(url_for('usuarios.perfis_index'))
            except ValueError as e:
                flash(str(e), 'danger')

    return render_template(
        'usuarios/perfis/form.html',
        perfil=perfil,
        permissoes_atuais=perfil.listar_permissoes_dict(),
        **_contexto_matriz()
    )


@usuarios_bp.route('/perfis/<int:perfil_id>/excluir', methods=['POST'])
@login_required
@requires_admin
def perfil_excluir(perfil_id):
    """Excluir perfil."""
    try:
        UsuarioService.excluir_perfil(perfil_id)
        flash('Perfil excluído com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')

    return redirect(url_for('usuarios.perfis_index'))


def _contexto_matriz():
    """Variáveis da matriz de permissões (usuarios/partials/matriz_permissoes.html)."""
    return {
        'modulos': UsuarioService.get_modulos(),
        'acoes': UsuarioService.get_acoes(),
        'paginas_modulo': UsuarioService.get_paginas_liberaveis(),
        'paginas_alta_gestao': UsuarioService.get_paginas_alta_gestao(),
    }
