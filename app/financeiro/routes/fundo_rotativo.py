"""
Rotas CRUD da aba "Saldo" do módulo Gestão do Fundo Rotativo.

Sub-seção do módulo Financeiro. Permissões: fundo_rotativo.visualizar / criar.
"""
from datetime import datetime

from flask import render_template, request, flash, redirect, url_for
from flask_login import login_required, current_user

from app.constants import FUNDO_ROTATIVO_EXERCICIOS, FUNDO_ROTATIVO_FONTES_PERMITIDAS
from app.financeiro.routes import financeiro_bp
from app.models.class_fonte import ClassFonte
from app.services.fundo_rotativo_service import (
    criar_saldo,
    listar_saldos,
    atualizar_saldo,
    excluir_saldo,
    listar_naturezas_disponiveis,
    listar_anos_disponiveis,
    listar_anos_dashboard_disponiveis,
    obter_dashboard_fundo_rotativo,
)
from app.utils.permissions import requires_permission


def _parse_data(valor):
    """Converte string 'YYYY-MM-DDTHH:MM' (input datetime-local) em datetime."""
    if not valor:
        return None
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(valor, fmt)
        except ValueError:
            continue
    return None


# =============================================================================
# Lista (GET)
# =============================================================================
@financeiro_bp.route('/fundo-rotativo/saldo')
@login_required
@requires_permission('fundo_rotativo.visualizar')
def fundo_rotativo_saldo_lista():
    page = max(1, request.args.get('page', 1, type=int) or 1)
    busca = request.args.get('busca', '').strip()
    fonte_codigo = request.args.get('fonte_codigo', '').strip()
    id_exercicio = request.args.get('id_exercicio', '').strip()
    ano = request.args.get('ano', '').strip()
    natureza = request.args.get('natureza', '').strip()

    pagination, soma_total = listar_saldos(
        page=page,
        busca=busca or None,
        fonte_codigo=fonte_codigo or None,
        id_exercicio=id_exercicio or None,
        ano=ano or None,
        natureza=natureza or None,
    )

    fontes = (
        ClassFonte.query
        .filter(ClassFonte.codigo.in_(FUNDO_ROTATIVO_FONTES_PERMITIDAS))
        .order_by(ClassFonte.codigo.asc())
        .all()
    )
    naturezas = listar_naturezas_disponiveis()
    anos = listar_anos_disponiveis()

    return render_template(
        'financeiro/fundo_rotativo/saldo.html',
        saldos=pagination.items,
        pagination=pagination,
        soma_total=soma_total,
        busca=busca,
        fonte_codigo_filtro=fonte_codigo,
        id_exercicio_filtro=id_exercicio,
        ano_filtro=ano,
        natureza_filtro=natureza,
        fontes=fontes,
        exercicios=FUNDO_ROTATIVO_EXERCICIOS,
        naturezas=naturezas,
        anos=anos,
    )


# =============================================================================
# Dashboard (GET)
# =============================================================================
@financeiro_bp.route('/fundo-rotativo/dashboard')
@login_required
@requires_permission('fundo_rotativo.visualizar')
def fundo_rotativo_dashboard():
    ano = request.args.get('ano', '').strip()
    fonte_codigo = request.args.get('fonte_codigo', '').strip()
    natureza = request.args.get('natureza', '').strip()

    dashboard = obter_dashboard_fundo_rotativo(
        ano=ano or None,
        fonte_codigo=fonte_codigo or None,
        natureza=natureza or None,
    )
    fontes = (
        ClassFonte.query
        .filter(ClassFonte.codigo.in_(FUNDO_ROTATIVO_FONTES_PERMITIDAS))
        .order_by(ClassFonte.codigo.asc())
        .all()
    )
    naturezas = listar_naturezas_disponiveis()
    anos = listar_anos_dashboard_disponiveis()

    return render_template(
        'financeiro/fundo_rotativo/dashboard.html',
        kpis=dashboard['kpis'],
        rows=dashboard['rows'],
        anos=anos,
        fontes=fontes,
        naturezas=naturezas,
        ano_filtro=ano,
        fonte_codigo_filtro=fonte_codigo,
        natureza_filtro=natureza,
    )


# =============================================================================
# Cadastrar (POST)
# =============================================================================
@financeiro_bp.route('/fundo-rotativo/saldo/cadastrar', methods=['POST'])
@login_required
@requires_permission('fundo_rotativo.criar')
def fundo_rotativo_saldo_cadastrar():
    valor = request.form.get('valor', '').strip()
    data_str = request.form.get('data', '').strip()
    fonte_codigo = request.form.get('fonte_codigo', '').strip()
    id_exercicio = request.form.get('id_exercicio', '').strip()

    data = _parse_data(data_str)

    try:
        criar_saldo(
            valor=valor,
            data=data,
            fonte_codigo=fonte_codigo,
            id_exercicio=id_exercicio,
            usuario_id=current_user.id,
        )
        flash('Saldo cadastrado com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')
    except Exception:
        from app.extensions import db
        db.session.rollback()
        flash('Erro ao salvar. Tente novamente.', 'danger')

    return redirect(url_for('financeiro.fundo_rotativo_saldo_lista'))


# =============================================================================
# Editar (POST)
# =============================================================================
@financeiro_bp.route('/fundo-rotativo/saldo/<int:saldo_id>/editar', methods=['POST'])
@login_required
@requires_permission('fundo_rotativo.criar')
def fundo_rotativo_saldo_editar(saldo_id):
    valor = request.form.get('valor', '').strip()
    data_str = request.form.get('data', '').strip()
    fonte_codigo = request.form.get('fonte_codigo', '').strip()
    id_exercicio = request.form.get('id_exercicio', '').strip()

    data = _parse_data(data_str)

    try:
        atualizar_saldo(
            saldo_id,
            valor=valor,
            data=data,
            fonte_codigo=fonte_codigo,
            id_exercicio=id_exercicio,
        )
        flash('Saldo atualizado com sucesso!', 'success')
    except ValueError as e:
        flash(str(e), 'danger')
    except Exception:
        from app.extensions import db
        db.session.rollback()
        flash('Erro ao salvar. Tente novamente.', 'danger')

    return redirect(url_for('financeiro.fundo_rotativo_saldo_lista'))


# =============================================================================
# Excluir (POST)
# =============================================================================
@financeiro_bp.route('/fundo-rotativo/saldo/<int:saldo_id>/excluir', methods=['POST'])
@login_required
@requires_permission('fundo_rotativo.criar')
def fundo_rotativo_saldo_excluir(saldo_id):
    try:
        excluir_saldo(saldo_id)
        flash('Saldo excluído com sucesso!', 'success')
    except Exception:
        from app.extensions import db
        db.session.rollback()
        flash('Erro ao excluir. Tente novamente.', 'danger')

    return redirect(url_for('financeiro.fundo_rotativo_saldo_lista'))
