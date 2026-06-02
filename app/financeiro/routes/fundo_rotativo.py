"""
Rotas da aba "Saldo" e dashboard do modulo Gestao do Fundo Rotativo.

Sub-secao do modulo Financeiro. Permissoes: fundo_rotativo.visualizar / criar.
"""
from flask import flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.constants import FUNDO_ROTATIVO_EXERCICIOS
from app.financeiro.routes import financeiro_bp
from app.services.fundo_rotativo_service import (
    listar_anos_dashboard_disponiveis,
    listar_anos_disponiveis,
    listar_contas_disponiveis,
    listar_fontes_saldo_disponiveis,
    listar_meses_disponiveis,
    listar_naturezas_disponiveis,
    listar_saldos,
    obter_dashboard_fundo_rotativo,
    sincronizar_saldos_inicial,
    sincronizar_saldos_mes_atual,
)
from app.utils.permissions import requires_permission


@financeiro_bp.route('/fundo-rotativo/saldo')
@login_required
@requires_permission('fundo_rotativo.visualizar')
def fundo_rotativo_saldo_lista():
    page = max(1, request.args.get('page', 1, type=int) or 1)
    busca = request.args.get('busca', '').strip()
    fontes_codigo = [v for v in request.args.getlist('fonte_codigo') if v]
    ids_exercicio = [v for v in request.args.getlist('id_exercicio') if v]
    anos_filtro = [v for v in request.args.getlist('ano') if v]
    meses_filtro = [v for v in request.args.getlist('mes') if v]
    contas_filtro = [v for v in request.args.getlist('conta_bancaria') if v]

    pagination, soma_total, soma_filtrada = listar_saldos(
        page=page,
        busca=busca or None,
        fonte_codigo=fontes_codigo or None,
        id_exercicio=ids_exercicio or None,
        ano=anos_filtro or None,
        mes=meses_filtro or None,
        conta_bancaria=contas_filtro or None,
    )

    tem_filtro_ativo = bool(busca or fontes_codigo or ids_exercicio or anos_filtro or meses_filtro or contas_filtro)

    return render_template(
        'financeiro/fundo_rotativo/saldo.html',
        saldos=pagination.items,
        pagination=pagination,
        soma_total=soma_total,
        soma_filtrada=soma_filtrada,
        tem_filtro_ativo=tem_filtro_ativo,
        busca=busca,
        fontes_codigo_filtro=fontes_codigo,
        ids_exercicio_filtro=ids_exercicio,
        anos_filtro=anos_filtro,
        meses_filtro=meses_filtro,
        contas_filtro=contas_filtro,
        fontes=listar_fontes_saldo_disponiveis(),
        exercicios=FUNDO_ROTATIVO_EXERCICIOS,
        anos=listar_anos_disponiveis(),
        meses=listar_meses_disponiveis(),
        contas=listar_contas_disponiveis(),
    )


@financeiro_bp.route('/fundo-rotativo/saldo/sincronizar-inicial', methods=['POST'])
@login_required
@requires_permission('fundo_rotativo.criar')
def fundo_rotativo_saldo_sincronizar_inicial():
    try:
        resultado = sincronizar_saldos_inicial(usuario_id=current_user.id)
        flash(
            'Carga inicial SIAFE concluida: '
            f'{resultado["registros"]} registros em {resultado["periodos"]} periodos.',
            'success',
        )
    except Exception as exc:
        flash(f'Erro na carga inicial SIAFE: {exc}', 'danger')
    return redirect(url_for('financeiro.fundo_rotativo_saldo_lista'))


@financeiro_bp.route('/fundo-rotativo/saldo/sincronizar', methods=['POST'])
@login_required
@requires_permission('fundo_rotativo.criar')
def fundo_rotativo_saldo_sincronizar():
    try:
        resultado = sincronizar_saldos_mes_atual(usuario_id=current_user.id)
        flash(
            'Sincronizacao SIAFE concluida: '
            f'{resultado["registros"]} registros em {resultado["periodos"]} periodo.',
            'success',
        )
    except Exception as exc:
        flash(f'Erro na sincronizacao SIAFE: {exc}', 'danger')
    return redirect(url_for('financeiro.fundo_rotativo_saldo_lista'))


@financeiro_bp.route('/fundo-rotativo/dashboard')
@login_required
@requires_permission('fundo_rotativo.visualizar')
def fundo_rotativo_dashboard():
    anos_filtro = [v for v in request.args.getlist('ano') if v]
    fontes_filtro = [v for v in request.args.getlist('fonte_codigo') if v]
    naturezas_filtro = [v for v in request.args.getlist('natureza') if v]

    dashboard = obter_dashboard_fundo_rotativo(
        ano=anos_filtro or None,
        fonte_codigo=fontes_filtro or None,
        natureza=naturezas_filtro or None,
    )
    fontes = listar_fontes_saldo_disponiveis()
    naturezas = listar_naturezas_disponiveis()
    anos = listar_anos_dashboard_disponiveis()

    return render_template(
        'financeiro/fundo_rotativo/dashboard.html',
        kpis=dashboard['kpis'],
        rows=dashboard['rows'],
        anos=anos,
        fontes=fontes,
        naturezas=naturezas,
        anos_filtro=anos_filtro,
        fontes_filtro=fontes_filtro,
        naturezas_filtro=naturezas_filtro,
    )
