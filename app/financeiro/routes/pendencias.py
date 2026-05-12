"""
Rotas de Pendências de NE - Módulo Financeiro.
"""
from flask import render_template, request, flash, redirect, url_for
from flask_login import login_required
from sqlalchemy.orm import joinedload

from app.financeiro.routes import financeiro_bp
from app.models import Solicitacao, Contrato, SolicitacaoEmpenho
from app.extensions import db
from app.services.siafe_service import validar_ne_siafe
from app.utils.permissions import requires_permission


@financeiro_bp.route('/pendencias_ne')
@login_required
@requires_permission('financeiro.visualizar')
def pendencias_ne():
    """Lista solicitações pendentes de inserção de NE."""
    filtro_contratado = request.args.getlist('filtro_contratado')
    busca = request.args.get('q', '').strip()
    try:
        page = max(1, int(request.args.get('page', 1) or 1))
    except (TypeError, ValueError):
        page = 1
    per_page = 20

    # Busca solicitações com empenho mas sem NE (eager load contrato)
    query = db.session.query(
        Solicitacao, SolicitacaoEmpenho
    ).join(
        Contrato
    ).options(
        joinedload(Solicitacao.contrato),
    ).join(
        SolicitacaoEmpenho,
        SolicitacaoEmpenho.id_solicitacao == Solicitacao.id
    ).filter(
        SolicitacaoEmpenho.ne.is_(None)
    )

    if filtro_contratado:
        query = query.filter(
            Contrato.nomeContratado.in_(filtro_contratado)
        )

    if busca:
        query = query.filter(
            db.or_(
                Solicitacao.codigo_contrato.ilike(f'%{busca}%'),
                Solicitacao.protocolo_gerado_sei.ilike(f'%{busca}%'),
                Contrato.nomeContratado.ilike(f'%{busca}%'),
                Contrato.numeroOriginal.ilike(f'%{busca}%')
            )
        )

    pagination = query.order_by(
        Solicitacao.data_solicitacao.desc(),
        SolicitacaoEmpenho.data.desc(),
        SolicitacaoEmpenho.id.desc(),
    ).paginate(page=page, per_page=per_page, error_out=False)

    pendencias = pagination.items

    # Lista de contratados para filtro (apenas os que têm pendência)
    contratados_query = db.session.query(
        Contrato.nomeContratado
    ).join(
        Solicitacao
    ).join(
        SolicitacaoEmpenho,
        SolicitacaoEmpenho.id_solicitacao == Solicitacao.id
    ).filter(
        SolicitacaoEmpenho.ne.is_(None)
    ).distinct().order_by(Contrato.nomeContratado).all()

    todos_contratados = [c[0] for c in contratados_query]

    return render_template(
        'financeiro/pendencias_ne.html',
        pendencias=pendencias,
        pagination=pagination,
        todos_contratados=todos_contratados,
        filtro_contratado=filtro_contratado
    )


@financeiro_bp.route('/inserir_ne/<int:empenho_id>', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_ne(empenho_id):
    """Insere NE em uma solicitação de empenho específica."""
    ne = request.form.get('ne', '').strip()

    if not ne or len(ne) < 4:
        flash('NE inválida. Deve ter pelo menos 4 dígitos.', 'danger')
        return redirect(url_for('financeiro.pendencias_ne'))

    sol_empenho = SolicitacaoEmpenho.query.filter(
        SolicitacaoEmpenho.id == empenho_id,
        SolicitacaoEmpenho.ne.is_(None),
    ).first()

    if not sol_empenho:
        flash('Solicitação de empenho pendente não encontrada.', 'danger')
        return redirect(url_for('financeiro.pendencias_ne'))

    # Valida NE no SIAFE (opcional)
    try:
        validacao = validar_ne_siafe(ne)
        if not validacao.get('sucesso'):
            flash(f"Aviso SIAFE: {validacao.get('mensagem', 'NE não validada')}", 'warning')
    except Exception:
        pass  # Continua mesmo se SIAFE falhar

    sol_empenho.ne = ne

    solicitacao = db.session.get(Solicitacao, sol_empenho.id_solicitacao)
    if solicitacao:
        solicitacao.status_empenho_id = 2

    db.session.commit()

    flash(f'NE {ne} inserida com sucesso!', 'success')
    return redirect(url_for('financeiro.pendencias_ne'))
