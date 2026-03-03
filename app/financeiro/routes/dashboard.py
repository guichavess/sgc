"""
Dashboard do Módulo Financeiro.
"""
from datetime import datetime
from flask import render_template, request
from flask_login import login_required
from sqlalchemy import func, case

from app.financeiro.routes import financeiro_bp
from app.models import Solicitacao, Contrato, SolicitacaoEmpenho
from app.extensions import db
from app.repositories import EmpenhoRepository
from app.utils.permissions import requires_permission


@financeiro_bp.route('/')
@login_required
@requires_permission('financeiro.visualizar')
def dashboard():
    """Dashboard financeiro com visão de saldos e empenhos por contrato."""
    ano = request.args.get('ano', datetime.now().year, type=int)
    page = request.args.get('page', 1, type=int)
    per_page = 20

    # Query base agrupada por contrato
    base_query = db.session.query(
        Contrato.codigo,
        Contrato.numeroOriginal,
        Contrato.nomeContratado,
        func.sum(SolicitacaoEmpenho.valor).label('total_solicitado'),
        func.count(SolicitacaoEmpenho.id).label('qtd_solicitacoes'),
        func.sum(
            case((SolicitacaoEmpenho.ne.is_(None), 1), else_=0)
        ).label('qtd_pendentes_ne')
    ).join(
        Solicitacao, Solicitacao.codigo_contrato == Contrato.codigo
    ).join(
        SolicitacaoEmpenho,
        SolicitacaoEmpenho.id_solicitacao == Solicitacao.id
    ).group_by(
        Contrato.codigo,
        Contrato.numeroOriginal,
        Contrato.nomeContratado
    ).order_by(
        Contrato.nomeContratado
    )

    # Totais gerais (calculados sobre TODOS os contratos, não apenas a página)
    todos_contratos = base_query.all()

    total_geral_empenhado = 0
    total_geral_solicitado = 0
    total_geral_pendentes = 0

    for row in todos_contratos:
        total_empenhado = EmpenhoRepository.calcular_total_empenhado(
            codigo_contrato=row.codigo,
            ano=ano
        )
        total_geral_empenhado += total_empenhado
        total_geral_solicitado += float(row.total_solicitado or 0)
        total_geral_pendentes += int(row.qtd_pendentes_ne or 0)

    totais = {
        'empenhado': total_geral_empenhado,
        'solicitado': total_geral_solicitado,
        'saldo': max(0, total_geral_empenhado - total_geral_solicitado),
        'pendentes_ne': total_geral_pendentes
    }

    # Paginação: subquery para paginar resultados agrupados
    total_contratos = len(todos_contratos)
    offset = (page - 1) * per_page
    contratos_pagina = todos_contratos[offset:offset + per_page]

    # Monta resumo apenas para a página atual
    resumo_contratos = []
    for row in contratos_pagina:
        total_empenhado = EmpenhoRepository.calcular_total_empenhado(
            codigo_contrato=row.codigo,
            ano=ano
        )

        total_solicitado = float(row.total_solicitado or 0)
        qtd_pendentes = int(row.qtd_pendentes_ne or 0)

        resumo_contratos.append({
            'codigo': row.codigo,
            'numero_original': row.numeroOriginal,
            'contratado': row.nomeContratado,
            'total_empenhado': total_empenhado,
            'total_solicitado': total_solicitado,
            'saldo_disponivel': max(0, total_empenhado - total_solicitado),
            'qtd_solicitacoes': row.qtd_solicitacoes,
            'qtd_pendentes_ne': qtd_pendentes
        })

    # Objeto de paginação manual
    pagination = PaginacaoManual(
        items=resumo_contratos,
        page=page,
        per_page=per_page,
        total=total_contratos
    )

    return render_template(
        'financeiro/dashboard.html',
        resumo_contratos=resumo_contratos,
        pagination=pagination,
        totais=totais,
        ano=ano
    )


class PaginacaoManual:
    """Objeto de paginação compatível com o template de paginação do projeto."""

    def __init__(self, items, page, per_page, total):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total
        self.pages = max(1, (total + per_page - 1) // per_page)
        self.has_prev = page > 1
        self.has_next = page < self.pages
        self.prev_num = page - 1 if self.has_prev else None
        self.next_num = page + 1 if self.has_next else None

    def iter_pages(self, left_edge=1, left_current=2, right_current=2, right_edge=1):
        """Gera números de página para exibição, com reticências."""
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (self.page - left_current <= num <= self.page + right_current)
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num
