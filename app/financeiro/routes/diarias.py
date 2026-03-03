"""
Rotas de Diárias - Módulo Financeiro.
Lista solicitações de diárias despachadas para DFIN e permite inserção de Nota de Reserva.
"""
from flask import render_template, request, flash, redirect, url_for, abort
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.models.diaria import DiariasItinerario, DiariasItemItinerario
from app.extensions import db
from app.constants import DiariasEtapaID
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import gerar_token_sei_admin, adicionar_documento_externo
from app.utils.permissions import requires_permission


@financeiro_bp.route('/diarias')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_lista():
    """Lista solicitações de diárias despachadas para o financeiro (etapa >= 2)."""
    busca = request.args.get('q', '').strip()
    filtro_status = request.args.get('status_nr', '')
    page = request.args.get('page', 1, type=int)

    query = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id >= DiariasEtapaID.SOLICITACAO_AUTORIZADA
    )

    # Filtro de busca por processo SEI ou usuário gerador
    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    # Filtro por status da NR
    if filtro_status == 'pendente':
        query = query.filter(
            DiariasItinerario.nota_reserva.is_(None),
            DiariasItinerario.etapa_atual_id == DiariasEtapaID.SOLICITACAO_AUTORIZADA,
        )
    elif filtro_status == 'inserida':
        query = query.filter(
            DiariasItinerario.nota_reserva.isnot(None),
        )

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc()
    ).paginate(page=page, per_page=20, error_out=False)

    # Conta pessoas por itinerário para exibição
    itinerarios = pagination.items
    pessoas_count = {}
    if itinerarios:
        ids = [it.id for it in itinerarios]
        counts = db.session.query(
            DiariasItemItinerario.id_itinerario,
            db.func.count(DiariasItemItinerario.id)
        ).filter(
            DiariasItemItinerario.id_itinerario.in_(ids)
        ).group_by(DiariasItemItinerario.id_itinerario).all()
        pessoas_count = {row[0]: row[1] for row in counts}

    return render_template(
        'financeiro/diarias_lista.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        filtro_status=filtro_status,
    )


@financeiro_bp.route('/diarias/<int:id>')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_detalhe(id):
    """Exibe detalhes de uma solicitação de diária para o financeiro."""
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']

    # Só mostra se já chegou no financeiro (etapa >= 2)
    if itinerario.etapa_atual_id < DiariasEtapaID.SOLICITACAO_AUTORIZADA:
        abort(404)

    return render_template(
        'financeiro/diarias_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
    )


@financeiro_bp.route('/diarias/<int:id>/inserir-nr', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nr(id):
    """Insere Nota de Reserva em uma solicitação de diária."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: só permite inserção se etapa == 2 (pendente de NR)
    if itinerario.etapa_atual_id != DiariasEtapaID.SOLICITACAO_AUTORIZADA:
        flash('Esta solicitação já possui Nota de Reserva ou não está na etapa correta.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    nr_code = request.form.get('nota_reserva', '').strip()
    arquivo = request.files.get('arquivo_nr')

    if not nr_code:
        flash('O código da Nota de Reserva é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Salva o código da NR
    itinerario.nota_reserva = nr_code

    # Upload do PDF ao SEI (se arquivo fornecido e processo SEI existe)
    sei_upload_ok = False
    if arquivo and arquivo.filename and itinerario.sei_protocolo:
        try:
            arquivo_bytes = arquivo.read()
            if len(arquivo_bytes) > 0:
                token = gerar_token_sei_admin()
                if token:
                    retorno = adicionar_documento_externo(
                        token=token,
                        protocolo_formatado=itinerario.sei_protocolo,
                        arquivo_bytes=arquivo_bytes,
                        nome_arquivo=arquivo.filename,
                        descricao=f'Nota de Reserva {nr_code}',
                    )
                    if retorno:
                        itinerario.sei_id_nota_reserva = str(retorno.get('IdDocumento', ''))
                        itinerario.sei_nota_reserva_formatado = retorno.get('DocumentoFormatado', '')
                        sei_upload_ok = True
                    else:
                        flash('Aviso: NR salva, mas o upload do documento ao SEI falhou.', 'warning')
                else:
                    flash('Aviso: NR salva, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: NR salva, mas erro ao enviar documento ao SEI: {e}', 'warning')

    # Avança para etapa 3 (Financeiro)
    DiariaService.registrar_movimentacao(
        id_itinerario=id,
        etapa_nova_id=DiariasEtapaID.FINANCEIRO,
        usuario_id=current_user.id if current_user else None,
        comentario=f'Nota de Reserva {nr_code} inserida pelo financeiro',
    )

    if sei_upload_ok:
        flash(f'Nota de Reserva {nr_code} inserida e documento enviado ao SEI com sucesso!', 'success')
    elif not arquivo or not arquivo.filename:
        flash(f'Nota de Reserva {nr_code} inserida com sucesso!', 'success')

    return redirect(url_for('financeiro.diarias_lista'))
