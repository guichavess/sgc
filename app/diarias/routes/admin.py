"""
Rotas administrativas do módulo de Diárias (gerenciar agências, cargos/valores, administração).
"""
from flask import render_template, request, redirect, url_for, flash, abort
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.models.diaria import (
    DiariasValorCargo, DiariasCargo,
    DiariasItinerario, DiariasItemItinerario,
)
from app.constants import DiariasEtapaID
from app.extensions import db


# ── Agências (leitura a partir dos contratos com Natureza 339033) ────────────

@diarias_bp.route('/agencias')
@login_required
@requires_permission('diarias.aprovar')
def agencias():
    """Lista agências de viagem vinculadas a contratos com Natureza 339033."""
    return render_template('diarias/agencias.html',
        agencias=DiariaService.get_agencias(),
    )


# ── Cargos e Valores ────────────────────────────────────────────────────────

@diarias_bp.route('/cargos')
@login_required
@requires_permission('diarias.aprovar')
def cargos():
    """Lista cargos e valores de diárias."""
    return render_template('diarias/cargos.html',
        cargos=DiariasCargo.query.order_by(DiariasCargo.nome).all(),
        valores=DiariasValorCargo.query.order_by(
            DiariasValorCargo.cargo_id,
            DiariasValorCargo.tipo_itinerario_id,
        ).all(),
    )


@diarias_bp.route('/cargos/salvar-valor', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def salvar_valor_cargo():
    """Cria ou atualiza um valor de diária."""
    vc_id = request.form.get('id')
    cargo_id = request.form.get('cargo_id', type=int)
    tipo_id = request.form.get('tipo_itinerario_id', type=int)
    valor = request.form.get('valor', type=float)

    if not all([cargo_id, tipo_id, valor is not None]):
        flash('Todos os campos são obrigatórios.', 'danger')
        return redirect(url_for('diarias.cargos'))

    if vc_id:
        vc = DiariasValorCargo.query.get(int(vc_id))
        if vc:
            vc.cargo_id = cargo_id
            vc.tipo_itinerario_id = tipo_id
            vc.valor = valor
    else:
        vc = DiariasValorCargo(cargo_id=cargo_id, tipo_itinerario_id=tipo_id, valor=valor)
        db.session.add(vc)

    db.session.commit()
    flash('Valor salvo com sucesso!', 'success')
    return redirect(url_for('diarias.cargos'))


# ── Administração / Acompanhamento ────────────────────────────────────────

@diarias_bp.route('/administracao')
@login_required
@requires_permission('diarias.aprovar')
def administracao():
    """
    Painel de administração: lista todas as solicitações com dados resumidos
    e progresso na timeline para o gestor acompanhar.
    """
    busca = request.args.get('q', '').strip()
    filtro_etapa = request.args.get('etapa', '')
    page = request.args.get('page', 1, type=int)

    query = DiariasItinerario.query

    if busca:
        query = query.filter(
            db.or_(
                DiariasItinerario.sei_protocolo.ilike(f'%{busca}%'),
                DiariasItinerario.n_processo.ilike(f'%{busca}%'),
                DiariasItinerario.usuario_gerador.ilike(f'%{busca}%'),
            )
        )

    if filtro_etapa:
        query = query.filter(DiariasItinerario.etapa_atual_id == int(filtro_etapa))

    pagination = query.order_by(
        DiariasItinerario.data_solicitacao.desc()
    ).paginate(page=page, per_page=20, error_out=False)

    itinerarios = pagination.items

    # Conta pessoas por itinerário
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

    from app.models.diaria import DiariasEtapa
    etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()

    return render_template(
        'diarias/administracao.html',
        itinerarios=itinerarios,
        pagination=pagination,
        pessoas_count=pessoas_count,
        etapas=etapas,
        filtro_etapa=filtro_etapa,
    )


@diarias_bp.route('/administracao/<int:id>')
@login_required
@requires_permission('diarias.aprovar')
def administracao_detalhe(id):
    """
    Detalhe administrativo de uma solicitação: timeline completa, info geral,
    upload de cotação ao SEI e visualização da NR do financeiro.
    """
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        abort(404)

    itinerario = dados['itinerario']
    timeline_data = DiariaService.obter_timeline(itinerario)

    return render_template(
        'diarias/administracao_detalhe.html',
        itinerario=itinerario,
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        cotacoes_voos=dados.get('cotacoes_voos', []),
        timeline_data=timeline_data,
        agencias=DiariaService.get_agencias(),
    )


@diarias_bp.route('/administracao/<int:id>/upload-cotacao', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def upload_cotacao(id):
    """
    Faz upload de um documento de cotação de passagem ao processo SEI
    usando a série "Cotação" (IdSerie 272).
    """
    from app.services.diarias_sei_integration import (
        gerar_token_sei_admin, adicionar_documento_externo, ID_SERIE_COTACAO,
    )

    itinerario = DiariasItinerario.query.get_or_404(id)

    if not itinerario.sei_protocolo:
        flash('Esta solicitação não possui processo SEI para enviar o documento.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    arquivo = request.files.get('arquivo_cotacao')
    descricao_cotacao = request.form.get('descricao_cotacao', '').strip()

    if not arquivo or not arquivo.filename:
        flash('Selecione um arquivo para enviar.', 'danger')
        return redirect(url_for('diarias.administracao_detalhe', id=id))

    try:
        arquivo_bytes = arquivo.read()
        if len(arquivo_bytes) == 0:
            flash('O arquivo está vazio.', 'danger')
            return redirect(url_for('diarias.administracao_detalhe', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação com o SEI.', 'danger')
            return redirect(url_for('diarias.administracao_detalhe', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao=descricao_cotacao or 'Cotação de passagens',
            id_serie=ID_SERIE_COTACAO,
            numero=descricao_cotacao or None,
        )

        if retorno:
            doc_fmt = retorno.get('DocumentoFormatado', '')
            flash(
                f'Cotação enviada ao SEI com sucesso! Documento: {doc_fmt}',
                'success',
            )
        else:
            flash('Erro ao enviar cotação ao SEI. Tente novamente.', 'danger')

    except Exception as e:
        flash(f'Erro ao processar upload: {str(e)}', 'danger')

    return redirect(url_for('diarias.administracao_detalhe', id=id))
