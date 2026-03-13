"""
Rotas de Diárias - Módulo Financeiro.
Lista solicitações de diárias despachadas para DFIN e permite inserção de Nota de Reserva,
Quadro Orçamentário, upload de Autorização SCDP e criação de Nota de Empenho.
"""
from decimal import Decimal, InvalidOperation
from flask import render_template, request, flash, redirect, url_for, abort, jsonify
from flask_login import login_required, current_user

from app.financeiro.routes import financeiro_bp
from app.models.diaria import DiariasItinerario, DiariasItemItinerario
from app.extensions import db
from app.constants import DiariasEtapaID
from app.services.diaria_service import DiariaService
from app.services.diarias_sei_integration import (
    gerar_token_sei_admin, adicionar_documento_externo, gerar_quadro_orcamentario,
    gerar_nota_empenho, ID_SERIE_AUTORIZACAO_SCDP,
)
from app.utils.permissions import requires_permission


@financeiro_bp.route('/diarias')
@login_required
@requires_permission('financeiro.visualizar')
def diarias_lista():
    """Lista solicitações de diárias na etapa financeira ou posterior (etapa >= 2)."""
    busca = request.args.get('q', '').strip()
    filtro_status = request.args.get('status_nr', '')
    page = request.args.get('page', 1, type=int)

    query = DiariasItinerario.query.filter(
        DiariasItinerario.etapa_atual_id >= DiariasEtapaID.FINANCEIRO
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
            DiariasItinerario.etapa_atual_id == DiariasEtapaID.FINANCEIRO,
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
    if itinerario.etapa_atual_id < DiariasEtapaID.FINANCEIRO:
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

    # Guard: só permite inserção se etapa == 2 (Financeiro - pendente de NR)
    if itinerario.etapa_atual_id != DiariasEtapaID.FINANCEIRO:
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

    # Avança para etapa 3 (Aquisição de Passagens)
    DiariaService.registrar_movimentacao(
        id_itinerario=id,
        etapa_nova_id=DiariasEtapaID.AQUISICAO_PASSAGENS,
        usuario_id=current_user.id if current_user else None,
        comentario=f'Nota de Reserva {nr_code} inserida pelo financeiro',
    )

    if sei_upload_ok:
        flash(f'Nota de Reserva {nr_code} inserida e documento enviado ao SEI com sucesso!', 'success')
    elif not arquivo or not arquivo.filename:
        flash(f'Nota de Reserva {nr_code} inserida com sucesso!', 'success')

    return redirect(url_for('financeiro.diarias_lista'))


def _parse_valor_brl(valor_str):
    """Converte string de valor BR (1.234,56 ou 1234.56) para Decimal."""
    if not valor_str:
        return None
    valor_str = valor_str.strip().replace('R$', '').strip()
    # Formato brasileiro: 1.234,56 → 1234.56
    if ',' in valor_str:
        valor_str = valor_str.replace('.', '').replace(',', '.')
    try:
        return Decimal(valor_str)
    except (InvalidOperation, ValueError):
        return None


@financeiro_bp.route('/diarias/<int:id>/inserir-quadro-orcamentario', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_quadro_orcamentario(id):
    """Insere Quadro Orçamentário em uma solicitação de diária (após NR)."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: NR deve estar inserida e quadro ainda não preenchido
    if not itinerario.nota_reserva:
        flash('A Nota de Reserva deve ser inserida antes do Quadro Orçamentário.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.quadro_ug:
        flash('O Quadro Orçamentário já foi inserido para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Coleta campos do formulário
    ug = request.form.get('quadro_ug', '').strip()
    funcao = request.form.get('quadro_funcao', '').strip()
    subfuncao = request.form.get('quadro_subfuncao', '').strip()
    programa = request.form.get('quadro_programa', '').strip()
    plano_interno = request.form.get('quadro_plano_interno', '').strip()
    fonte_recursos = request.form.get('quadro_fonte_recursos', '').strip()
    natureza_despesa = request.form.get('quadro_natureza_despesa', '').strip()
    valor_inicial_nr = _parse_valor_brl(request.form.get('quadro_valor_inicial_nr', ''))
    saldo_nr = _parse_valor_brl(request.form.get('quadro_saldo_nr', ''))
    valor_despesa = _parse_valor_brl(request.form.get('quadro_valor_despesa', ''))
    saldo_atual_nr = _parse_valor_brl(request.form.get('quadro_saldo_atual_nr', ''))

    # Validação básica
    if not ug or not natureza_despesa:
        flash('UG e Natureza da Despesa são obrigatórios.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if valor_despesa is None:
        flash('Valor da Despesa é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Salva no banco
    itinerario.quadro_ug = ug
    itinerario.quadro_funcao = funcao
    itinerario.quadro_subfuncao = subfuncao
    itinerario.quadro_programa = programa
    itinerario.quadro_plano_interno = plano_interno
    itinerario.quadro_fonte_recursos = fonte_recursos
    itinerario.quadro_natureza_despesa = natureza_despesa
    itinerario.quadro_valor_inicial_nr = valor_inicial_nr
    itinerario.quadro_saldo_nr = saldo_nr
    itinerario.quadro_valor_despesa = valor_despesa
    itinerario.quadro_saldo_atual_nr = saldo_atual_nr

    # Gera documento no SEI (se processo SEI existe)
    sei_ok = False
    if itinerario.sei_id_procedimento:
        try:
            token = gerar_token_sei_admin()
            if token:
                retorno = gerar_quadro_orcamentario(
                    token=token,
                    id_procedimento=itinerario.sei_id_procedimento,
                    dados_quadro={
                        'ug': ug,
                        'funcao': funcao,
                        'subfuncao': subfuncao,
                        'programa': programa,
                        'plano_interno': plano_interno,
                        'fonte_recursos': fonte_recursos,
                        'natureza_despesa': natureza_despesa,
                        'valor_inicial_nr': valor_inicial_nr,
                        'saldo_nr': saldo_nr,
                        'valor_despesa': valor_despesa,
                        'saldo_atual_nr': saldo_atual_nr,
                    },
                    sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
                )
                if retorno:
                    itinerario.sei_id_quadro_orcamentario = str(retorno.get('IdDocumento', ''))
                    itinerario.sei_quadro_orcamentario_formatado = retorno.get('DocumentoFormatado', '')
                    sei_ok = True
                else:
                    flash('Aviso: Quadro salvo, mas a geração do documento no SEI falhou.', 'warning')
            else:
                flash('Aviso: Quadro salvo, mas não foi possível autenticar no SEI.', 'warning')
        except Exception as e:
            flash(f'Aviso: Quadro salvo, mas erro ao gerar documento no SEI: {e}', 'warning')

    db.session.commit()

    if sei_ok:
        flash('Quadro Orçamentário inserido e documento gerado no SEI com sucesso!', 'success')
    elif not itinerario.sei_id_procedimento:
        flash('Quadro Orçamentário inserido com sucesso!', 'success')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/upload-scdp', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def upload_autorizacao_scdp(id):
    """Upload do PDF 'Autorização SOLICITAÇÃO APROVADA SCDP' ao processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: precisa ter processo SEI
    if not itinerario.sei_protocolo:
        flash('Esta solicitação não possui processo SEI vinculado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.sei_id_autorizacao_scdp:
        flash('A Autorização SCDP já foi enviada para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    arquivo = request.files.get('arquivo_scdp')
    numero_scdp = request.form.get('numero_scdp', '').strip()

    if not arquivo or not arquivo.filename:
        flash('O arquivo PDF da Autorização SCDP é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    try:
        arquivo_bytes = arquivo.read()
        if len(arquivo_bytes) == 0:
            flash('O arquivo está vazio.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao=f'Autorização SOLICITAÇÃO APROVADA SCDP{" (" + numero_scdp + ")" if numero_scdp else ""}',
            id_serie=ID_SERIE_AUTORIZACAO_SCDP,
            numero=numero_scdp or None,
        )

        if retorno:
            itinerario.sei_id_autorizacao_scdp = str(retorno.get('IdDocumento', ''))
            itinerario.sei_autorizacao_scdp_formatado = retorno.get('DocumentoFormatado', '')
            db.session.commit()
            flash('Autorização SCDP enviada ao SEI com sucesso!', 'success')
        else:
            flash('Erro ao enviar documento ao SEI.', 'danger')

    except Exception as e:
        flash(f'Erro ao enviar Autorização SCDP: {e}', 'danger')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))


@financeiro_bp.route('/diarias/<int:id>/inserir-nota-empenho', methods=['POST'])
@login_required
@requires_permission('financeiro.criar')
def inserir_nota_empenho(id):
    """Cria documento Nota de Empenho (idSerie 419) no processo SEI."""
    itinerario = DiariasItinerario.query.get_or_404(id)

    # Guard: precisa ter processo SEI
    if not itinerario.sei_id_procedimento:
        flash('Esta solicitação não possui processo SEI vinculado.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    if itinerario.sei_id_nota_empenho:
        flash('A Nota de Empenho já foi inserida para esta solicitação.', 'warning')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    codigo_ne = request.form.get('nota_empenho_codigo', '').strip()
    if not codigo_ne:
        flash('O código da Nota de Empenho é obrigatório.', 'danger')
        return redirect(url_for('financeiro.diarias_detalhe', id=id))

    # Dados opcionais para enriquecer o documento
    dados_empenho = {
        'valor': request.form.get('ne_valor', '').strip() or None,
        'natureza_despesa': request.form.get('ne_natureza_despesa', '').strip() or None,
        'fonte_recursos': request.form.get('ne_fonte_recursos', '').strip() or None,
        'favorecido': request.form.get('ne_favorecido', '').strip() or None,
        'objeto': request.form.get('ne_objeto', '').strip() or None,
    }

    try:
        token = gerar_token_sei_admin()
        if not token:
            flash('Não foi possível autenticar no SEI.', 'danger')
            return redirect(url_for('financeiro.diarias_detalhe', id=id))

        retorno = gerar_nota_empenho(
            token=token,
            id_procedimento=itinerario.sei_id_procedimento,
            sei_protocolo=itinerario.sei_protocolo or itinerario.n_processo or '',
            codigo_ne=codigo_ne,
            dados_empenho=dados_empenho,
        )

        if retorno:
            itinerario.nota_empenho_codigo = codigo_ne
            itinerario.sei_id_nota_empenho = str(retorno.get('IdDocumento', ''))
            itinerario.sei_nota_empenho_formatado = retorno.get('DocumentoFormatado', '')
            db.session.commit()
            flash(f'Nota de Empenho {codigo_ne} inserida e documento gerado no SEI!', 'success')
        else:
            flash('Erro ao gerar documento de Nota de Empenho no SEI.', 'danger')

    except Exception as e:
        flash(f'Erro ao inserir Nota de Empenho: {e}', 'danger')

    return redirect(url_for('financeiro.diarias_detalhe', id=id))
