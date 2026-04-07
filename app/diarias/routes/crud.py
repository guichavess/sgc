"""
Rotas CRUD do módulo de Diárias (criar, visualizar, atender).
"""
import json
import threading
from flask import render_template, request, redirect, url_for, flash, current_app, jsonify
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.services.sei_auth import gerar_token_sei_admin, autenticar_usuario_sei
from app.services.sei_integration import assinar_documento
from app.models.diaria import DiariasTipoSolicitacao, DiariasValorCargo, DiariasItinerario, Estado
from app.constants import DiariasEtapaID
from app.services.diarias_notification import DiariasNotifier


# IDs dos tipos de solicitação (espelhados do seed)
TIPO_SOL_APENAS_DIARIAS = 1
TIPO_SOL_DIARIAS_PASSAGENS = 2
TIPO_SOL_APENAS_PASSAGENS = 3


@diarias_bp.route('/nova')
@login_required
@requires_permission('diarias.criar')
def nova():
    """Formulário de nova solicitação de diária."""
    # Monta mapa de valores: { "cargo_id_tipo_itinerario_id": valor }
    valores_cargo = DiariasValorCargo.query.all()
    valores_map = {}
    for vc in valores_cargo:
        valores_map[f"{vc.cargo_id}_{vc.tipo_itinerario_id}"] = float(vc.valor)

    return render_template('diarias/nova.html',
        tipos_solicitacao=DiariaService.get_tipos_solicitacao(),
        estados=DiariaService.get_estados(),
        municipios_pi=DiariaService.get_municipios_por_estado(22),
        cargos=DiariaService.get_cargos(),
        valores_cargo_json=json.dumps(valores_map),
    )


def _executar_sei_background(app, itinerario_id, pessoas, dados, tipo_solicitacao_id,
                             justificativa_memorando, objetivo, arquivo_externo, usuario_id):
    """Executa integração SEI + notificação em thread separada para não bloquear o request."""
    with app.app_context():
        try:
            itinerario = DiariasItinerario.query.get(itinerario_id)
            if not itinerario:
                return

            _integrar_sei_diarias(itinerario, pessoas, dados, tipo_solicitacao_id,
                                  justificativa_memorando, objetivo, arquivo_externo)
        except Exception as e:
            current_app.logger.error(f'[DIARIAS] Erro SEI background: {e}')

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'nova_solicitacao', usuario_id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')


@diarias_bp.route('/store', methods=['POST'])
@login_required
@requires_permission('diarias.criar')
def store():
    """Salva nova solicitação de diária."""
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    try:
        tipo = int(request.form.get('tipo_itinerario') or 0)
        if tipo not in (1, 2, 3):
            if is_ajax:
                return jsonify({'success': False, 'error': 'Tipo de itinerário inválido.'}), 400
            flash('Tipo de itinerário inválido.', 'danger')
            return redirect(url_for('diarias.nova'))

        pessoas_matricula = request.form.getlist('pessoas_matricula[]')
        pessoas_cpf = request.form.getlist('pessoas_cpf[]')
        pessoas_nome = request.form.getlist('pessoas_nome[]')
        pessoas_cargo_id = request.form.getlist('pessoas_cargo_id[]')
        pessoas_cargo_assessorado = request.form.getlist('pessoas_cargo_assessorado_id[]')
        # Campos da API pessoaSGA
        pessoas_banco_agencia = request.form.getlist('pessoas_banco_agencia[]')
        pessoas_banco_conta = request.form.getlist('pessoas_banco_conta[]')
        pessoas_vinculo = request.form.getlist('pessoas_vinculo[]')
        pessoas_cargo_folha = request.form.getlist('pessoas_cargo_folha[]')
        pessoas_setor = request.form.getlist('pessoas_setor[]')
        pessoas_orgao = request.form.getlist('pessoas_orgao[]')

        if not pessoas_cpf:
            if is_ajax:
                return jsonify({'success': False, 'error': 'Adicione pelo menos uma pessoa à viagem.'}), 400
            flash('Adicione pelo menos uma pessoa à viagem.', 'danger')
            return redirect(url_for('diarias.nova'))

        pessoas = []
        for i in range(len(pessoas_cpf)):
            cargo_id_str = pessoas_cargo_id[i] if i < len(pessoas_cargo_id) else ''
            cargo_ass_str = pessoas_cargo_assessorado[i] if i < len(pessoas_cargo_assessorado) else ''
            pessoas.append({
                'cpf': pessoas_cpf[i],
                'matricula': pessoas_matricula[i] if i < len(pessoas_matricula) else '',
                'nome': pessoas_nome[i] if i < len(pessoas_nome) else '',
                'cargo_id': int(cargo_id_str) if cargo_id_str else None,
                'cargo_assessorado_id': int(cargo_ass_str) if cargo_ass_str else None,
                'banco_agencia': pessoas_banco_agencia[i] if i < len(pessoas_banco_agencia) else '',
                'banco_conta': pessoas_banco_conta[i] if i < len(pessoas_banco_conta) else '',
                'vinculo': pessoas_vinculo[i] if i < len(pessoas_vinculo) else '',
                'cargo_folha': pessoas_cargo_folha[i] if i < len(pessoas_cargo_folha) else '',
                'setor': pessoas_setor[i] if i < len(pessoas_setor) else '',
                'orgao': pessoas_orgao[i] if i < len(pessoas_orgao) else '',
            })

        paradas = request.form.getlist('paradas[]') if tipo == 1 else None
        justificativa = request.form.get('justificativa', '').strip() or None
        justificativa_memorando = request.form.get('justificativa_memorando', '').strip() or None

        tipo_solicitacao_id = int(request.form.get('tipo_solicitacao') or 0)

        objetivo = request.form.get('objetivo', '').strip() or None

        # Arquivo anexo (documento externo SEI)
        arquivo_anexo = request.files.get('arquivo_anexo')
        arquivo_externo = None
        if arquivo_anexo and arquivo_anexo.filename:
            arquivo_externo = {
                'bytes': arquivo_anexo.read(),
                'nome_arquivo': arquivo_anexo.filename,
                'descricao': f'Documento anexo - Solicitacao de Diarias',
            }
            current_app.logger.debug(f"SEI store(): Arquivo recebido: {arquivo_anexo.filename}, "
                  f"tamanho: {len(arquivo_externo['bytes'])} bytes")

        dados = {
            'tipo_solicitacao_id': tipo_solicitacao_id,
            'tipo_itinerario': tipo,
            'data_viagem': request.form.get('data_viagem'),
            'data_retorno': request.form.get('data_retorno'),
            'usuario_gerador': current_user.sigla_login,
            'estado_origem': request.form.get('estado_origem'),
            'estado_destino': request.form.get('estado_destino'),
            'objetivo': objetivo,
        }

        itinerario = DiariaService.criar_itinerario(dados, pessoas, paradas, justificativa)

        # ── Registra etapa 1 na timeline ──
        DiariaService.registrar_movimentacao(
            itinerario.id,
            DiariasEtapaID.SOLICITACAO_INICIAL,
            current_user.id,
            'Solicitacao criada pelo usuario',
        )

        # ── Integração SEI em background (não bloqueia o response) ──
        precisa_sei = tipo in (2, 3) and tipo_solicitacao_id in (TIPO_SOL_DIARIAS_PASSAGENS, TIPO_SOL_APENAS_PASSAGENS)
        if precisa_sei:
            app = current_app._get_current_object()
            usuario_id = current_user.id
            t = threading.Thread(
                target=_executar_sei_background,
                args=(app, itinerario.id, pessoas, dados, tipo_solicitacao_id,
                      justificativa_memorando, objetivo, arquivo_externo, usuario_id),
                daemon=True,
            )
            t.start()
        else:
            # Sem SEI: notifica de forma síncrona (rápido)
            try:
                DiariasNotifier.notificar_etapa(itinerario, 'nova_solicitacao', current_user.id)
            except Exception as exc_notif:
                current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

        if is_ajax:
            return jsonify({
                'success': True,
                'redirect': url_for('diarias.dashboard'),
                'message': 'Solicitacao criada com sucesso!',
                'sei_background': precisa_sei,
            })

        flash('Solicitação de diária criada com sucesso!', 'success')
        return redirect(url_for('diarias.dashboard'))

    except Exception as e:
        import traceback
        current_app.logger.error(f'[DIARIAS] Erro ao criar solicitacao: {e}\n{traceback.format_exc()}')
        if is_ajax:
            return jsonify({'success': False, 'error': str(e)}), 500
        flash(f'Erro ao criar solicitação: {str(e)}', 'danger')
        return redirect(url_for('diarias.nova'))


def _integrar_sei_diarias(itinerario, pessoas, dados, tipo_solicitacao_id,
                          justificativa_memorando, objetivo, arquivo_externo=None):
    """
    Executa a integração SEI para viagens Nacionais com passagens.
    Cria procedimento + memorando SGA + requisição de diárias + documento externo no SEI.
    """
    from app.services.diarias_sei_integration import criar_processo_diarias_completo
    from app.models.diaria import Estado, DiariasCargo
    from app.extensions import db

    try:
        tipo_itinerario = dados.get('tipo_itinerario', 2)

        # Pré-carrega todos os cargos necessários em uma única query (evita N+1)
        cargo_ids = set()
        for p in pessoas:
            if p.get('cargo_id'):
                cargo_ids.add(int(p['cargo_id']))
            if p.get('cargo_assessorado_id'):
                cargo_ids.add(int(p['cargo_assessorado_id']))
        cargos_map = {}
        if cargo_ids:
            cargos_map = {c.id: c for c in DiariasCargo.query.filter(DiariasCargo.id.in_(cargo_ids)).all()}

        # Monta dados dos servidores a partir dos dados da API (passados via form)
        servidores_sei = []
        primeira_matricula = ''
        for p in pessoas:
            mat = p.get('matricula', '')
            if not primeira_matricula:
                primeira_matricula = mat

            # Busca nome do cargo (diarias_cargos) e valor da diária
            cargo_id = p.get('cargo_id')
            cargo_assessorado_id = p.get('cargo_assessorado_id')
            cargo_obj = cargos_map.get(int(cargo_id)) if cargo_id else None
            cargo_nome = cargo_obj.nome if cargo_obj else p.get('cargo_folha', '')

            # Se assessorando, usa o cargo do assessorado para cálculo do valor
            cargo_para_calculo = cargo_assessorado_id or cargo_id
            valor_unitario = float(DiariaService.get_valor_cargo(cargo_para_calculo, tipo_itinerario)) if cargo_para_calculo else 0.0
            valor_total_pessoa = valor_unitario * float(itinerario.qtd_diarias_solicitadas)

            # Busca nome do cargo assessorado para exibição no SEI
            cargo_assessorado_obj = cargos_map.get(int(cargo_assessorado_id)) if cargo_assessorado_id else None
            cargo_assessorado_nome = cargo_assessorado_obj.nome if cargo_assessorado_obj else None

            # Dados bancários da API pessoaSGA
            banco_agencia = p.get('banco_agencia', '')
            banco_conta = p.get('banco_conta', '')

            servidores_sei.append({
                'matricula': mat,
                'cpf': p.get('cpf', ''),
                'nome': p.get('nome', ''),
                'cargo': cargo_nome,
                'cargo_assessorado': cargo_assessorado_nome,
                'vinculo': p.get('vinculo', ''),
                'banco': '',
                'agencia': banco_agencia,
                'conta': banco_conta,
                'valor_unitario': valor_unitario,
                'valor_total_pessoa': valor_total_pessoa,
            })

        primeiro_cargo = pessoas[0].get('cargo_id') if pessoas else None
        cargo_principal_obj = cargos_map.get(int(primeiro_cargo)) if primeiro_cargo else None

        dados_servidor = {
            'cargo': cargo_principal_obj.nome if cargo_principal_obj else (pessoas[0].get('cargo_folha', 'Servidor') if pessoas else 'Servidor'),
            'matricula': primeira_matricula,
        }

        # Nome do tipo de solicitação
        tipo_sol = DiariasTipoSolicitacao.query.get(tipo_solicitacao_id)
        tipo_sol_nome = tipo_sol.nome if tipo_sol else 'Diárias + Passagens Aéreas'

        # Monta trecho (Estado Origem → Estado Destino)
        estado_orig = Estado.query.get(int(dados.get('estado_origem', 22) or 22))
        estado_dest = Estado.query.get(int(dados.get('estado_destino', 0) or 0))
        trecho = ''
        if estado_orig and estado_dest:
            trecho = estado_orig.nome + ' - ' + estado_dest.nome

        tipo_itinerario = dados.get('tipo_itinerario', 2)
        dados_itinerario = {
            'tipo_solicitacao_nome': tipo_sol_nome,
            'tipo_itinerario_nome': 'Internacional' if tipo_itinerario == 3 else 'Nacional',
            'data_viagem': dados.get('data_viagem'),
            'data_retorno': dados.get('data_retorno'),
        }

        dados_requisicao = {
            'objetivo': objetivo or '',
            'servidores': servidores_sei,
            'qtd_diarias': itinerario.qtd_diarias_solicitadas,
            'trecho': trecho,
        }

        resultado = criar_processo_diarias_completo(
            dados_itinerario, dados_servidor, justificativa_memorando,
            dados_requisicao=dados_requisicao,
            arquivo_externo=arquivo_externo,
            tipo_solicitacao_id=tipo_solicitacao_id,
        )

        if resultado['sucesso']:
            # Salva dados do SEI no itinerário
            proc = resultado['procedimento']
            memo = resultado['memorando']
            req = resultado.get('requisicao')

            itinerario.sei_protocolo = resultado.get('protocolo', '')
            itinerario.sei_id_procedimento = str(proc.get('IdProcedimento', '')) if proc else None

            # Documentos SEI vão na tabela normalizada
            if memo:
                itinerario.set_doc('memorando',
                                   sei_id=str(memo.get('IdDocumento', '')),
                                   sei_formatado=str(memo.get('DocumentoFormatado', '')))

            if req:
                itinerario.set_doc('requisicao',
                                   sei_id=str(req.get('IdDocumento', '')),
                                   sei_formatado=str(req.get('DocumentoFormatado', '')))

            req_pass = resultado.get('requisicao_passagens')
            if req_pass:
                itinerario.set_doc('requisicao_passagens',
                                   sei_id=str(req_pass.get('IdDocumento', '')),
                                   sei_formatado=str(req_pass.get('DocumentoFormatado', '')))

            doc_ext = resultado.get('doc_externo')
            if doc_ext:
                itinerario.set_doc('doc_externo',
                                   sei_id=str(doc_ext.get('IdDocumento', '')),
                                   sei_formatado=str(doc_ext.get('DocumentoFormatado', '')))

            # Atualiza n_processo com o protocolo SEI se não tinha processo informado
            if not itinerario.n_processo and resultado.get('protocolo'):
                itinerario.n_processo = resultado['protocolo']

            db.session.commit()
            current_app.logger.info(
                f"SEI Diárias: Integração concluída para itinerário {itinerario.id} - "
                f"Protocolo: {resultado.get('protocolo')}"
            )
        else:
            current_app.logger.warning(
                f"SEI Diárias: Integração falhou para itinerário {itinerario.id}: "
                f"{resultado.get('erro')}"
            )

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"SEI Diárias: Erro na integração para itinerário {itinerario.id}: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())


@diarias_bp.route('/detalhes/<int:id>')
@login_required
@requires_permission('diarias.visualizar')
def detalhes(id):
    """Visualização detalhada de uma solicitação."""
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        flash('Solicitação não encontrada.', 'warning')
        return redirect(url_for('diarias.dashboard'))

    # Monta timeline
    timeline_data = DiariaService.obter_timeline(dados['itinerario'])

    return render_template('diarias/detalhes.html',
        itinerario=dados['itinerario'],
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        timeline_data=timeline_data,
    )


@diarias_bp.route('/atender/<int:id>')
@login_required
@requires_permission('diarias.aprovar')
def atender(id):
    """Página de análise/aprovação de uma solicitação."""
    dados = DiariaService.get_itinerario_completo(id)
    if not dados:
        flash('Solicitação não encontrada.', 'warning')
        return redirect(url_for('diarias.todas'))

    return render_template('diarias/atender.html',
        itinerario=dados['itinerario'],
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        cotacoes_voos=dados.get('cotacoes_voos', []),
        agencias=DiariaService.get_agencias(),
    )


@diarias_bp.route('/atender/<int:id>/update', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def update_atendimento(id):
    """Processa a aprovação/rejeição de um itinerário."""
    try:
        conclusao = request.form.get('conclusao')
        if not conclusao:
            flash('Selecione uma conclusão.', 'danger')
            return redirect(url_for('diarias.atender', id=id))

        # Cotações por pessoa (para viagens nacionais)
        cotacoes_pessoas = {}
        for key, value in request.form.items():
            if key.startswith('cotacoes_pessoas['):
                item_id = key.split('[')[1].split(']')[0]
                cotacoes_pessoas[item_id] = value

        DiariaService.atender_itinerario(id, conclusao, cotacoes_pessoas or None)
        flash('Solicitação atualizada com sucesso!', 'success')
        return redirect(url_for('diarias.todas'))

    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('diarias.atender', id=id))
    except Exception as e:
        flash(f'Erro ao processar: {str(e)}', 'danger')
        return redirect(url_for('diarias.atender', id=id))


@diarias_bp.route('/<int:id>/gerar-relatorio', methods=['POST'])
@login_required
@requires_permission('diarias.visualizar')
def gerar_relatorio(id):
    """Gera o Relatório de Viagem (IdSerie 1908) no processo SEI.

    Disponível para o solicitante após a OB ter sido inserida no processo.
    O relatório contém dados do servidor, dados da viagem e o relato preenchido pelo viajante.
    """
    from app.services.diarias_sei_integration import gerar_relatorio_viagem
    from app.extensions import db

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Valida: OB deve existir
    if not itinerario.has_doc('ob'):
        return jsonify({'success': False, 'error': 'A Ordem Bancária (OB) ainda não foi inserida no processo.'}), 400

    # Valida: relatório ainda não gerado
    if itinerario.has_doc('relatorio_viagem'):
        return jsonify({'success': False, 'error': 'O Relatório de Viagem já foi gerado para esta solicitação.'}), 400

    # Obtém dados do formulário
    relato = request.form.get('relato', '').strip()
    if not relato:
        return jsonify({'success': False, 'error': 'O relato da viagem é obrigatório.'}), 400

    # Credenciais do usuário para assinatura
    usuario_sei = request.form.get('usuario_sei', '').strip()
    senha_sei = request.form.get('senha_sei', '').strip()
    if not usuario_sei or not senha_sei:
        return jsonify({'success': False, 'error': 'Credenciais SEI são obrigatórias para assinar o documento.'}), 400

    # Monta dados do relatório a partir do itinerário
    primeiro_item = itinerario.itens.first()
    if not primeiro_item:
        return jsonify({'success': False, 'error': 'Nenhuma pessoa encontrada na solicitação.'}), 400

    # Formata valor da diária individual
    valor_cargo = primeiro_item.valor_cargo or 0
    valor_diaria_fmt = f'R${valor_cargo:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    # Formata valor total
    valor_total = itinerario.valor_total or 0
    valor_total_fmt = f'R$ {valor_total:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

    # Monta trajeto
    origem_nome = ''
    destino_nome = ''
    if itinerario.municipio_origem_obj:
        origem_nome = itinerario.municipio_origem_obj.nome
    if itinerario.estado_origem_obj:
        origem_nome = f'{origem_nome}-{itinerario.estado_origem_obj.nome}' if origem_nome else itinerario.estado_origem_obj.nome
    if itinerario.estado_destino_obj:
        destino_nome = itinerario.estado_destino_obj.nome

    trajeto = f'{origem_nome}/{destino_nome}/{origem_nome}' if origem_nome and destino_nome else 'N/A'

    # Formata período
    periodo_inicio = itinerario.data_viagem.strftime('%d/%m/%Y') if itinerario.data_viagem else ''
    periodo_fim = itinerario.data_retorno.strftime('%d/%m/%Y') if itinerario.data_retorno else ''

    # Lotação (setor/orgão do servidor)
    lotacao = primeiro_item.setor or primeiro_item.orgao or ''
    if primeiro_item.orgao and primeiro_item.setor:
        lotacao = f'{primeiro_item.orgao} / {primeiro_item.setor}'

    # Cargo/Função
    cargo_funcao = primeiro_item.cargo.nome if primeiro_item.cargo else (primeiro_item.cargo_folha or '')

    dados_relatorio = {
        'nome': primeiro_item.nome_pessoa or '',
        'matricula': primeiro_item.matricula_pessoa or '',
        'cpf': primeiro_item.cpf_pessoa or '',
        'lotacao': lotacao,
        'cargo_funcao': cargo_funcao,
        'periodo_inicio': periodo_inicio,
        'periodo_fim': periodo_fim,
        'qtd_diarias': str(itinerario.qtd_diarias_solicitadas or ''),
        'valor_diaria': valor_diaria_fmt,
        'valor_total': valor_total_fmt,
        'trajeto': trajeto,
        'relato': relato,
    }

    cargo_sei = request.form.get('cargo_sei', '').strip() or cargo_funcao

    try:
        # 1. Autentica o usuario para assinar
        auth = autenticar_usuario_sei(usuario_sei, senha_sei)
        if not auth or not auth.get('token'):
            return jsonify({'success': False, 'error': 'Falha na autenticacao SEI. Verifique suas credenciais.'}), 401

        # 2. Token admin para criar o documento
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'success': False, 'error': 'Falha ao obter token administrativo SEI.'}), 500

        # 3. Gera o documento
        retorno = gerar_relatorio_viagem(
            token_admin,
            itinerario.sei_id_procedimento,
            itinerario.sei_protocolo,
            dados_relatorio,
        )

        if not retorno:
            return jsonify({'success': False, 'error': 'Erro ao gerar documento no SEI.'}), 500

        id_documento = str(retorno.get('IdDocumento', ''))
        doc_formatado = retorno.get('DocumentoFormatado', '')

        # 4. Assina o documento com credenciais do usuario
        from app.services.diarias_sei_integration import UNIDADE_SEAD
        resultado_assinatura = assinar_documento(
            token=auth['token'],
            unidade_id=UNIDADE_SEAD,
            dados_assinatura={
                'protocolo_doc': id_documento,
                'orgao': 'SEAD-PI',
                'cargo': cargo_sei,
                'id_login': auth['id_login'],
                'id_usuario': auth['id_usuario'],
                'senha': senha_sei,
            }
        )

        aviso = None
        if not resultado_assinatura.get('sucesso'):
            aviso = f'Documento gerado mas assinatura falhou: {resultado_assinatura.get("erro", "")}'

        # 5. Salva no banco
        itinerario.set_doc('relatorio_viagem', sei_id=id_documento, sei_formatado=doc_formatado)
        db.session.commit()

        try:
            DiariasNotifier.notificar_etapa(itinerario, 'relatorio_viagem', current_user.id)
        except Exception as exc_notif:
            current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')

        resp = {
            'success': True,
            'message': f'Relatório de Viagem gerado com sucesso! ({doc_formatado})',
            'documento_formatado': doc_formatado,
            'id_documento': id_documento,
        }
        if aviso:
            resp['aviso'] = aviso
        return jsonify(resp)

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erro ao gerar relatório de viagem: {e}")
        return jsonify({'success': False, 'error': f'Erro inesperado: {str(e)}'}), 500


@diarias_bp.route('/<int:id>/upload-comprovante', methods=['POST'])
@login_required
@requires_permission('diarias.visualizar')
def upload_comprovante(id):
    """Upload do Comprovante de Viagem (IdSerie 35) ao processo SEI.

    Disponível para o solicitante após gerar o Relatório de Viagem.
    Após upload, o processo avança para a etapa de Prestação de Contas na CCDP.
    """
    from app.services.diarias_sei_integration import (
        adicionar_documento_externo, ID_SERIE_COMPROVANTE_VIAGEM,
    )
    from app.extensions import db

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Valida: relatório já gerado
    if not itinerario.has_doc('relatorio_viagem'):
        flash('O Relatório de Viagem deve ser gerado antes de enviar o comprovante.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    # Valida: comprovante não já enviado
    if itinerario.has_doc('comprovante_viagem'):
        flash('O comprovante de viagem já foi enviado.', 'warning')
        return redirect(url_for('diarias.detalhes', id=id))

    arquivo = request.files.get('arquivo_comprovante')
    if not arquivo or not arquivo.filename:
        flash('Selecione um arquivo PDF para enviar.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB

    try:
        arquivo_bytes = arquivo.read()
        if not arquivo_bytes:
            flash('Arquivo vazio.', 'danger')
            return redirect(url_for('diarias.detalhes', id=id))

        if len(arquivo_bytes) > MAX_UPLOAD_SIZE:
            flash('Arquivo excede o tamanho máximo de 10MB.', 'danger')
            return redirect(url_for('diarias.detalhes', id=id))

        token = gerar_token_sei_admin()
        if not token:
            flash('Falha na autenticação SEI.', 'danger')
            return redirect(url_for('diarias.detalhes', id=id))

        retorno = adicionar_documento_externo(
            token=token,
            protocolo_formatado=itinerario.sei_protocolo,
            arquivo_bytes=arquivo_bytes,
            nome_arquivo=arquivo.filename,
            descricao='Comprovante de Realização da Viagem',
            id_serie=ID_SERIE_COMPROVANTE_VIAGEM,
        )

        if retorno:
            itinerario.set_doc('comprovante_viagem',
                               sei_id=str(retorno.get('IdDocumento', '')),
                               sei_formatado=retorno.get('DocumentoFormatado', ''))

            # Avança etapa para Prestação de Contas (§1.1 — commit único)
            DiariaService.registrar_movimentacao(
                itinerario.id,
                DiariasEtapaID.PRESTACAO_CONTAS,
                current_user.id,
                'Comprovante de viagem enviado pelo solicitante',
                auto_commit=False,
            )

            db.session.commit()
            try:
                DiariasNotifier.notificar_etapa(itinerario, 'comprovante_viagem', current_user.id)
            except Exception as exc_notif:
                current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')
            flash('Comprovante de viagem enviado ao SEI com sucesso!', 'success')
        else:
            flash('Erro ao enviar comprovante ao SEI.', 'danger')

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erro ao enviar comprovante: {e}")
        flash(f'Erro ao enviar comprovante: {str(e)}', 'danger')

    return redirect(url_for('diarias.detalhes', id=id))
