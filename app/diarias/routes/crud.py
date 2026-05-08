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


EXTENSOES_PERMITIDAS = {'.pdf', '.png', '.jpg', '.jpeg', '.doc', '.docx', '.xls', '.xlsx'}
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB


def _validar_arquivo(arquivo, campo_nome='arquivo'):
    """Valida extensão e tamanho de arquivo enviado. Retorna (bytes, erro)."""
    if not arquivo or not arquivo.filename:
        return None, None  # Arquivo opcional não enviado
    ext = '.' + arquivo.filename.rsplit('.', 1)[-1].lower() if '.' in arquivo.filename else ''
    if ext not in EXTENSOES_PERMITIDAS:
        return None, f'{campo_nome}: extensão "{ext}" não permitida. Aceitas: {", ".join(sorted(EXTENSOES_PERMITIDAS))}'
    conteudo = arquivo.read()
    if not conteudo:
        return None, f'{campo_nome}: arquivo vazio.'
    if len(conteudo) > MAX_UPLOAD_SIZE:
        return None, f'{campo_nome}: excede o tamanho máximo de 10MB.'
    return conteudo, None


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
        cargos=DiariaService.get_cargos(apenas_com_valor=True),
        valores_cargo_json=json.dumps(valores_map),
        unidades_sei=current_user.unidades_sei,
    )


def _executar_sei_background(app, itinerario_id, pessoas, dados, tipo_solicitacao_id,
                             justificativa_memorando, objetivo, arquivo_externo, usuario_id,
                             unidade_sei_id=None, justificativa_solicitante=None):
    """Executa integração SEI + notificação em thread separada para não bloquear o request."""
    from app.extensions import db

    with app.app_context():
        sei_ok = False
        itinerario = None
        try:
            itinerario = DiariasItinerario.query.get(itinerario_id)
            if not itinerario:
                return

            _integrar_sei_diarias(itinerario, pessoas, dados, tipo_solicitacao_id,
                                  justificativa_memorando, objetivo, arquivo_externo,
                                  unidade_sei_id=unidade_sei_id,
                                  justificativa_solicitante=justificativa_solicitante)
            # CRIT-03: Verifica se a integração de fato salvou o protocolo
            db.session.refresh(itinerario)
            sei_ok = bool(itinerario.sei_protocolo)
        except Exception as e:
            current_app.logger.error(f'[DIARIAS] Erro SEI background: {e}')
        finally:
            db.session.remove()

        # CRIT-03: Só notifica se SEI teve sucesso ou se não precisava de SEI
        with app.app_context():
            try:
                if not itinerario:
                    itinerario = DiariasItinerario.query.get(itinerario_id)
                if itinerario:
                    DiariasNotifier.notificar_etapa(itinerario, 'nova_solicitacao', usuario_id)
                    if not sei_ok:
                        # Notifica admins sobre falha na integração SEI
                        current_app.logger.warning(
                            f'[DIARIAS] Integração SEI falhou para itinerário {itinerario_id}. '
                            f'Notificação enviada mas processo SEI pode não existir.'
                        )
            except Exception as exc_notif:
                current_app.logger.warning(f'[DIARIAS] Falha ao enviar notificacao: {exc_notif}')
            finally:
                db.session.remove()


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

        # Validar consistência dos arrays — todos devem ter o mesmo tamanho que pessoas_cpf
        n_pessoas = len(pessoas_cpf)
        arrays_obrigatorios = [
            (pessoas_nome, 'nomes'),
            (pessoas_cargo_id, 'cargos'),
        ]
        for arr, label in arrays_obrigatorios:
            if len(arr) != n_pessoas:
                msg = f'Dados inconsistentes ({label}: {len(arr)}, pessoas: {n_pessoas}). Recarregue a página.'
                if is_ajax:
                    return jsonify({'success': False, 'error': msg}), 400
                flash(msg, 'danger')
                return redirect(url_for('diarias.nova'))

        pessoas = []
        for i in range(n_pessoas):
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
        unidade_sei_id = request.form.get('unidade_sei_id', '').strip() or None

        # Arquivo anexo (documento externo SEI) — com validação de extensão/tamanho
        arquivo_anexo = request.files.get('arquivo_anexo')
        arquivo_externo = None
        if arquivo_anexo and arquivo_anexo.filename:
            arq_bytes, arq_erro = _validar_arquivo(arquivo_anexo, 'Arquivo anexo')
            if arq_erro:
                if is_ajax:
                    return jsonify({'success': False, 'error': arq_erro}), 400
                flash(arq_erro, 'danger')
                return redirect(url_for('diarias.nova'))
            if arq_bytes:
                arquivo_externo = {
                    'bytes': arq_bytes,
                    'nome_arquivo': arquivo_anexo.filename,
                    'descricao': 'Documento anexo - Solicitacao de Diarias',
                }

        dados = {
            'tipo_solicitacao_id': tipo_solicitacao_id,
            'tipo_itinerario': tipo,
            'data_viagem': request.form.get('data_viagem'),
            'data_retorno': request.form.get('data_retorno'),
            'usuario_gerador': current_user.sigla_login,
            'estado_origem': request.form.get('estado_origem'),
            'estado_destino': request.form.get('estado_destino'),
            'objetivo': objetivo,
            'unidade_sei_id': unidade_sei_id,
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
        precisa_sei = True  # Todos os tipos criam processo SEI
        if precisa_sei:
            app = current_app._get_current_object()
            usuario_id = current_user.id
            t = threading.Thread(
                target=_executar_sei_background,
                args=(app, itinerario.id, pessoas, dados, tipo_solicitacao_id,
                      justificativa_memorando, objetivo, arquivo_externo, usuario_id,
                      unidade_sei_id, justificativa),
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
                          justificativa_memorando, objetivo, arquivo_externo=None,
                          unidade_sei_id=None, justificativa_solicitante=None):
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
            'sei_protocolo': itinerario.sei_protocolo,  # CRIT-01: passa para verificação de duplicata
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
            unidade_sei_id=unidade_sei_id,
            justificativa_solicitante=justificativa_solicitante,
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
        cotacoes_voos=dados.get('cotacoes_voos', []),
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

    timeline_data = DiariaService.obter_timeline(dados['itinerario'])

    return render_template('diarias/atender.html',
        itinerario=dados['itinerario'],
        itens=dados['itens'],
        paradas=dados['paradas'],
        cotacoes=dados['cotacoes'],
        cotacoes_voos=dados.get('cotacoes_voos', []),
        agencias=DiariaService.get_agencias(),
        timeline_data=timeline_data,
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
@requires_permission('diarias.criar')
def gerar_relatorio(id):
    """Gera o Relatório de Viagem (IdSerie 1908) no processo SEI.

    Disponível para o solicitante após a OB ter sido inserida no processo.
    O relatório contém dados do servidor, dados da viagem e o relato preenchido pelo viajante.
    """
    from app.services.diarias_sei_integration import gerar_relatorio_viagem
    from app.extensions import db

    itinerario = DiariasItinerario.query.get_or_404(id)

    # Valida: todas as OBs (1 por servidor) devem estar inseridas.
    # Nao usa has_doc('ob') porque o marcador agregado carrega apenas `codigo`,
    # sem `sei_id` (padrao pos-refactor 1-por-servidor). Consulta direta:
    from app.models.diaria import DiariasOrdemBancaria, DiariasItemItinerario
    total_serv_ob = DiariasItemItinerario.query.filter_by(id_itinerario=itinerario.id).count()
    total_obs = DiariasOrdemBancaria.query.filter_by(itinerario_id=itinerario.id).count()
    if total_serv_ob == 0 or total_obs < total_serv_ob:
        return jsonify({
            'success': False,
            'error': f'Todas as Ordens Bancárias precisam ter sido inseridas ({total_obs}/{total_serv_ob} servidores com OB).',
        }), 400

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

    # HIGH-04: Monta dados do relatório com TODAS as pessoas (não apenas a primeira)
    from app.models.diaria import DiariasItemItinerario
    todos_itens = DiariasItemItinerario.query.filter_by(
        id_itinerario=itinerario.id
    ).order_by(DiariasItemItinerario.id).all()

    if not todos_itens:
        return jsonify({'success': False, 'error': 'Nenhuma pessoa encontrada na solicitação.'}), 400

    primeiro_item = todos_itens[0]

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

    # Monta nomes e dados de todos os servidores
    nomes_todos = ', '.join(item.nome_pessoa for item in todos_itens if item.nome_pessoa)
    cpfs_todos = ', '.join(item.cpf_pessoa for item in todos_itens if item.cpf_pessoa)
    matriculas_todos = ', '.join(item.matricula_pessoa for item in todos_itens if item.matricula_pessoa)

    # Lotação (setor/orgão do primeiro servidor como referência)
    lotacao = primeiro_item.setor or primeiro_item.orgao or ''
    if primeiro_item.orgao and primeiro_item.setor:
        lotacao = f'{primeiro_item.orgao} / {primeiro_item.setor}'

    # Cargo/Função (do primeiro — para assinatura)
    cargo_funcao = primeiro_item.cargo.nome if primeiro_item.cargo else (primeiro_item.cargo_folha or '')

    # Valor diária por pessoa (mostra todas)
    valores_diaria = []
    for item in todos_itens:
        vc = item.valor_cargo or 0
        vc_fmt = f'R${vc:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
        valores_diaria.append(f'{item.nome_pessoa}: {vc_fmt}')
    valor_diaria_fmt = '; '.join(valores_diaria) if len(todos_itens) > 1 else (
        f'R${(primeiro_item.valor_cargo or 0):,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
    )

    dados_relatorio = {
        'nome': nomes_todos,
        'matricula': matriculas_todos,
        'cpf': cpfs_todos,
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

    # Protocolo do processo (usado para bypass por processo em auth e assinatura)
    protocolo_proc = itinerario.sei_protocolo or itinerario.n_processo or ''

    try:
        # 1. Autentica o usuario para assinar (aceita bypass por protocolo em testes)
        auth = autenticar_usuario_sei(usuario_sei, senha_sei, protocolo_bypass=protocolo_proc)
        if not auth or not auth.get('token'):
            return jsonify({'success': False, 'error': 'Falha na autenticacao SEI. Verifique suas credenciais.'}), 401

        # 2. Token admin para criar o documento
        token_admin = gerar_token_sei_admin()
        if not token_admin:
            return jsonify({'success': False, 'error': 'Falha ao obter token administrativo SEI.'}), 500

        # 3. Envia o processo para UNIDADE_SEAD antes de criar o documento — o
        #    SEI exige que o processo esteja visivel na unidade onde sera criado
        #    o doc (caso contrario, assinatura falha com "Documento nao encontrado").
        from app.services.diarias_sei_integration import UNIDADE_SEAD, enviar_procedimento
        enviar_procedimento(
            token_admin, protocolo_proc, [UNIDADE_SEAD], manter_aberto=True
        )

        # 4. Gera o documento em UNIDADE_SEAD
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

        # 5. Assina o documento com credenciais do usuario (bypass por protocolo)
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
            },
            protocolo_proc=protocolo_proc,
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


@diarias_bp.route('/<int:id>/escolha-passagens', methods=['POST'])
@login_required
@requires_permission('diarias.criar')
def escolha_passagens_solicitante(id):
    """Solicitante escolhe os voos (ida + volta). Mesma lógica do admin mas acessível ao criador."""
    from app.models.diaria import DiariasCotacaoVoo
    from app.services.diarias_sei_integration import (
        gerar_token_sei_admin, gerar_escolha_passagens,
        gerar_memorando_cotacoes, consultar_documentos_procedimento,
        ID_SERIE_COTACAO,
    )
    from app.extensions import db

    itinerario = DiariasItinerario.query.get_or_404(id)

    if itinerario.tipo_itinerario not in [2, 3]:
        flash('Escolha de passagens só se aplica a viagens nacionais/internacionais.', 'warning')
        return redirect(url_for('diarias.detalhes', id=id))

    if itinerario.escolha_voo_ida_id:
        flash('A escolha de passagens já foi realizada.', 'warning')
        return redirect(url_for('diarias.detalhes', id=id))

    if itinerario.etapa_atual_id not in (DiariasEtapaID.ESCOLHA_VOO, DiariasEtapaID.ANALISE_SOLICITACAO):
        flash('Esta solicitação não está na etapa de escolha de voo.', 'warning')
        return redirect(url_for('diarias.detalhes', id=id))

    voo_ida_id = request.form.get('escolha_voo_ida', type=int)
    voo_volta_id = request.form.get('escolha_voo_volta', type=int)

    if not voo_ida_id or not voo_volta_id:
        flash('Selecione um voo de IDA e um voo de VOLTA.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    voo_ida = DiariasCotacaoVoo.query.get(voo_ida_id)
    voo_volta = DiariasCotacaoVoo.query.get(voo_volta_id)

    if not voo_ida or voo_ida.itinerario_id != id or voo_ida.tipo_trecho != 'ida':
        flash('Voo de IDA inválido.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    if not voo_volta or voo_volta.itinerario_id != id or voo_volta.tipo_trecho != 'volta':
        flash('Voo de VOLTA inválido.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    # Detecta menor valor
    all_ida = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=id, tipo_trecho='ida'
    ).order_by(DiariasCotacaoVoo.valor.asc()).all()
    all_volta = DiariasCotacaoVoo.query.filter_by(
        itinerario_id=id, tipo_trecho='volta'
    ).order_by(DiariasCotacaoVoo.valor.asc()).all()

    menor_ida = all_ida[0].valor if all_ida else None
    menor_volta = all_volta[0].valor if all_volta else None
    is_cheapest = (voo_ida.valor <= menor_ida and voo_volta.valor <= menor_volta)

    # Justificativa (obrigatória se não for menor valor)
    justificativa_codigos = []
    justificativa_outros = None
    if not is_cheapest:
        for code in ['J1', 'J2', 'J3', 'J4', 'J5']:
            if request.form.get(f'justificativa_{code}'):
                justificativa_codigos.append(code)
        justificativa_outros = request.form.get('justificativa_outros_texto', '').strip() or None
        if not justificativa_codigos and not justificativa_outros:
            flash('Justificativa obrigatória quando o voo escolhido não é o menor valor.', 'danger')
            return redirect(url_for('diarias.detalhes', id=id))

    declaracao = bool(request.form.get('declaracao_responsabilidade'))

    # Salva escolha
    itinerario.escolha_voo_ida_id = voo_ida_id
    itinerario.escolha_voo_volta_id = voo_volta_id
    itinerario.escolha_menor_valor = is_cheapest
    itinerario.escolha_justificativa_codigos = ','.join(justificativa_codigos) if justificativa_codigos else None
    itinerario.escolha_justificativa_outros = justificativa_outros
    itinerario.escolha_declaracao_responsabilidade = declaracao

    # Gera documentos SEI
    if itinerario.sei_id_procedimento:
        try:
            token = gerar_token_sei_admin()
            if token:
                sei_protocolo = itinerario.sei_protocolo or itinerario.n_processo or ''

                retorno = gerar_escolha_passagens(
                    token=token,
                    id_procedimento=itinerario.sei_id_procedimento,
                    dados_escolha={
                        'voos_ida': all_ida, 'voos_volta': all_volta,
                        'escolha_ida_id': voo_ida_id, 'escolha_volta_id': voo_volta_id,
                        'menor_valor': is_cheapest,
                        'justificativa_codigos': justificativa_codigos,
                        'justificativa_outros_texto': justificativa_outros,
                        'declaracao': declaracao,
                    },
                    sei_protocolo=sei_protocolo,
                )
                if retorno:
                    itinerario.set_doc('escolha_passagens',
                        sei_id=str(retorno.get('IdDocumento', '')),
                        sei_formatado=retorno.get('DocumentoFormatado', ''))

                    # 2º Memorando SGA
                    ref_cotacoes = ''
                    resp_docs = consultar_documentos_procedimento(sei_protocolo)
                    if resp_docs.get('sucesso'):
                        ids_cotacao = [
                            d.get('DocumentoFormatado', '')
                            for d in resp_docs['documentos']
                            if str(d.get('Serie', {}).get('IdSerie', '')) == ID_SERIE_COTACAO
                        ]
                        ref_cotacoes = ', '.join(ids_cotacao) if ids_cotacao else ''

                    doc_req_pass = itinerario.get_doc('requisicao_passagens')
                    ref_req = (doc_req_pass.sei_formatado if doc_req_pass else '') or ''
                    if ref_cotacoes and ref_req:
                        ret_memo = gerar_memorando_cotacoes(
                            token=token,
                            id_procedimento=itinerario.sei_id_procedimento,
                            sei_protocolo=sei_protocolo,
                            ref_cotacoes_fmt=ref_cotacoes,
                            ref_requisicao_passagens_fmt=ref_req,
                        )
                        if ret_memo:
                            itinerario.set_doc('memorando_cotacoes',
                                sei_id=str(ret_memo.get('IdDocumento', '')),
                                sei_formatado=ret_memo.get('DocumentoFormatado', ''))
        except Exception as e:
            current_app.logger.error(f'[DIARIAS] Erro SEI escolha passagens: {e}')
            flash(f'Escolha salva, mas erro na integração SEI: {e}', 'warning')

    # Escolha de passagens concluída → avança para Análise 2ª Parte (etapa 6)
    if itinerario.etapa_atual_id == DiariasEtapaID.ESCOLHA_VOO:
        DiariaService.registrar_movimentacao(
            id_itinerario=id,
            etapa_nova_id=DiariasEtapaID.ANALISE_SOLICITACAO_2,
            usuario_id=current_user.id,
            comentario='Escolha de passagens registrada. Avançando para Análise 2ª Parte.',
            auto_commit=False,
        )
    db.session.commit()

    try:
        DiariasNotifier.notificar_etapa(itinerario, 'escolha_passagens', current_user.id)
    except Exception as e:
        current_app.logger.warning(f'[DIARIAS] Falha ao notificar escolha_passagens: {e}')

    flash('Escolha de passagens registrada com sucesso!', 'success')
    return redirect(url_for('diarias.detalhes', id=id))


@diarias_bp.route('/<int:id>/upload-comprovante', methods=['POST'])
@login_required
@requires_permission('diarias.criar')
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
    arquivo_bytes, arq_erro = _validar_arquivo(arquivo, 'Comprovante')
    if arq_erro:
        flash(arq_erro, 'danger')
        return redirect(url_for('diarias.detalhes', id=id))
    if not arquivo_bytes:
        flash('Selecione um arquivo para enviar.', 'danger')
        return redirect(url_for('diarias.detalhes', id=id))

    try:

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
