"""
Rotas CRUD do módulo de Diárias (criar, visualizar, atender).
"""
import json
from flask import render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required, current_user

from app.diarias.routes import diarias_bp
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.models.diaria import DiariasTipoSolicitacao, DiariasValorCargo
from app.constants import DiariasEtapaID


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


@diarias_bp.route('/store', methods=['POST'])
@login_required
@requires_permission('diarias.criar')
def store():
    """Salva nova solicitação de diária."""
    try:
        tipo = int(request.form.get('tipo_itinerario', 0))
        if tipo not in (1, 2):
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

        tipo_solicitacao_id = int(request.form.get('tipo_solicitacao', 0))

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
            print(f"[DEBUG SEI] store(): Arquivo recebido: {arquivo_anexo.filename}, "
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
            DiariasEtapaID.SOLICITACAO_INICIADA,
            current_user.id,
            'Solicitacao criada pelo usuario',
        )

        # ── Integração SEI: Nacional + Passagens (Diárias+Passagens ou Apenas Passagens) ──
        print(f"[DEBUG SEI] store(): tipo={tipo}, tipo_solicitacao_id={tipo_solicitacao_id}")
        print(f"[DEBUG SEI] store(): condicao SEI = {tipo == 2 and tipo_solicitacao_id in (TIPO_SOL_DIARIAS_PASSAGENS, TIPO_SOL_APENAS_PASSAGENS)}")
        if tipo == 2 and tipo_solicitacao_id in (TIPO_SOL_DIARIAS_PASSAGENS, TIPO_SOL_APENAS_PASSAGENS):
            print(f"[DEBUG SEI] store(): Entrando na integracao SEI...")
            _integrar_sei_diarias(itinerario, pessoas, dados, tipo_solicitacao_id,
                                  justificativa_memorando, objetivo, arquivo_externo)

        flash('Solicitação de diária criada com sucesso!', 'success')
        return redirect(url_for('diarias.dashboard'))

    except Exception as e:
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
        print(f"[DEBUG SEI] _integrar_sei_diarias: INICIO - itinerario.id={itinerario.id}")
        tipo_itinerario = dados.get('tipo_itinerario', 2)

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
            cargo_obj = DiariasCargo.query.get(cargo_id) if cargo_id else None
            cargo_nome = cargo_obj.nome if cargo_obj else p.get('cargo_folha', '')

            # Se assessorando, usa o cargo do assessorado para cálculo do valor
            cargo_para_calculo = cargo_assessorado_id or cargo_id
            valor_unitario = float(DiariaService.get_valor_cargo(cargo_para_calculo, tipo_itinerario)) if cargo_para_calculo else 0.0
            valor_total_pessoa = valor_unitario * float(itinerario.qtd_diarias_solicitadas)

            # Busca nome do cargo assessorado para exibição no SEI
            cargo_assessorado_obj = DiariasCargo.query.get(cargo_assessorado_id) if cargo_assessorado_id else None
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
        cargo_principal_obj = DiariasCargo.query.get(primeiro_cargo) if primeiro_cargo else None

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

        dados_itinerario = {
            'tipo_solicitacao_nome': tipo_sol_nome,
            'tipo_itinerario_nome': 'Nacional',
            'data_viagem': dados.get('data_viagem'),
            'data_retorno': dados.get('data_retorno'),
        }

        dados_requisicao = {
            'objetivo': objetivo or '',
            'servidores': servidores_sei,
            'qtd_diarias': itinerario.qtd_diarias_solicitadas,
            'trecho': trecho,
        }

        print(f"[DEBUG SEI] _integrar_sei_diarias: Chamando criar_processo_diarias_completo")
        print(f"[DEBUG SEI]   dados_requisicao keys: {list(dados_requisicao.keys())}")
        print(f"[DEBUG SEI]   servidores count: {len(servidores_sei)}, trecho: {trecho}")
        print(f"[DEBUG SEI]   tipo_solicitacao_id: {tipo_solicitacao_id}")

        resultado = criar_processo_diarias_completo(
            dados_itinerario, dados_servidor, justificativa_memorando,
            dados_requisicao=dados_requisicao,
            arquivo_externo=arquivo_externo,
            tipo_solicitacao_id=tipo_solicitacao_id,
        )

        print(f"[DEBUG SEI] _integrar_sei_diarias: resultado sucesso={resultado['sucesso']}, erro={resultado.get('erro')}")
        print(f"[DEBUG SEI]   requisicao presente: {resultado.get('requisicao') is not None}")
        print(f"[DEBUG SEI]   doc_externo presente: {resultado.get('doc_externo') is not None}")

        if resultado['sucesso']:
            # Salva dados do SEI no itinerário
            proc = resultado['procedimento']
            memo = resultado['memorando']
            req = resultado.get('requisicao')

            itinerario.sei_protocolo = resultado.get('protocolo', '')
            itinerario.sei_id_procedimento = str(proc.get('IdProcedimento', '')) if proc else None
            itinerario.sei_id_memorando = str(memo.get('IdDocumento', '')) if memo else None
            itinerario.sei_memorando_formatado = str(memo.get('DocumentoFormatado', '')) if memo else None

            if req:
                itinerario.sei_id_requisicao = str(req.get('IdDocumento', '')) if req else None
                itinerario.sei_requisicao_formatado = str(req.get('DocumentoFormatado', '')) if req else None

            req_pass = resultado.get('requisicao_passagens')
            if req_pass:
                itinerario.sei_id_requisicao_passagens = str(req_pass.get('IdDocumento', ''))
                itinerario.sei_requisicao_passagens_formatado = str(req_pass.get('DocumentoFormatado', ''))

            doc_ext = resultado.get('doc_externo')
            if doc_ext:
                itinerario.sei_id_doc_externo = str(doc_ext.get('IdDocumento', '')) if doc_ext else None
                itinerario.sei_doc_externo_formatado = str(doc_ext.get('DocumentoFormatado', '')) if doc_ext else None

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
        print(f"[DEBUG SEI] _integrar_sei_diarias: EXCECAO GERAL: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        current_app.logger.error(f"SEI Diárias: Erro na integração para itinerário {itinerario.id}: {e}")


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
