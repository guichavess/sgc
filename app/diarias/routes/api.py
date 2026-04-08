"""
Endpoints AJAX (JSON) do módulo de Diárias.
"""
from flask import request, jsonify
from flask_login import login_required

from app.diarias.routes import diarias_bp
from app.extensions import db
from app.utils.permissions import requires_permission
from app.services.diaria_service import DiariaService
from app.services.sga_service import SGAService
from app.models.diaria import Municipio, Setor, Orgao, DiariasServidor


@diarias_bp.route('/api/buscar-pessoa')
@login_required
@requires_permission('diarias.criar')
def api_buscar_pessoa_cpf():
    """
    Busca servidor por CPF.
    1º Consulta a tabela local diarias_servidores.
    2º Se não encontrar, consulta a API pessoaSGA do Gestor SEAD.
    """
    cpf = request.args.get('cpf', '').strip()

    # Limpa formatação do CPF
    cpf_limpo = ''.join(c for c in cpf if c.isdigit())

    if len(cpf_limpo) != 11:
        return jsonify({'encontrado': False, 'erro': 'CPF deve conter 11 dígitos.'}), 400

    # 1. Busca na tabela local
    servidor_local = DiariasServidor.query.filter_by(cpf=cpf_limpo).first()
    if servidor_local and servidor_local.nome:
        return jsonify({
            'encontrado': True,
            'origem': 'local',
            'matricula': servidor_local.matricula or '',
            'cpf': servidor_local.cpf,
            'nome': servidor_local.nome,
            'cargo': servidor_local.cargo or '',
            'setor': servidor_local.setor or '',
            'orgao': servidor_local.nome_orgao or '',
            'superintendencia': servidor_local.nome_superintendencia or '',
            'banco_agencia': servidor_local.num_agencia_banco or '',
            'banco_conta': servidor_local.num_conta_banco or '',
            'vinculo': servidor_local.vinculo or '',
        })

    # 2. Busca na API externa
    pessoa = SGAService.buscar_pessoa_por_cpf(cpf_limpo)

    if not pessoa:
        return jsonify({'encontrado': False})

    return jsonify({
        'encontrado': True,
        'origem': 'api',
        'matricula': pessoa.get('matricula', ''),
        'cpf': pessoa.get('cpf', cpf_limpo),
        'nome': pessoa.get('nome', ''),
        'cargo': pessoa.get('cargo', ''),
        'setor': pessoa.get('setor', ''),
        'orgao': pessoa.get('orgao', ''),
        'superintendencia': pessoa.get('superintendencia', ''),
        'banco_agencia': pessoa.get('banco_agencia', ''),
        'banco_conta': pessoa.get('banco_conta', ''),
        'vinculo': pessoa.get('vinculo', ''),
    })


@diarias_bp.route('/api/salvar-servidor', methods=['POST'])
@login_required
@requires_permission('diarias.criar')
def api_salvar_servidor():
    """Salva servidor manualmente cadastrado na tabela local."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    cpf = ''.join(c for c in (data.get('cpf') or '') if c.isdigit())
    nome = (data.get('nome') or '').strip()

    if not cpf or len(cpf) != 11:
        return jsonify({'error': 'CPF inválido.'}), 400
    if not nome:
        return jsonify({'error': 'Nome é obrigatório.'}), 400

    try:
        # Verifica se já existe por CPF
        existente = DiariasServidor.query.filter_by(cpf=cpf).first()
        if existente:
            # Atualiza dados
            existente.nome = nome
            existente.matricula = (data.get('matricula') or '').strip() or existente.matricula
            existente.cargo = (data.get('cargo') or '').strip() or existente.cargo
            existente.setor = (data.get('setor') or '').strip() or existente.setor
            existente.vinculo = (data.get('vinculo') or '').strip() or existente.vinculo
            existente.num_agencia_banco = (data.get('banco_agencia') or '').strip() or existente.num_agencia_banco
            existente.num_conta_banco = (data.get('banco_conta') or '').strip() or existente.num_conta_banco
            existente.nome_orgao = (data.get('orgao') or '').strip() or existente.nome_orgao
            db.session.commit()
            return jsonify({'salvo': True, 'id': existente.id, 'atualizado': True})
        else:
            # Cria novo
            servidor = DiariasServidor(
                nome=nome,
                cpf=cpf,
                matricula=(data.get('matricula') or '').strip() or None,
                cargo=(data.get('cargo') or '').strip() or None,
                setor=(data.get('setor') or '').strip() or None,
                vinculo=(data.get('vinculo') or '').strip() or None,
                num_agencia_banco=(data.get('banco_agencia') or '').strip() or None,
                num_conta_banco=(data.get('banco_conta') or '').strip() or None,
                nome_orgao=(data.get('orgao') or '').strip() or None,
            )
            db.session.add(servidor)
            db.session.commit()
            return jsonify({'salvo': True, 'id': servidor.id, 'atualizado': False})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/municipios')
@login_required
def api_municipios():
    """Retorna municípios, opcionalmente filtrados por estado."""
    estado = request.args.get('estado', type=int)
    municipios = DiariaService.get_municipios_por_estado(estado)
    return jsonify([
        {'cod_ibge': m.cod_ibge, 'nome': m.nome}
        for m in municipios
    ])


@diarias_bp.route('/api/setores')
@login_required
def api_setores():
    """Retorna setores filtrados por órgão."""
    orgao_id = request.args.get('orgao_id', type=int)
    if not orgao_id:
        return jsonify([])
    setores = DiariaService.get_setores_por_orgao(orgao_id)
    return jsonify([
        {'identidade': s.identidade, 'nome': s.nome}
        for s in setores
    ])


@diarias_bp.route('/api/cotacoes/<int:itinerario_id>')
@login_required
@requires_permission('diarias.aprovar')
def api_cotacoes(itinerario_id):
    """Retorna cotações de um itinerário."""
    cotacoes = DiariaService.get_cotacoes_itinerario(itinerario_id)
    return jsonify([
        {
            'id': c.id,
            'agencia': c.nome_agencia,
            'valor': float(c.valor),
            'data_hora': c.data_hora.strftime('%d/%m/%Y %H:%M') if c.data_hora else '',
        }
        for c in cotacoes
    ])


@diarias_bp.route('/api/cotacoes', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_criar_cotacao():
    """Cria uma nova cotação via AJAX."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    try:
        cotacao = DiariaService.criar_cotacao(
            itinerario_id=int(data['itinerario_id']),
            contrato_codigo=str(data['contrato_codigo']),
            valor=data['valor'],
            data_hora=data.get('data_hora'),
        )
        return jsonify({
            'id': cotacao.id,
            'agencia': cotacao.nome_agencia,
            'valor': float(cotacao.valor),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/orgaos')
@login_required
def api_orgaos():
    """Retorna lista de órgãos."""
    orgaos = DiariaService.get_orgaos()
    return jsonify([
        {'idorgao': o.idorgao, 'nome': o.nome, 'sigla': o.sigla or ''}
        for o in orgaos
    ])


@diarias_bp.route('/api/cotacoes-voos', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_criar_cotacao_voo():
    """Cria uma nova cotacao de voo detalhada."""
    from datetime import datetime as dt

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados invalidos.'}), 400

    campos_obrigatorios = ['itinerario_id', 'tipo_trecho', 'cia', 'voo',
                           'saida', 'chegada', 'origem', 'destino', 'valor']
    for campo in campos_obrigatorios:
        if not data.get(campo):
            return jsonify({'error': f'Campo obrigatorio ausente: {campo}'}), 400

    if data['tipo_trecho'] not in ('ida', 'volta'):
        return jsonify({'error': 'tipo_trecho deve ser "ida" ou "volta".'}), 400

    try:
        saida = dt.strptime(data['saida'], '%Y-%m-%dT%H:%M')
        chegada = dt.strptime(data['chegada'], '%Y-%m-%dT%H:%M')
    except (ValueError, TypeError):
        return jsonify({'error': 'Formato de data invalido. Use YYYY-MM-DDTHH:MM.'}), 400

    saida_conexao = None
    chegada_conexao = None
    if data.get('voo_conexao'):
        try:
            if data.get('saida_conexao'):
                saida_conexao = dt.strptime(data['saida_conexao'], '%Y-%m-%dT%H:%M')
            if data.get('chegada_conexao'):
                chegada_conexao = dt.strptime(data['chegada_conexao'], '%Y-%m-%dT%H:%M')
        except (ValueError, TypeError):
            return jsonify({'error': 'Formato de data da conexao invalido.'}), 400

    try:
        cotacao = DiariaService.criar_cotacao_voo(
            itinerario_id=int(data['itinerario_id']),
            contrato_codigo=data.get('contrato_codigo') or None,
            tipo_trecho=data['tipo_trecho'],
            cia=data['cia'].strip(),
            voo=data['voo'].strip(),
            saida=saida,
            chegada=chegada,
            origem=data['origem'].strip(),
            destino=data['destino'].strip(),
            valor=data['valor'],
            bagagem=data.get('bagagem', '').strip() or None,
            cia_conexao=data.get('cia_conexao', '').strip() or None,
            voo_conexao=data.get('voo_conexao', '').strip() or None,
            saida_conexao=saida_conexao,
            chegada_conexao=chegada_conexao,
            origem_conexao=data.get('origem_conexao', '').strip() or None,
            destino_conexao=data.get('destino_conexao', '').strip() or None,
            fonte='manual',
        )
        return jsonify({
            'id': cotacao.id,
            'tipo_trecho': cotacao.tipo_trecho,
            'cia': cotacao.cia,
            'voo': cotacao.voo,
            'rota': cotacao.resumo_trecho,
            'valor': float(cotacao.valor),
            'valor_formatado': cotacao.valor_formatado,
            'nome_agencia': cotacao.nome_agencia,
            'fonte': cotacao.fonte,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/cotacoes-voos/<int:itinerario_id>')
@login_required
@requires_permission('diarias.aprovar')
def api_listar_cotacoes_voos(itinerario_id):
    """Retorna cotacoes de voos de um itinerario, agrupadas por tipo."""
    dados = DiariaService.get_cotacoes_voos_itinerario(itinerario_id)

    def serializar(c):
        return {
            'id': c.id,
            'tipo_trecho': c.tipo_trecho,
            'contrato_codigo': c.contrato_codigo,
            'nome_agencia': c.nome_agencia,
            'cia': c.cia,
            'voo': c.voo,
            'saida': c.saida.strftime('%d/%m/%Y %H:%M') if c.saida else '',
            'chegada': c.chegada.strftime('%d/%m/%Y %H:%M') if c.chegada else '',
            'origem': c.origem,
            'destino': c.destino,
            'tem_conexao': c.tem_conexao,
            'cia_conexao': c.cia_conexao,
            'voo_conexao': c.voo_conexao,
            'saida_conexao': c.saida_conexao.strftime('%d/%m/%Y %H:%M') if c.saida_conexao else '',
            'chegada_conexao': c.chegada_conexao.strftime('%d/%m/%Y %H:%M') if c.chegada_conexao else '',
            'origem_conexao': c.origem_conexao,
            'destino_conexao': c.destino_conexao,
            'bagagem': c.bagagem,
            'valor': float(c.valor),
            'valor_formatado': c.valor_formatado,
            'rota': c.resumo_trecho,
        }

    return jsonify({
        'ida': [serializar(c) for c in dados['ida']],
        'volta': [serializar(c) for c in dados['volta']],
    })


@diarias_bp.route('/api/cotacoes-voos/<int:cotacao_id>', methods=['DELETE'])
@login_required
@requires_permission('diarias.aprovar')
def api_excluir_cotacao_voo(cotacao_id):
    """Exclui uma cotacao de voo."""
    ok = DiariaService.excluir_cotacao_voo(cotacao_id)
    if ok:
        return jsonify({'sucesso': True})
    return jsonify({'error': 'Cotação não encontrada.'}), 404


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS ADMIN — Edição manual de dados (processos legados)
# ══════════════════════════════════════════════════════════════════════════════


@diarias_bp.route('/api/admin/alterar-etapa', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_alterar_etapa():
    """Altera manualmente a etapa do itinerário e registra no histórico."""
    from flask_login import current_user
    from app.models.diaria import (
        DiariasItinerario, DiariasEtapa, DiariasHistoricoMovimentacao,
    )
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    itinerario_id = data.get('itinerario_id')
    etapa_id = data.get('etapa_id')
    comentario = (data.get('comentario') or '').strip()

    if not itinerario_id or not etapa_id:
        return jsonify({'error': 'itinerario_id e etapa_id são obrigatórios.'}), 400

    itinerario = DiariasItinerario.query.get_or_404(itinerario_id)
    etapa = DiariasEtapa.query.get(etapa_id)
    if not etapa:
        return jsonify({'error': 'Etapa não encontrada.'}), 404

    try:
        etapa_anterior = itinerario.etapa_atual_id
        itinerario.etapa_atual_id = etapa.id

        historico = DiariasHistoricoMovimentacao(
            id_itinerario=itinerario.id,
            id_etapa_anterior=etapa_anterior,
            id_etapa_nova=etapa.id,
            id_usuario_responsavel=current_user.id,
            comentario=comentario or 'Alteração manual de etapa (admin)',
        )
        db.session.add(historico)
        db.session.commit()

        current_app.logger.info(
            f"[DIARIAS] Etapa alterada manualmente: itinerario={itinerario.id} "
            f"{etapa_anterior} → {etapa.id} por user={current_user.id}"
        )
        return jsonify({'sucesso': True, 'etapa_nome': etapa.nome})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao alterar etapa: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/editar-dados-basicos', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_editar_dados_basicos():
    """Edita dados básicos do itinerário (tipo, datas, origem/destino, valor, etc.)."""
    from datetime import datetime as dt
    from app.models.diaria import DiariasItinerario
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    itinerario_id = data.get('itinerario_id')
    if not itinerario_id:
        return jsonify({'error': 'itinerario_id é obrigatório.'}), 400

    itinerario = DiariasItinerario.query.get_or_404(itinerario_id)

    try:
        if data.get('tipo_solicitacao_id'):
            itinerario.tipo_solicitacao_id = int(data['tipo_solicitacao_id'])
        if data.get('tipo_itinerario'):
            itinerario.tipo_itinerario = int(data['tipo_itinerario'])
        if data.get('data_viagem'):
            itinerario.data_viagem = dt.strptime(data['data_viagem'], '%Y-%m-%dT%H:%M')
        if data.get('data_retorno'):
            itinerario.data_retorno = dt.strptime(data['data_retorno'], '%Y-%m-%dT%H:%M')
        if 'origem' in data:
            itinerario.origem = data['origem'] or None
        if 'estado_origem' in data:
            itinerario.estado_origem = int(data['estado_origem']) if data['estado_origem'] else None
        if 'estado_destino' in data:
            itinerario.estado_destino = int(data['estado_destino']) if data['estado_destino'] else None
        if 'objetivo' in data:
            itinerario.objetivo = (data['objetivo'] or '').strip() or None
        if 'valor_total' in data:
            itinerario.valor_total = float(data['valor_total']) if data['valor_total'] else None
        if 'qtd_diarias_solicitadas' in data:
            itinerario.qtd_diarias_solicitadas = float(data['qtd_diarias_solicitadas']) if data['qtd_diarias_solicitadas'] else 0

        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Dados básicos editados: itinerario={itinerario.id}")
        return jsonify({'sucesso': True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao editar dados básicos: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/salvar-pessoa', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_salvar_pessoa():
    """Cria ou atualiza uma pessoa (DiariasItemItinerario) no itinerário."""
    from app.models.diaria import DiariasItinerario, DiariasItemItinerario
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    itinerario_id = data.get('itinerario_id')
    item_id = data.get('item_id')
    cpf = (data.get('cpf_pessoa') or '').strip()
    nome = (data.get('nome_pessoa') or '').strip()

    if not itinerario_id:
        return jsonify({'error': 'itinerario_id é obrigatório.'}), 400
    if not cpf:
        return jsonify({'error': 'CPF é obrigatório.'}), 400

    itinerario = DiariasItinerario.query.get_or_404(itinerario_id)

    try:
        if item_id:
            item = DiariasItemItinerario.query.get_or_404(item_id)
        else:
            item = DiariasItemItinerario(id_itinerario=itinerario.id)
            db.session.add(item)

        item.cpf_pessoa = ''.join(c for c in cpf if c.isdigit())
        item.nome_pessoa = nome or None
        item.matricula_pessoa = (data.get('matricula_pessoa') or '').strip() or None
        item.cargo_id = int(data['cargo_id']) if data.get('cargo_id') else None
        item.cargo_assessorado_id = int(data['cargo_assessorado_id']) if data.get('cargo_assessorado_id') else None
        item.natureza_id = int(data['natureza_id']) if data.get('natureza_id') else None
        item.valor_cargo = float(data['valor_cargo']) if data.get('valor_cargo') else None
        item.banco_agencia = (data.get('banco_agencia') or '').strip() or None
        item.banco_conta = (data.get('banco_conta') or '').strip() or None
        item.vinculo = (data.get('vinculo') or '').strip() or None
        item.cargo_folha = (data.get('cargo_folha') or '').strip() or None
        item.setor = (data.get('setor') or '').strip() or None
        item.orgao = (data.get('orgao') or '').strip() or None

        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Pessoa salva: itinerario={itinerario.id} item={item.id}")
        return jsonify({'sucesso': True, 'id': item.id})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao salvar pessoa: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/remover-pessoa/<int:item_id>', methods=['DELETE'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_remover_pessoa(item_id):
    """Remove uma pessoa do itinerário."""
    from app.models.diaria import DiariasItemItinerario
    from flask import current_app

    item = DiariasItemItinerario.query.get_or_404(item_id)
    try:
        db.session.delete(item)
        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Pessoa removida: item={item_id}")
        return jsonify({'sucesso': True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao remover pessoa: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/editar-documentos-sei', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_editar_documentos_sei():
    """Edita manualmente documentos SEI do itinerário."""
    from app.models.diaria import DiariasItinerario
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    itinerario_id = data.get('itinerario_id')
    documentos = data.get('documentos', [])

    if not itinerario_id:
        return jsonify({'error': 'itinerario_id é obrigatório.'}), 400

    itinerario = DiariasItinerario.query.get_or_404(itinerario_id)

    try:
        count = 0
        for doc_data in documentos:
            tipo = doc_data.get('tipo_documento', '').strip()
            sei_id = doc_data.get('sei_id', '').strip() or None
            sei_formatado = doc_data.get('sei_formatado', '').strip() or None
            codigo = doc_data.get('codigo', '').strip() or None

            if not tipo:
                continue
            if not sei_id and not sei_formatado and not codigo:
                continue

            itinerario.set_doc(tipo, sei_id=sei_id, sei_formatado=sei_formatado, codigo=codigo)
            count += 1

        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Documentos SEI editados: itinerario={itinerario.id} count={count}")
        return jsonify({'sucesso': True, 'documentos_atualizados': count})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao editar documentos SEI: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/editar-cotacao-voo', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_editar_cotacao_voo():
    """Edita uma cotação de voo existente."""
    from datetime import datetime as dt
    from app.models.diaria import DiariasCotacaoVoo
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    cotacao_id = data.get('cotacao_id')
    if not cotacao_id:
        return jsonify({'error': 'cotacao_id é obrigatório.'}), 400

    cotacao = DiariasCotacaoVoo.query.get_or_404(cotacao_id)

    try:
        if data.get('tipo_trecho') and data['tipo_trecho'] in ('ida', 'volta'):
            cotacao.tipo_trecho = data['tipo_trecho']
        if data.get('contrato_codigo') is not None:
            cotacao.contrato_codigo = data['contrato_codigo'] or None
        if data.get('cia'):
            cotacao.cia = data['cia'].strip()
        if data.get('voo'):
            cotacao.voo = data['voo'].strip()
        if data.get('saida'):
            cotacao.saida = dt.strptime(data['saida'], '%Y-%m-%dT%H:%M')
        if data.get('chegada'):
            cotacao.chegada = dt.strptime(data['chegada'], '%Y-%m-%dT%H:%M')
        if data.get('origem'):
            cotacao.origem = data['origem'].strip()
        if data.get('destino'):
            cotacao.destino = data['destino'].strip()
        if data.get('valor') is not None:
            cotacao.valor = float(data['valor'])
        if 'bagagem' in data:
            cotacao.bagagem = (data['bagagem'] or '').strip() or None

        # Conexão
        if 'cia_conexao' in data:
            cotacao.cia_conexao = (data['cia_conexao'] or '').strip() or None
        if 'voo_conexao' in data:
            cotacao.voo_conexao = (data['voo_conexao'] or '').strip() or None
        if data.get('saida_conexao'):
            cotacao.saida_conexao = dt.strptime(data['saida_conexao'], '%Y-%m-%dT%H:%M')
        elif 'saida_conexao' in data:
            cotacao.saida_conexao = None
        if data.get('chegada_conexao'):
            cotacao.chegada_conexao = dt.strptime(data['chegada_conexao'], '%Y-%m-%dT%H:%M')
        elif 'chegada_conexao' in data:
            cotacao.chegada_conexao = None
        if 'origem_conexao' in data:
            cotacao.origem_conexao = (data['origem_conexao'] or '').strip() or None
        if 'destino_conexao' in data:
            cotacao.destino_conexao = (data['destino_conexao'] or '').strip() or None

        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Cotação voo editada: id={cotacao.id}")
        return jsonify({'sucesso': True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao editar cotação voo: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/admin/editar-quadro-orcamentario', methods=['POST'])
@login_required
@requires_permission('diarias.aprovar')
def api_admin_editar_quadro_orcamentario():
    """Cria ou edita o quadro orçamentário de um itinerário."""
    from app.models.diaria import DiariasItinerario, DiariasQuadroOrcamentario
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Dados inválidos.'}), 400

    itinerario_id = data.get('itinerario_id')
    if not itinerario_id:
        return jsonify({'error': 'itinerario_id é obrigatório.'}), 400

    itinerario = DiariasItinerario.query.get_or_404(itinerario_id)

    try:
        quadro = itinerario.quadro_orcamentario
        if not quadro:
            quadro = DiariasQuadroOrcamentario(itinerario_id=itinerario.id)
            db.session.add(quadro)

        quadro.ug = (data.get('ug') or '').strip() or None
        quadro.funcao = (data.get('funcao') or '').strip() or None
        quadro.subfuncao = (data.get('subfuncao') or '').strip() or None
        quadro.programa = (data.get('programa') or '').strip() or None
        quadro.plano_interno = (data.get('plano_interno') or '').strip() or None
        quadro.fonte_recursos = (data.get('fonte_recursos') or '').strip() or None
        quadro.natureza_despesa = (data.get('natureza_despesa') or '').strip() or None
        quadro.valor_inicial_nr = float(data['valor_inicial_nr']) if data.get('valor_inicial_nr') else None
        quadro.saldo_nr = float(data['saldo_nr']) if data.get('saldo_nr') else None
        quadro.valor_despesa = float(data['valor_despesa']) if data.get('valor_despesa') else None
        quadro.saldo_atual_nr = float(data['saldo_atual_nr']) if data.get('saldo_atual_nr') else None

        db.session.commit()
        current_app.logger.info(f"[DIARIAS] Quadro orçamentário editado: itinerario={itinerario.id}")
        return jsonify({'sucesso': True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[DIARIAS] Erro ao editar quadro orçamentário: {e}")
        return jsonify({'error': str(e)}), 500


@diarias_bp.route('/api/verificar-autorizacao/<int:itinerario_id>')
@login_required
@requires_permission('diarias.visualizar')
def api_verificar_autorizacao(itinerario_id):
    """
    Verifica se o processo SEI possui documento SEAD_AUTORIZACAO_DO_SECRETARIO (IdSerie 574).
    Se encontrado, avanca automaticamente para etapa 2 (Autorizada).
    """
    from app.models.diaria import DiariasItinerario
    from app.services.diarias_sei_integration import verificar_autorizacao_diaria

    itinerario = DiariasItinerario.query.get(itinerario_id)
    if not itinerario:
        return jsonify({'error': 'Itinerario nao encontrado.'}), 404

    resultado = verificar_autorizacao_diaria(itinerario)

    doc = resultado.get('documento_autorizacao')
    assinaturas_doc = []
    if doc and doc.get('assinaturas'):
        for a in doc['assinaturas']:
            assinaturas_doc.append({
                'nome': a.get('Nome', ''),
                'cargo': a.get('CargoFuncao', ''),
                'data_hora': a.get('DataHora', ''),
            })

    envio = resultado.get('envio_procedimento')

    return jsonify({
        'autorizada': resultado['autorizada'],
        'documento': {
            'documento_formatado': doc['documento_formatado'] if doc else None,
            'serie_nome': doc['serie_nome'] if doc else None,
            'data': doc['data'] if doc else None,
            'assinaturas': assinaturas_doc,
        } if doc else None,
        'avancou_etapa': resultado['avancou_etapa'],
        'envio': {
            'sucesso': envio['sucesso'] if envio else None,
            'erro': envio.get('erro') if envio else None,
        } if envio else None,
        'erro': resultado['erro'],
    })
