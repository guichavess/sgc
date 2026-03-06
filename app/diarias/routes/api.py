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
@requires_permission('diarias.visualizar')
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
    return jsonify({'error': 'Cotacao nao encontrada.'}), 404


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
