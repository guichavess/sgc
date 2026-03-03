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
