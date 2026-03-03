"""
Rotas API de notificacoes (endpoints JSON).
"""
from datetime import datetime
from flask import jsonify, request
from flask_login import login_required, current_user

from app.notificacoes import notificacoes_bp
from app.repositories.notificacao_repository import NotificacaoRepository
from app.models.contrato import Contrato
from app.extensions import db
from app.utils.permissions import requires_admin


@notificacoes_bp.route('/api/contagem')
@login_required
def api_contagem():
    """Retorna contagem de notificacoes nao lidas e criticas pendentes."""
    nao_lidas = NotificacaoRepository.contar_nao_lidas(current_user.id)
    criticas = NotificacaoRepository.contar_criticas_pendentes(current_user.id)

    return jsonify({
        'nao_lidas': nao_lidas,
        'criticas_pendentes': criticas,
    })


@notificacoes_bp.route('/api/recentes')
@login_required
def api_recentes():
    """Retorna as 10 notificacoes mais recentes para o dropdown do sino."""
    recentes = NotificacaoRepository.listar_recentes(current_user.id, limite=10)
    nao_lidas = NotificacaoRepository.contar_nao_lidas(current_user.id)

    notificacoes = []
    for n in recentes:
        notificacoes.append({
            'id': n.id,
            'titulo': n.titulo,
            'mensagem': n.mensagem[:100],
            'nivel': n.nivel,
            'ref_url': n.ref_url,
            'lida': n.lida,
            'tempo': n.tempo_relativo,
            'created_at': n.created_at.strftime('%d/%m/%Y %H:%M'),
        })

    return jsonify({
        'notificacoes': notificacoes,
        'total_nao_lidas': nao_lidas,
    })


@notificacoes_bp.route('/api/marcar-lida', methods=['POST'])
@login_required
def api_marcar_lida():
    """Marca uma notificacao como lida."""
    data = request.get_json(silent=True) or {}
    notificacao_id = data.get('id')

    if not notificacao_id:
        return jsonify({'sucesso': False, 'msg': 'ID nao fornecido'}), 400

    sucesso = NotificacaoRepository.marcar_como_lida(notificacao_id, current_user.id)
    return jsonify({'sucesso': sucesso})


@notificacoes_bp.route('/api/marcar-todas-lidas', methods=['POST'])
@login_required
def api_marcar_todas_lidas():
    """Marca todas as notificacoes como lidas (exceto criticas)."""
    count = NotificacaoRepository.marcar_todas_lidas(current_user.id)
    return jsonify({'sucesso': True, 'marcadas': count})


@notificacoes_bp.route('/api/confirmar-critica', methods=['POST'])
@login_required
def api_confirmar_critica():
    """Confirma notificacao critica via CPF."""
    data = request.get_json(silent=True) or {}
    notificacao_id = data.get('notificacao_id')
    cpf = data.get('cpf', '').strip().replace('.', '').replace('-', '')

    if not notificacao_id or not cpf:
        return jsonify({'sucesso': False, 'msg': 'Dados incompletos'}), 400

    # Validar CPF do usuario
    if not current_user.cpf:
        return jsonify({
            'sucesso': False,
            'msg': 'CPF nao cadastrado. Atualize seus dados de contato.'
        }), 400

    if cpf != current_user.cpf:
        return jsonify({'sucesso': False, 'msg': 'CPF nao confere'}), 400

    sucesso = NotificacaoRepository.confirmar_critica(
        notificacao_id, current_user.id, cpf
    )

    if sucesso:
        return jsonify({'sucesso': True})
    return jsonify({'sucesso': False, 'msg': 'Notificacao nao encontrada'}), 404


@notificacoes_bp.route('/api/preferencias', methods=['GET'])
@login_required
@requires_admin
def api_preferencias_get():
    """Lista preferencias de notificacao de um usuario (admin-only)."""
    from app.models.notificacao import NotificacaoTipo, NotificacaoPreferencia

    usuario_id = request.args.get('usuario_id', type=int)
    if not usuario_id:
        return jsonify({'sucesso': False, 'msg': 'usuario_id obrigatorio'}), 400

    tipos = NotificacaoTipo.query.filter_by(ativo=True).all()
    prefs = NotificacaoPreferencia.query.filter_by(
        usuario_id=usuario_id
    ).all()
    prefs_map = {p.tipo_id: p for p in prefs}

    resultado = []
    for t in tipos:
        pref = prefs_map.get(t.id)
        resultado.append({
            'tipo_id': t.id,
            'codigo': t.codigo,
            'nome': t.nome,
            'modulo': t.modulo,
            'nivel': t.nivel,
            'canal_in_app': pref.canal_in_app if pref else t.canal_in_app,
            'silenciado': pref.silenciado if pref else False,
        })

    return jsonify({'preferencias': resultado})


@notificacoes_bp.route('/api/preferencias', methods=['POST'])
@login_required
@requires_admin
def api_preferencias_post():
    """Atualiza preferencias de notificacao de um usuario (admin-only)."""
    from app.models.notificacao import NotificacaoPreferencia

    data = request.get_json(silent=True) or {}
    usuario_id = data.get('usuario_id')
    tipo_id = data.get('tipo_id')

    if not usuario_id or not tipo_id:
        return jsonify({'sucesso': False, 'msg': 'usuario_id e tipo_id obrigatorios'}), 400

    pref = NotificacaoPreferencia.query.filter_by(
        usuario_id=usuario_id, tipo_id=tipo_id
    ).first()

    if not pref:
        pref = NotificacaoPreferencia(
            usuario_id=usuario_id,
            tipo_id=tipo_id,
        )
        db.session.add(pref)

    pref.canal_in_app = data.get('canal_in_app', pref.canal_in_app)
    pref.silenciado = data.get('silenciado', pref.silenciado)

    db.session.commit()
    return jsonify({'sucesso': True})


@notificacoes_bp.route('/api/contrato/silenciar-vigencia', methods=['POST'])
@login_required
@requires_admin
def api_silenciar_vigencia():
    """Silencia notificacoes de vigencia para um contrato (aditivo em andamento)."""
    data = request.get_json(silent=True) or {}
    codigo_contrato = data.get('codigo_contrato')
    motivo = data.get('motivo', 'Aditivo em tramitacao')
    silenciar = data.get('silenciar', True)

    if not codigo_contrato:
        return jsonify({'sucesso': False, 'msg': 'codigo_contrato obrigatorio'}), 400

    contrato = Contrato.query.get(codigo_contrato)
    if not contrato:
        return jsonify({'sucesso': False, 'msg': 'Contrato nao encontrado'}), 404

    contrato.vigencia_notificacao_silenciada = silenciar
    contrato.vigencia_silenciada_por = current_user.id if silenciar else None
    contrato.vigencia_silenciada_em = datetime.now() if silenciar else None
    contrato.vigencia_silenciada_motivo = motivo if silenciar else None

    db.session.commit()

    acao = 'silenciadas' if silenciar else 'reativadas'
    return jsonify({
        'sucesso': True,
        'msg': f'Notificacoes de vigencia {acao} para {codigo_contrato}',
    })
