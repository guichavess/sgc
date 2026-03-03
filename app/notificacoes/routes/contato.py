"""
Rotas para coleta e atualizacao de dados de contato do usuario.
"""
import re
from flask import jsonify, request
from flask_login import login_required, current_user

from app.notificacoes import notificacoes_bp
from app.extensions import db


def _validar_email(email: str) -> bool:
    """Valida formato de email."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def _validar_telefone(telefone: str) -> bool:
    """Valida telefone brasileiro (apenas digitos, 10 ou 11)."""
    digits = re.sub(r'\D', '', telefone)
    return len(digits) in (10, 11)


def _validar_cpf(cpf: str) -> bool:
    """Valida CPF brasileiro (digitos verificadores)."""
    cpf = re.sub(r'\D', '', cpf)
    if len(cpf) != 11:
        return False
    if cpf == cpf[0] * 11:
        return False

    # Primeiro digito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto
    if int(cpf[9]) != d1:
        return False

    # Segundo digito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto
    if int(cpf[10]) != d2:
        return False

    return True


@notificacoes_bp.route('/api/salvar-contato', methods=['POST'])
@login_required
def api_salvar_contato():
    """Salva dados de contato do usuario (primeiro login ou atualizacao).

    Aceita dois modos:
    1. Completo (email + cpf obrigatorios) - para primeiro preenchimento
    2. Parcial (apenas toggles globais) - para pagina de preferencias
    """
    data = request.get_json(silent=True) or {}

    # Modo parcial: apenas toggles de notificacao
    if 'notificacoes_email' in data or 'notificacoes_telegram' in data:
        if 'notificacoes_email' in data:
            current_user.notificacoes_email = bool(data['notificacoes_email'])
        if 'notificacoes_telegram' in data:
            current_user.notificacoes_telegram = bool(data['notificacoes_telegram'])
        db.session.commit()
        return jsonify({'sucesso': True, 'msg': 'Preferencias atualizadas'})

    # Modo completo: coleta de contato
    email = data.get('email', '').strip()
    telefone = data.get('telefone', '').strip()
    cpf = data.get('cpf', '').strip()
    telegram_chat_id = data.get('telegram_chat_id', '').strip()

    erros = []

    # Validar email (obrigatorio)
    if not email:
        erros.append('Email e obrigatorio')
    elif not _validar_email(email):
        erros.append('Formato de email invalido')

    # Validar CPF (obrigatorio)
    if not cpf:
        erros.append('CPF e obrigatorio')
    elif not _validar_cpf(cpf):
        erros.append('CPF invalido')

    # Validar telefone (opcional, mas se fornecido, validar)
    if telefone and not _validar_telefone(telefone):
        erros.append('Formato de telefone invalido. Use (XX) XXXXX-XXXX')

    if erros:
        return jsonify({'sucesso': False, 'erros': erros}), 400

    # Salvar dados
    current_user.email = email
    current_user.cpf = re.sub(r'\D', '', cpf)

    if telefone:
        current_user.telefone = re.sub(r'\D', '', telefone)

    if telegram_chat_id:
        current_user.telegram_chat_id = telegram_chat_id

    current_user.contato_preenchido = True
    db.session.commit()

    return jsonify({'sucesso': True, 'msg': 'Dados salvos com sucesso'})


@notificacoes_bp.route('/api/atualizar-contato', methods=['POST'])
@login_required
def api_atualizar_contato():
    """Atualiza dados de contato (reutiliza mesma logica)."""
    return api_salvar_contato()
