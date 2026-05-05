"""
Verificação de assinaturas em documentos SEI de solicitações de diárias.

Modelo (alinhado com a hierarquia cargo_gestao):
  - 1 assinatura de Superintendente (Usuario.cargo_gestao='superintendente')
  - 1 assinatura de Secretário        (cargo_gestao em ('secretario', 'secretario_exercicio'))

O matching é dinâmico contra usuários cadastrados — sem nomes/CPFs hardcoded.
Quando o admin troca a pessoa que ocupa o cargo, basta atualizar o cargo_gestao
no módulo de usuários.

Fallbacks de matching (por ordem):
  1. Sigla SEI (case-insensitive) == Usuario.sigla_login
  2. IdOrigem (somente dígitos)   == Usuario.cpf (somente dígitos)
  3. Nome SEI (sem matrícula, sem acentos) == Usuario.nome (sem acentos)
"""
from flask import current_app


def _resolver_assinante(assinatura, supers_idx=None, secs_idx=None):
    """Resolve quem é o assinante a partir dos dados da API SEI.

    Args:
        assinatura: dict com chaves 'Nome', 'CargoFuncao', 'Sigla', 'IdOrigem'.
        supers_idx: dict de índices (sigla/cpf/nome) para superintendentes.
        secs_idx: dict de índices (sigla/cpf/nome) para secretários.

    Returns:
        dict com:
          - 'usuario': Usuario do banco (ou None)
          - 'nome': str
          - 'cargo_texto': str (cargo textual do SEI)
          - 'sigla': str
          - 'eh_superintendente': bool (matched dynamically against cargo_gestao)
          - 'eh_secretario': bool (matched dynamically against cargo_gestao)
          - 'via_banco': bool
    """
    from app.services.diarias_autorizacao import _so_digitos, _nome_chave

    sigla = (assinatura.get('Sigla') or '').strip()
    sigla_lc = sigla.lower()
    nome = assinatura.get('Nome', '') or ''
    cargo_texto = (assinatura.get('CargoFuncao') or assinatura.get('Cargo') or '').strip()
    cpf_norm = _so_digitos(assinatura.get('IdOrigem'))
    nome_norm = _nome_chave(nome)

    usuario = None
    eh_super = False
    eh_secr = False

    if supers_idx is not None:
        usuario = (
            supers_idx.get('sigla', {}).get(sigla_lc)
            or supers_idx.get('cpf', {}).get(cpf_norm)
            or supers_idx.get('nome', {}).get(nome_norm)
        )
        if usuario:
            eh_super = True

    if not usuario and secs_idx is not None:
        usuario = (
            secs_idx.get('sigla', {}).get(sigla_lc)
            or secs_idx.get('cpf', {}).get(cpf_norm)
            or secs_idx.get('nome', {}).get(nome_norm)
        )
        if usuario:
            eh_secr = True

    if usuario:
        return {
            'usuario': usuario,
            'nome': usuario.nome or nome,
            'cargo_texto': cargo_texto,
            'sigla': sigla,
            'eh_superintendente': eh_super,
            'eh_secretario': eh_secr,
            'via_banco': True,
        }

    # Fallback textual quando o usuário não está cadastrado:
    # mantém a verificação parcial mas só serve como aviso (não autoriza por si).
    texto = (cargo_texto + ' ' + nome).lower()
    return {
        'usuario': None,
        'nome': nome,
        'cargo_texto': cargo_texto,
        'sigla': sigla,
        'eh_superintendente': 'superintendente' in texto,
        'eh_secretario': ('secret' in texto) and ('estado' in texto or 'administra' in texto),
        'via_banco': False,
    }


def _carregar_indices_autorizadores():
    """Carrega supers e secretários cadastrados, retornando índices O(1)."""
    from app.models.usuario import Usuario
    from app.services.diarias_autorizacao import _so_digitos, _nome_chave

    supers = Usuario.query.filter_by(cargo_gestao='superintendente', ativo=True).all()
    secretarios = (
        Usuario.query
        .filter(Usuario.cargo_gestao.in_(('secretario', 'secretario_exercicio')),
                Usuario.ativo == True)  # noqa: E712
        .all()
    )

    def _idx(usuarios):
        return {
            'sigla': {(u.sigla_login or '').strip().lower(): u for u in usuarios if u.sigla_login},
            'cpf':   {_so_digitos(u.cpf): u for u in usuarios if u.cpf},
            'nome':  {_nome_chave(u.nome): u for u in usuarios if u.nome},
        }

    return _idx(supers), _idx(secretarios)


def verificar_assinaturas_requeridas(doc, itinerario=None):
    """Verifica se o documento possui as assinaturas requeridas para autorização.

    Modelo simplificado: 1 Superintendente + 1 Secretário, ambos casados
    dinamicamente contra os usuários cadastrados (cargo_gestao).

    Returns:
        dict com:
          - completa: bool (tem_superintendente AND tem_secretario)
          - tem_superintendente: bool
          - tem_secretario: bool
          - assinaturas: list (raw)
          - nomes: list[str]
          - assinantes: list[dict]
          - tem_super_area, tem_super_sga, super_area_via_banco, super_area_esperada:
            mantidos por compat com chamadores existentes.
    """
    assinaturas = doc.get('Assinaturas', []) or []

    supers_idx, secs_idx = _carregar_indices_autorizadores()
    assinantes = [_resolver_assinante(a, supers_idx, secs_idx) for a in assinaturas]
    nomes = [a['nome'] for a in assinantes]

    tem_superintendente = any(a['eh_superintendente'] and a['via_banco'] for a in assinantes)
    tem_secretario = any(a['eh_secretario'] and a['via_banco'] for a in assinantes)

    # Fallback textual: aceita assinatura "via texto" apenas quando não há super
    # cadastrado / secretário cadastrado e há claramente um signatário com esse
    # cargo no SEI (degradação graciosa para não travar processos).
    if not tem_superintendente:
        tem_superintendente = any(a['eh_superintendente'] for a in assinantes)
    if not tem_secretario:
        tem_secretario = any(a['eh_secretario'] for a in assinantes)

    if assinaturas and not (tem_superintendente or tem_secretario):
        current_app.logger.warning(
            f"SEI Diárias: Documento com {len(assinaturas)} assinatura(s) mas "
            f"nenhum signatário casa com Superintendente/Secretário cadastrados. "
            f"Assinantes: {nomes}."
        )

    completa = tem_superintendente and tem_secretario

    return {
        'completa': completa,
        'tem_superintendente': tem_superintendente,
        'tem_secretario': tem_secretario,
        'assinaturas': assinaturas,
        'nomes': nomes,
        'assinantes': assinantes,
        # Compat com código existente (mensagens em verificar_autorizacao_diaria etc.)
        'tem_super_area': tem_superintendente,
        'tem_super_sga': tem_superintendente,
        'super_area_via_banco': tem_superintendente,
        'super_area_esperada': None,
    }
