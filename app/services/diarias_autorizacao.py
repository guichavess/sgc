"""
Lógica de hierarquia de autorização para diárias (Etapa 1).

Hierarquia de autorizadores:
  Nível 1 — Secretário titular        (cargo_gestao='secretario')
  Nível 2 — Secretário em exercício   (cargo_gestao='secretario_exercicio')
  Nível 3 — Superintendente           (cargo_gestao='superintendente') — ação única

Regra de escalonamento: se o titular do nível N é integrante da viagem,
o nível N+1 assume automaticamente.
"""


def get_nivel_autorizacao(itinerario):
    """
    Determina o nível hierárquico que deve autorizar o itinerário.

    Returns:
        {
            'nivel': 1 | 2 | 3,
            'autorizadores': [Usuario, ...],
            'motivo_escalada': str | None,
        }
    """
    from app.models.usuario import Usuario

    cpfs_integrantes = {
        i.cpf_pessoa.strip()
        for i in itinerario.itens.all()
        if i.cpf_pessoa
    }

    nivel1 = Usuario.query.filter_by(cargo_gestao='secretario', ativo=True).all()
    nivel2 = Usuario.query.filter_by(cargo_gestao='secretario_exercicio', ativo=True).all()
    nivel3 = Usuario.query.filter_by(cargo_gestao='superintendente', ativo=True).all()

    def _conflitado(usuarios):
        return any((u.cpf or '').strip() in cpfs_integrantes for u in usuarios)

    if not _conflitado(nivel1):
        return {'nivel': 1, 'autorizadores': nivel1, 'motivo_escalada': None}

    if not _conflitado(nivel2):
        return {
            'nivel': 2,
            'autorizadores': nivel2,
            'motivo_escalada': 'Secretário titular é integrante da solicitação',
        }

    return {
        'nivel': 3,
        'autorizadores': nivel3,
        'motivo_escalada': 'Secretário titular e Secretário em exercício são integrantes',
    }


def superintendente_dispensado(itinerario):
    """
    Retorna True se o superintendente é integrante da viagem.
    Quando dispensado, a etapa de pré-assinatura é pulada.
    """
    from app.models.usuario import Usuario

    cpfs_integrantes = {
        i.cpf_pessoa.strip()
        for i in itinerario.itens.all()
        if i.cpf_pessoa
    }
    if not cpfs_integrantes:
        return False

    supers = Usuario.query.filter_by(cargo_gestao='superintendente', ativo=True).all()
    return any((u.cpf or '').strip() in cpfs_integrantes for u in supers)


# ID da série SEI para "SEAD_REQUISIÇÃO DE DIÁRIAS"
ID_SERIE_REQUISICAO_DIARIAS = '532'


def verificar_assinatura_superintendente_sei(itinerario):
    """
    Consulta o SEI para verificar se algum usuário cadastrado como
    Superintendente (cargo_gestao='superintendente') já assinou a
    Requisição de Diárias (série 532) deste itinerário.

    O matching é dinâmico: usa os usuários cadastrados em sis_usuarios,
    sem nomes/CPFs hardcoded. Quando o admin troca o Superintendente,
    basta atualizar o cargo_gestao no módulo de usuários.

    Estratégia de matching (por ordem de prioridade):
      1. Sigla SEI (ex: 'pedro.alexandre@sead.pi.gov.br') == Usuario.sigla_login
      2. IdOrigem (CPF) == Usuario.cpf

    Returns:
        {
            'assinada': bool,
            'assinante_nome': str | None,
            'assinante_usuario_id': int | None,
            'doc_sei_id': str | None,
            'doc_sei_formatado': str | None,
            'erro': str | None,
        }
    """
    from app.models.usuario import Usuario
    from app.services.diarias_sei_integration import consultar_documentos_procedimento

    if not itinerario.sei_protocolo:
        return {'assinada': False, 'erro': 'Itinerário sem protocolo SEI'}

    supers = Usuario.query.filter_by(cargo_gestao='superintendente', ativo=True).all()
    if not supers:
        return {'assinada': False, 'erro': 'Nenhum Superintendente cadastrado no sistema'}

    # Indexa por sigla (case-insensitive) e CPF para lookup O(1)
    sigla_to_user = {
        (u.sigla_login or '').strip().lower(): u
        for u in supers if u.sigla_login
    }
    cpf_to_user = {
        (u.cpf or '').strip(): u
        for u in supers if u.cpf
    }

    resp = consultar_documentos_procedimento(itinerario.sei_protocolo)
    if not resp or not resp.get('sucesso'):
        return {
            'assinada': False,
            'erro': resp.get('erro', 'Falha ao consultar documentos no SEI') if resp else 'Sem resposta do SEI',
        }

    doc_req_sei = None
    for sei_doc in resp.get('documentos', []) or []:
        serie = sei_doc.get('Serie') or {}
        if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
            doc_req_sei = sei_doc
            break

    if not doc_req_sei:
        return {'assinada': False, 'erro': 'Requisição de Diárias não encontrada no processo SEI'}

    doc_id = doc_req_sei.get('IdDocumento')
    doc_fmt = doc_req_sei.get('DocumentoFormatado')

    for ass in (doc_req_sei.get('Assinaturas') or []):
        sigla = (ass.get('Sigla') or '').strip().lower()
        id_origem = (ass.get('IdOrigem') or '').strip()

        usuario_match = sigla_to_user.get(sigla) or cpf_to_user.get(id_origem)
        if usuario_match:
            return {
                'assinada': True,
                'assinante_nome': ass.get('Nome'),
                'assinante_usuario_id': usuario_match.id,
                'doc_sei_id': doc_id,
                'doc_sei_formatado': doc_fmt,
                'erro': None,
            }

    return {
        'assinada': False,
        'doc_sei_id': doc_id,
        'doc_sei_formatado': doc_fmt,
        'erro': None,
    }
