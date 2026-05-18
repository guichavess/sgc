"""
Lógica de hierarquia de autorização para diárias (Etapa 1).

Hierarquia de autorizadores:
  Nível 1 — Secretário titular        (cargo_gestao='secretario')
  Nível 2 — Secretário em exercício   (cargo_gestao='secretario_exercicio')
  Nível 3 — Superintendente           (cargo_gestao='superintendente') — ação única

Regra de escalonamento: se o titular do nível N é integrante da viagem,
o nível N+1 assume automaticamente.
"""


_CARGOS_AUTORIZADORES = ('secretario', 'secretario_exercicio', 'superintendente')


def _carregar_estado_autorizacao(itinerario):
    """
    Carrega em UMA única query todos os usuários relevantes (3 cargos)
    e em UMA única iteração os CPFs de integrantes.

    Reutilizado por get_nivel_autorizacao() e superintendente_dispensado()
    para evitar queries/iterações duplicadas no mesmo carregamento de página.
    """
    from app.models.usuario import Usuario

    usuarios = (
        Usuario.query
        .filter(Usuario.cargo_gestao.in_(_CARGOS_AUTORIZADORES), Usuario.ativo == True)  # noqa: E712
        .all()
    )

    cpfs_integrantes = {
        i.cpf_pessoa.strip()
        for i in itinerario.itens.all()
        if i.cpf_pessoa
    }

    nivel1, nivel2, nivel3 = [], [], []
    for u in usuarios:
        if u.cargo_gestao == 'secretario':
            nivel1.append(u)
        elif u.cargo_gestao == 'secretario_exercicio':
            nivel2.append(u)
        elif u.cargo_gestao == 'superintendente':
            nivel3.append(u)

    return {
        'nivel1': nivel1,
        'nivel2': nivel2,
        'nivel3': nivel3,
        'cpfs_integrantes': cpfs_integrantes,
    }


def get_nivel_autorizacao(itinerario, _estado=None):
    """
    Determina o nível hierárquico que deve autorizar o itinerário.

    Args:
        _estado: dict opcional retornado por _carregar_estado_autorizacao,
                 para evitar requery quando chamado em conjunto com
                 superintendente_dispensado.

    Returns:
        {
            'nivel': 1 | 2 | 3,
            'autorizadores': [Usuario, ...],
            'motivo_escalada': str | None,
        }
    """
    estado = _estado or _carregar_estado_autorizacao(itinerario)
    cpfs_integrantes = estado['cpfs_integrantes']

    def _conflitado(usuarios):
        return any((u.cpf or '').strip() in cpfs_integrantes for u in usuarios)

    if not _conflitado(estado['nivel1']):
        return {'nivel': 1, 'autorizadores': estado['nivel1'], 'motivo_escalada': None}

    if not _conflitado(estado['nivel2']):
        return {
            'nivel': 2,
            'autorizadores': estado['nivel2'],
            'motivo_escalada': 'Secretário titular é integrante da solicitação',
        }

    return {
        'nivel': 3,
        'autorizadores': estado['nivel3'],
        'motivo_escalada': 'Secretário titular e Secretário em exercício são integrantes',
    }


def superintendente_dispensado(itinerario, _estado=None):
    """
    Retorna True se o superintendente é integrante da viagem.
    Quando dispensado, a etapa de pré-assinatura é pulada.
    """
    estado = _estado or _carregar_estado_autorizacao(itinerario)
    cpfs_integrantes = estado['cpfs_integrantes']
    if not cpfs_integrantes:
        return False
    return any((u.cpf or '').strip() in cpfs_integrantes for u in estado['nivel3'])


def get_estado_etapa1(itinerario):
    """
    Helper otimizado para a rota administracao_detalhe: faz UMA query +
    UMA iteração e retorna nivel_autorizacao + super_dispensado.
    """
    estado = _carregar_estado_autorizacao(itinerario)
    return {
        'nivel_autorizacao': get_nivel_autorizacao(itinerario, _estado=estado),
        'super_dispensado': superintendente_dispensado(itinerario, _estado=estado),
    }


# IDs de série SEI para requisições
ID_SERIE_REQUISICAO_DIARIAS = '532'
ID_SERIE_REQUISICAO_PASSAGENS = '2975'


def _so_digitos(valor):
    """Remove tudo que não é dígito (normaliza CPF)."""
    if not valor:
        return ''
    return ''.join(c for c in str(valor) if c.isdigit())


def _nome_chave(valor):
    """Normaliza nome para comparação: lower + sem matrícula + sem acentos."""
    import unicodedata
    if not valor:
        return ''
    s = str(valor)
    # Remove sufixo " - Mat.XXX" / " - Matr.XXX"
    if ' - ' in s:
        s = s.split(' - ', 1)[0]
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    return s.strip().lower()


def parse_sei_datahora(texto):
    """Parse do formato 'dd/mm/yyyy HH:MM:SS' retornado pela API SEI."""
    from datetime import datetime as _dt
    if not texto:
        return None
    try:
        return _dt.strptime(texto.strip(), '%d/%m/%Y %H:%M:%S')
    except (ValueError, AttributeError):
        return None


def verificar_assinatura_superintendente_sei(itinerario, documentos_sei=None):
    """
    Consulta o SEI para verificar se algum usuário cadastrado como
    Superintendente (cargo_gestao='superintendente') já assinou a
    Requisição de Diárias (série 532) deste itinerário.

    Matching dinâmico (por ordem de prioridade):
      1. Sigla SEI (lower) == Usuario.sigla_login (lower)
      2. IdOrigem em dígitos == Usuario.cpf em dígitos
      3. Nome SEI (sem matrícula/acentos) == Usuario.nome (sem acentos)

    Sem nomes/CPFs hardcoded — quando o admin troca o Superintendente
    pelo módulo de usuários (cargo_gestao), o sistema passa a esperar
    pela assinatura do novo automaticamente.

    Args:
        itinerario: objeto DiariasItinerario
        documentos_sei: lista de docs já buscados do SEI (opcional). Quando
            fornecida, evita uma chamada extra à API do SEI.
    """
    from flask import current_app
    from app.models.usuario import Usuario
    from app.services.diarias_sei_integration import consultar_documentos_procedimento

    if not itinerario.sei_protocolo:
        return {'assinada': False, 'erro': 'Itinerário sem protocolo SEI'}

    supers = Usuario.query.filter_by(cargo_gestao='superintendente', ativo=True).all()
    if not supers:
        return {'assinada': False, 'erro': 'Nenhum usuário cadastrado com cargo_gestao=superintendente'}

    # Índices O(1) com normalização agressiva
    sigla_to_user = {}
    cpf_to_user = {}
    nome_to_user = {}
    for u in supers:
        if u.sigla_login:
            sigla_to_user[u.sigla_login.strip().lower()] = u
        cpf_norm = _so_digitos(u.cpf)
        if cpf_norm:
            cpf_to_user[cpf_norm] = u
        nome_norm = _nome_chave(u.nome)
        if nome_norm:
            nome_to_user[nome_norm] = u

    if documentos_sei is None:
        resp = consultar_documentos_procedimento(itinerario.sei_protocolo)
        if not resp or not resp.get('sucesso'):
            erro = (resp.get('erro') if resp else None) or 'Sem resposta do SEI'
            current_app.logger.warning(
                f'[DIARIAS] verificar_assinatura_super: falha SEI protocolo={itinerario.sei_protocolo!r} erro={erro!r}'
            )
            return {'assinada': False, 'erro': f'Falha ao consultar SEI: {erro}'}
        documentos_sei = resp.get('documentos', []) or []

    doc_req_sei = None
    for sei_doc in documentos_sei:
        serie = sei_doc.get('Serie') or {}
        if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_DIARIAS:
            doc_req_sei = sei_doc
            break

    if not doc_req_sei:
        for sei_doc in documentos_sei:
            serie = sei_doc.get('Serie') or {}
            if str(serie.get('IdSerie', '')) == ID_SERIE_REQUISICAO_PASSAGENS:
                doc_req_sei = sei_doc
                break

    if not doc_req_sei:
        return {'assinada': False, 'erro': 'Requisição (Diárias ou Passagens) não encontrada no processo SEI'}

    doc_id = doc_req_sei.get('IdDocumento')
    doc_fmt = doc_req_sei.get('DocumentoFormatado')
    assinaturas = doc_req_sei.get('Assinaturas') or []

    for ass in assinaturas:
        sigla = (ass.get('Sigla') or '').strip().lower()
        id_origem_norm = _so_digitos(ass.get('IdOrigem'))
        nome_norm = _nome_chave(ass.get('Nome'))

        usuario_match = (
            sigla_to_user.get(sigla)
            or cpf_to_user.get(id_origem_norm)
            or nome_to_user.get(nome_norm)
        )
        if usuario_match:
            current_app.logger.info(
                f'[DIARIAS] verificar_assinatura_super: MATCH itinerario={itinerario.id} '
                f'protocolo={itinerario.sei_protocolo!r} usuario={usuario_match.id} '
                f'sigla_sei={sigla!r}'
            )
            return {
                'assinada': True,
                'assinante_nome': ass.get('Nome'),
                'assinante_usuario_id': usuario_match.id,
                'doc_sei_id': doc_id,
                'doc_sei_formatado': doc_fmt,
                'data_hora_assinatura': parse_sei_datahora(ass.get('DataHora')),
                'erro': None,
            }

    # Diagnóstico: sem match. Loga o que foi comparado para o admin debugar.
    siglas_assinantes = [a.get('Sigla') for a in assinaturas]
    nomes_assinantes = [a.get('Nome') for a in assinaturas]
    current_app.logger.warning(
        f'[DIARIAS] verificar_assinatura_super: SEM MATCH itinerario={itinerario.id} '
        f'protocolo={itinerario.sei_protocolo!r} '
        f'supers_cadastrados={[(u.id, u.sigla_login, u.cpf) for u in supers]} '
        f'assinantes_sei_siglas={siglas_assinantes} '
        f'assinantes_sei_nomes={nomes_assinantes}'
    )
    return {
        'assinada': False,
        'doc_sei_id': doc_id,
        'doc_sei_formatado': doc_fmt,
        'erro': (
            'Documento existe no SEI mas nenhuma assinatura corresponde ao '
            'Superintendente cadastrado. Confirme em Admin > Usuários se o '
            'usuário com cargo_gestao=superintendente possui o mesmo email/CPF '
            'usado para assinar no SEI.'
        ),
    }
