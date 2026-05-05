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
