"""
Verificação robusta de assinaturas em documentos SEI de solicitações de diárias.

Três requisitos precisam estar satisfeitos para considerar um documento autorizado:
  1. Assinatura do Superintendente da área do solicitante (mesma `superintendencia_sigla`)
  2. Assinatura do Superintendente de Gestão Administrativa (SGACG)
  3. Assinatura do Secretário de Estado (cargo_gestao='secretario')

Quando o solicitante pertence à própria SGA, os requisitos 1 e 2 colapsam
(uma única assinatura satisfaz ambos).

Matching: prioriza correspondência exata pelo email (`Sigla` do SEI == `sigla_login`
do Usuario). Fallback textual por cargo quando o usuário não está cadastrado.
"""
from flask import current_app

from app.utils.unidade_sei import SUPER_SGA_SIGLA


def _resolver_assinante(assinatura):
    """Resolve quem é o assinante a partir dos dados da API SEI.

    Args:
        assinatura: dict com chaves 'Nome', 'CargoFuncao', 'Sigla'.

    Returns:
        dict com:
          - 'usuario': Usuario do banco (ou None se não encontrado)
          - 'nome': str
          - 'cargo_texto': str (cargo textual do SEI)
          - 'sigla': str (email SEI)
          - 'eh_superintendente': bool
          - 'eh_secretario': bool
          - 'superintendencia_sigla': str ou None (só preenchido se achou no banco)
          - 'via_banco': bool (True se identificado pelo sigla_login)
    """
    from app.models.usuario import Usuario

    sigla = (assinatura.get('Sigla') or '').strip()
    nome = assinatura.get('Nome', '') or ''
    cargo_texto = (assinatura.get('CargoFuncao') or assinatura.get('Cargo') or '').strip()

    usuario = None
    if sigla:
        usuario = Usuario.query.filter_by(sigla_login=sigla).first()

    if usuario:
        return {
            'usuario': usuario,
            'nome': usuario.nome or nome,
            'cargo_texto': cargo_texto,
            'sigla': sigla,
            'eh_superintendente': usuario.is_superintendente,
            'eh_secretario': usuario.is_secretario,
            'superintendencia_sigla': usuario.superintendencia_sigla,
            'via_banco': True,
        }

    # Fallback textual: identifica por palavras-chave no cargo SEI
    texto = (cargo_texto + ' ' + nome).lower()
    eh_super = 'superintendente' in texto
    eh_secr = ('secret' in texto) and ('estado' in texto or 'administra' in texto)

    return {
        'usuario': None,
        'nome': nome,
        'cargo_texto': cargo_texto,
        'sigla': sigla,
        'eh_superintendente': eh_super,
        'eh_secretario': eh_secr,
        'superintendencia_sigla': None,  # desconhecida (usuário não cadastrado)
        'via_banco': False,
    }


def verificar_assinaturas_requeridas(doc, itinerario=None):
    """Verifica se o documento possui as assinaturas requeridas para autorização.

    Args:
        doc: dict de documento SEI (contém lista 'Assinaturas').
        itinerario: objeto DiariasItinerario (opcional). Quando fornecido,
                    permite verificar se há assinatura do super da área do
                    solicitante.

    Returns:
        dict com:
          - completa: bool — todos os requisitos satisfeitos
          - tem_super_area: bool — requisito 1 (super da área do solicitante)
          - tem_super_sga: bool — requisito 2 (super da SGA)
          - tem_secretario: bool — requisito 3 (secretário)
          - super_area_via_banco: bool — se o match do super da área foi via banco (preciso)
          - assinaturas: list — assinaturas originais
          - nomes: list[str] — nomes dos assinantes
          - assinantes: list[dict] — dados resolvidos de cada assinante
          - super_area_esperada: str ou None — superintendência esperada do solicitante
    """
    from app.models.usuario import Usuario

    assinaturas = doc.get('Assinaturas', []) or []
    assinantes = [_resolver_assinante(a) for a in assinaturas]
    nomes = [a['nome'] for a in assinantes]

    # Descobre a superintendência esperada (a do solicitante)
    super_area_esperada = None
    if itinerario and itinerario.usuario_gerador:
        solicitante = Usuario.query.filter_by(
            sigla_login=itinerario.usuario_gerador
        ).first()
        if solicitante:
            super_area_esperada = solicitante.superintendencia_sigla

    tem_super_area = False
    super_area_via_banco = False
    tem_super_sga = False
    tem_secretario = False

    for ass in assinantes:
        # Requisito 3: Secretário
        if ass['eh_secretario']:
            tem_secretario = True

        # Requisito 2: Superintendente da SGA (SGACG)
        # Match preciso: via banco com superintendencia_sigla == 'SGACG'
        if (ass['via_banco']
                and ass['eh_superintendente']
                and ass['superintendencia_sigla'] == SUPER_SGA_SIGLA):
            tem_super_sga = True

        # Requisito 1: Superintendente da área do solicitante
        if super_area_esperada and ass['eh_superintendente']:
            if ass['via_banco'] and ass['superintendencia_sigla'] == super_area_esperada:
                # Match preciso via banco
                tem_super_area = True
                super_area_via_banco = True
            elif not ass['via_banco'] and not tem_super_area:
                # Fallback: assinatura de super não identificável no banco.
                # Aceita como "super genérico" para não travar processos.
                tem_super_area = True

    # Caso de colapso: solicitante é da própria SGA → req 1 == req 2
    if super_area_esperada == SUPER_SGA_SIGLA and tem_super_sga:
        tem_super_area = True
        super_area_via_banco = True

    # Se não temos o itinerário (ou não conseguimos identificar o solicitante),
    # cai para o modelo antigo: basta 1 super + 1 secretário.
    if not super_area_esperada:
        tem_super_area = any(a['eh_superintendente'] for a in assinantes)
        tem_super_sga = tem_super_area  # não consegue diferenciar

    # Aviso quando não há identificação precisa
    if assinaturas and not (tem_super_area or tem_super_sga or tem_secretario):
        current_app.logger.warning(
            f"SEI Diárias: Documento com {len(assinaturas)} assinatura(s) mas "
            f"nenhum requisito foi identificado. Assinantes: {nomes}. "
            f"Autorização NÃO concedida automaticamente."
        )

    completa = tem_super_area and tem_super_sga and tem_secretario

    return {
        'completa': completa,
        'tem_super_area': tem_super_area,
        'tem_super_sga': tem_super_sga,
        'tem_secretario': tem_secretario,
        'super_area_via_banco': super_area_via_banco,
        'super_area_esperada': super_area_esperada,
        'assinaturas': assinaturas,
        'nomes': nomes,
        'assinantes': assinantes,
        # Mantidos para compat com código existente que lê essas chaves
        'tem_superintendente': tem_super_area or tem_super_sga,
    }
