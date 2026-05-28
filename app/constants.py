"""
Constantes centralizadas da aplicação.
Define valores fixos usados em todo o sistema.
"""
from enum import Enum, IntEnum


# =============================================================================
# SÉRIE DE DOCUMENTOS SEI
# =============================================================================
class SerieDocumentoSEI:
    """IDs das séries de documentos no SEI."""
    SOLICITACAO = '2614'
    EMAIL = '30'
    REQUERIMENTO = '64'  # Usado para Doc Recebida e Fiscais Notificados
    NOTA_FISCAL = '64'
    ATESTO_FISCAL = '461'
    ATESTO_GESTOR = '464'
    NOTA_EMPENHO = '419'  # NE - Nota de Empenho
    LIQUIDACAO = '420'  # NL
    PD = '421'  # Programação de Desembolso
    OB = '422'  # Ordem Bancária
    MEMORANDO_SGA = '2986'  # SEAD_MEMORANDO_SGA (Diárias)


class SerieDocumentoCGFR:
    """IDs das séries de documentos SEI relevantes para timeline CGFR."""
    CGFR_DESPACHO = '3639'   # Etapa 2: CGFR
    SEFAZ_UGGP = '1179'      # Etapa 3: SEFAZ - UGGP
    NOTA_RESERVA = '425'     # NR - Nota de Reserva (link na Etapa 3 + lógica orçamento)
    CONTRATO = '37'            # Etapa 4: Contrato


# =============================================================================
# CGFR
# =============================================================================
CGFR_DELIBERACAO_OPTIONS = [
    'Aguardando Aprovação',
    'Aprovado',
    'Aprovado com redução',
    'Aprovado, com redução.',
    'Indeferido',
    'Negado',
    'Retirado',
    'Retirado de pauta',
]


# =============================================================================
# ETAPAS DO PROCESSO
# =============================================================================
class EtapaID(IntEnum):
    """IDs das etapas no banco de dados."""
    SOLICITACAO_CRIADA = 1
    DOC_SOLICITADA = 2
    DOC_RECEBIDA = 8
    FISCAIS_NOTIFICADOS = 12
    CONTRATO_FISCALIZADO = 13
    ATESTADO_CONTROLE_INTERNO = 14
    SOLICITACAO_NF = 15
    NF_ATESTADA = 11
    LIQUIDADO = 5
    PAGO = 6


class EtapaNome:
    """Nomes das etapas para exibição."""
    AGUARDANDO_DOCS = 'Aguardando Documentação'
    DOC_INCOMPLETA = 'Documentação Incompleta'
    DOC_SOLICITADA = 'Documentação Solicitada'
    DOC_RECEBIDA = 'Documentação Recebida'
    FISCAIS_NOTIFICADOS = 'Fiscais Notificados'
    CONTRATO_FISCALIZADO = 'Contrato Fiscalizado'
    ATESTADO = 'Atestado pelo Controle Interno'
    NF_ATESTADA = 'NF Atestada'
    LIQUIDADO = 'Liquidado'
    PAGO = 'Pago'


# Mapa de ordem cronológica das etapas (para comparação de avanço)
MAPA_ORDEM_ETAPAS = {
    EtapaID.SOLICITACAO_CRIADA: 1,
    EtapaID.DOC_SOLICITADA: 2,
    EtapaID.DOC_RECEBIDA: 3,
    EtapaID.SOLICITACAO_NF: 4,
    EtapaID.FISCAIS_NOTIFICADOS: 5,
    EtapaID.CONTRATO_FISCALIZADO: 6,
    EtapaID.ATESTADO_CONTROLE_INTERNO: 7,
    EtapaID.NF_ATESTADA: 8,
    EtapaID.LIQUIDADO: 9,
    EtapaID.PAGO: 10,
}

# Aliases das etapas (usados em lógica de negócio)
ETAPA_ALIASES = {
    'criado': EtapaID.SOLICITACAO_CRIADA,
    'aguardando_docs': EtapaID.DOC_SOLICITADA,
    'doc_incompleta': EtapaID.DOC_RECEBIDA,
    'assinado': EtapaID.DOC_SOLICITADA,
}


# =============================================================================
# DIÁRIAS — FLAG DE BYPASS DE ASSINATURAS (TEMPORÁRIO PARA TESTES)
# =============================================================================
# Quando True, as chamadas de assinatura SEI são simuladas (retornam sucesso
# sem chamar a API). Os documentos são criados normalmente, apenas a
# assinatura eletrônica é pulada.
# IMPORTANTE: Desativar (False) antes de usar em produção com processos reais.
DIARIAS_BYPASS_ASSINATURAS = False

# Lista de protocolos SEI (formato "00002.003853/2026-21") em que o sistema
# simula as assinaturas sem chamar o SEI. Usado para processos de teste onde
# não temos credenciais de assinantes reais (Secretário, Diretor DFIN, etc.).
# PROCESSOS REAIS NÃO DEVEM SER INCLUÍDOS AQUI.
DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS = {
    '00002.003853/2026-21',  # Processo teste — Somente Diárias
}


def protocolo_tem_bypass_assinatura(protocolo):
    """Retorna True se o protocolo (ex: '00002.003853/2026-21') deve ter
    as assinaturas simuladas. Verifica tanto a flag global quanto a lista
    de protocolos específicos.
    """
    if DIARIAS_BYPASS_ASSINATURAS:
        return True
    if not protocolo:
        return False
    return str(protocolo).strip() in DIARIAS_PROTOCOLOS_BYPASS_ASSINATURAS


# =============================================================================
# ETAPAS DO PROCESSO DE DIÁRIAS (6 etapas)
# Nota: os IDs refletem a tabela diarias_etapas. A ordem visual é controlada
# pelo campo `ordem` da tabela, não pelo valor do ID.
# =============================================================================
class DiariasEtapaID(IntEnum):
    """IDs das etapas no fluxo de diárias e passagens.

    Ordem do fluxo (campo `ordem` no banco):
    1. Solicitação Inicial (ID=1)
    2. Análise 1ª Parte (ID=3) — NR, Quadro, Habilitação
    3. Análise 2ª Parte (ID=6) — SCDP, NE, SGA, NCI
    4. Concessão (ID=4) — NL, PD, OB
    5. Prestação de Contas (ID=5)

    Passagens aéreas (tipos 2 e 3) são gerenciadas em fluxo paralelo
    independente, sem etapa fixa no fluxo principal.
    """
    SOLICITACAO_INICIAL = 1
    ESCOLHA_VOO = 2              # DESCONTINUADA — mantida para compatibilidade com dados históricos
    ANALISE_SOLICITACAO = 3      # 1ª Parte: NR, Quadro, Habilitação
    CONCESSAO_DIARIAS = 4
    PRESTACAO_CONTAS = 5
    ANALISE_SOLICITACAO_2 = 6    # 2ª Parte: SCDP, NE, SGA, NCI


# Ordem cronológica REAL do fluxo de diárias (o ID numérico não reflete a ordem).
# Fluxo: SOLICITACAO_INICIAL(1) → ANALISE_SOLICITACAO(3) → ANALISE_SOLICITACAO_2(6)
#         → CONCESSAO_DIARIAS(4) → PRESTACAO_CONTAS(5)
MAPA_ORDEM_ETAPAS_DIARIAS = {
    DiariasEtapaID.SOLICITACAO_INICIAL: 1,
    DiariasEtapaID.ANALISE_SOLICITACAO: 2,
    DiariasEtapaID.ANALISE_SOLICITACAO_2: 3,
    DiariasEtapaID.CONCESSAO_DIARIAS: 4,
    DiariasEtapaID.PRESTACAO_CONTAS: 5,
    DiariasEtapaID.ESCOLHA_VOO: 0,   # descontinuada
}


def ordem_etapa_diaria(etapa_id):
    """Posição cronológica da etapa no fluxo de diárias (0 se desconhecida).

    Use esta função — nunca compare IDs numericamente — porque CONCESSAO_DIARIAS(4)
    vem depois de ANALISE_SOLICITACAO_2(6) no fluxo real.
    """
    try:
        return MAPA_ORDEM_ETAPAS_DIARIAS.get(DiariasEtapaID(int(etapa_id)), 0)
    except (ValueError, TypeError):
        return 0


def etapa_diaria_em_ou_apos(etapa_atual, etapa_referencia):
    """True se etapa_atual está em ou após etapa_referencia na ordem cronológica real."""
    return ordem_etapa_diaria(etapa_atual) >= ordem_etapa_diaria(etapa_referencia)


# Configuração dos sub-itens de cada etapa (checklist da Lista de Verificação).
# Cada sub-item mapeia para um tipo de documento em DiariasDocumentoSei.
# 'condicional': 'passagens' → só aparece para tipo_solicitacao_id in {2, 3}
DIARIAS_SUBITENS = {
    DiariasEtapaID.SOLICITACAO_INICIAL: [
        {'id': 'memorando',              'nome': 'Memorando de solicitação',     'doc_tipo': 'memorando'},
        {'id': 'requisicao',             'nome': 'Requisição de Diárias',        'doc_tipo': 'requisicao'},
        {'id': 'requisicao_passagens',   'nome': 'Requisição de Passagens',      'doc_tipo': 'requisicao_passagens', 'condicional': 'passagens'},
        {'id': 'doc_evento',             'nome': 'Folder ou doc. do evento',     'doc_tipo': 'doc_externo', 'opcional': True},
        # Só aparece para tipos 2 e 3 (com passagens). Para tipo 1 (Apenas Diárias),
        # a autorização do Secretário ocorre via assinatura direta na Requisição de
        # Diárias (532) — não há documento separado de Autorização.
        {'id': 'autorizacao',            'nome': 'Autorização do Secretário',    'doc_tipo': 'autorizacao', 'condicional': 'passagens'},
    ],
    DiariasEtapaID.ANALISE_SOLICITACAO: [
        {'id': 'nota_reserva',           'nome': 'Nota de reserva',              'doc_tipo': 'nota_reserva'},
        {'id': 'quadro_orcamentario',    'nome': 'Quadro orçamentário',          'doc_tipo': 'quadro_orcamentario'},
        {'id': 'habilitacao',            'nome': 'Análise de habilitação para receber diárias', 'doc_tipo': 'analise_habilitacao'},
    ],
    DiariasEtapaID.ANALISE_SOLICITACAO_2: [
        {'id': 'scdp',                   'nome': 'Comprovante de lançamento do SCDP', 'doc_tipo': 'autorizacao_scdp'},
        {'id': 'nota_empenho',           'nome': 'Nota de empenho',              'doc_tipo': 'nota_empenho'},
        {'id': 'despacho_sga',           'nome': 'Despacho SGA',                 'doc_tipo': 'despacho_sga'},
        {'id': 'nci',                    'nome': 'Análise do NCI',               'doc_tipo': 'analise_pagamento'},
        {'id': 'despacho_nci',           'nome': 'Despacho NCI',                 'doc_tipo': 'despacho_nci'},
    ],
    DiariasEtapaID.CONCESSAO_DIARIAS: [
        {'id': 'nl',                     'nome': 'Nota de liquidação',           'doc_tipo': 'nl'},
        {'id': 'pd',                     'nome': 'Programação de Desembolso',    'doc_tipo': 'pd'},
        {'id': 'ob',                     'nome': 'Ordem bancária',               'doc_tipo': 'ob'},
    ],
    DiariasEtapaID.PRESTACAO_CONTAS: [
        {'id': 'relatorio',              'nome': 'Relatório de viagem',          'doc_tipo': 'relatorio_viagem'},
        {'id': 'comprovante_viagem',     'nome': 'Comprovante de viagem',        'doc_tipo': 'comprovante_viagem', 'opcional': True},
        {'id': 'np',                     'nome': 'Nota patrimonial',             'doc_tipo': 'np'},
        {'id': 'prestacao_scdp',         'nome': 'Comprovante de prestação de contas no SCDP', 'doc_tipo': 'prestacao_scdp'},
    ],
}

# Tipos de solicitação que incluem passagens aéreas
TIPOS_COM_PASSAGENS = {2, 3}  # Diárias+Passagens, Apenas Passagens

# Sub-itens de passagens — fluxo paralelo independente (não vinculado a etapa)
PASSAGENS_SUBITENS = [
    {'id': 'cotacoes',          'nome': 'Cotações aéreas',                      'doc_tipo': 'memorando_cotacoes'},
    {'id': 'escolha_passagens', 'nome': 'Justificativa de escolha da passagem', 'doc_tipo': 'escolha_passagens'},
]


# =============================================================================
# STATUS GERAIS
# =============================================================================
class StatusGeral:
    """Status gerais da solicitação."""
    EM_ANDAMENTO = 'EM ANDAMENTO'
    EM_LIQUIDACAO = 'EM LIQUIDAÇÃO'
    PAGO = 'PAGO'
    CANCELADO = 'CANCELADO'


class StatusEmpenhoID(IntEnum):
    """IDs do status de empenho no banco de dados."""
    SOLICITADO = 1
    ATENDIDO = 2
    NAO_SOLICITADO = 3


# =============================================================================
# UNIDADE GESTORA
# =============================================================================
UG_CODE = '210101'  # Código da Unidade Gestora padrão


# =============================================================================
# CHECKPOINTS PARA RELATÓRIOS
# =============================================================================
CHECKPOINTS_RELATORIO = [
    {
        'label': 'Solicitação Criada',
        'ids': [1],
        'tipo': 'single',
        'cor': '#0d6efd'
    },
    {
        'label': 'Documentação Solicitada',
        'ids': [2, 7],
        'tipo': 'single',
        'cor': '#6610f2'
    },
    {
        'label': 'Documentação Recebida',
        'ids': [3, 8],
        'tipo': 'single',
        'cor': '#6f42c1'
    },
    {
        'label': 'Solicitação da NF',
        'ids': [4, 10],
        'tipo': 'single',
        'cor': '#d63384'
    },
    {
        'label': 'Atesto e Fiscalização',
        'ids': [12, 13, 14],
        'tipo': 'group',
        'cor': '#fd7e14'
    },
    {
        'label': 'NF Atestada',
        'ids': [11, 15],
        'tipo': 'single',
        'cor': '#ffc107'
    },
    {
        'label': 'Financeiro',
        'ids': [5, 6],
        'tipo': 'group',
        'cor': '#198754'
    },
]


# =============================================================================
# MESES (para ordenação de competências)
# =============================================================================
MESES_PT_BR = {
    'Janeiro': 1,
    'Fevereiro': 2,
    'Março': 3,
    'Abril': 4,
    'Maio': 5,
    'Junho': 6,
    'Julho': 7,
    'Agosto': 8,
    'Setembro': 9,
    'Outubro': 10,
    'Novembro': 11,
    'Dezembro': 12,
}

# Mapeamento inverso: número → nome do mês
NUMERO_PARA_MES = {v: k for k, v in MESES_PT_BR.items()}


def normalizar_competencia(comp):
    """Converte '02/2025' → 'Fevereiro/2025'. Se já estiver por extenso, retorna como está."""
    if not comp or '/' not in comp:
        return comp
    partes = comp.split('/')
    if len(partes) != 2:
        return comp
    mes_str, ano = partes
    if mes_str.isdigit():
        num = int(mes_str)
        nome = NUMERO_PARA_MES.get(num)
        if nome:
            return f"{nome}/{ano}"
    return comp


# =============================================================================
# FORMATOS DE DATA
# =============================================================================
FORMATOS_DATA = [
    '%d/%m/%Y',
    '%Y-%m-%d',
    '%d-%m-%Y',
]

FORMATO_DATA_PADRAO = '%d/%m/%Y'
FORMATO_DATA_ISO = '%Y-%m-%d'
FORMATO_COMPETENCIA = '%m/%Y'


# =============================================================================
# MENSAGENS PADRÃO
# =============================================================================
class Mensagens:
    """Mensagens padrão do sistema."""
    SESSAO_EXPIRADA = 'Sessão expirada. Faça login novamente.'
    ERRO_GENERICO = 'Ocorreu um erro. Tente novamente.'
    SUCESSO_SALVO = 'Registro salvo com sucesso!'
    ERRO_PERMISSAO = 'Você não tem permissão para realizar esta ação.'
    ERRO_NAO_ENCONTRADO = 'Registro não encontrado.'
