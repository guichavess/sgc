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
# ETAPAS DO PROCESSO DE DIÁRIAS (5 etapas conforme Lista de Verificação)
# =============================================================================
class DiariasEtapaID(IntEnum):
    """IDs das 5 etapas principais no fluxo de diárias e passagens."""
    SOLICITACAO_INICIAL = 1      # Memorando, Requisições, Folder, Autorização Secretário
    ESCOLHA_VOO = 2              # Cotações aéreas + Justificativa (somente tipos 2,3)
    ANALISE_SOLICITACAO = 3      # Nota de Reserva, Habilitação, SCDP, Análise NCI
    CONCESSAO_DIARIAS = 4        # NL, PD, OB
    PRESTACAO_CONTAS = 5         # Relatório de viagem, NP, Prestação SCDP


# Configuração dos sub-itens de cada etapa (checklist da Lista de Verificação).
# Cada sub-item mapeia para um tipo de documento em DiariasDocumentoSei.
# 'condicional': 'passagens' → só aparece para tipo_solicitacao_id in {2, 3}
DIARIAS_SUBITENS = {
    DiariasEtapaID.SOLICITACAO_INICIAL: [
        {'id': 'memorando',              'nome': 'Memorando de solicitação',     'doc_tipo': 'memorando'},
        {'id': 'requisicao',             'nome': 'Requisição de Diárias',        'doc_tipo': 'requisicao'},
        {'id': 'requisicao_passagens',   'nome': 'Requisição de Passagens',      'doc_tipo': 'requisicao_passagens', 'condicional': 'passagens'},
        {'id': 'doc_evento',             'nome': 'Folder ou doc. do evento',     'doc_tipo': 'doc_externo'},
        {'id': 'autorizacao',            'nome': 'Autorização do Secretário',    'doc_tipo': 'autorizacao'},
    ],
    DiariasEtapaID.ESCOLHA_VOO: [
        {'id': 'cotacoes',               'nome': 'Cotações aéreas',              'doc_tipo': 'memorando_cotacoes'},
        {'id': 'escolha_passagens',      'nome': 'Justificativa de escolha da passagem', 'doc_tipo': 'escolha_passagens'},
    ],
    DiariasEtapaID.ANALISE_SOLICITACAO: [
        {'id': 'nota_reserva',           'nome': 'Nota de reserva',              'doc_tipo': 'nota_reserva'},
        {'id': 'habilitacao',            'nome': 'Análise de habilitação para receber diárias', 'doc_tipo': 'analise_habilitacao'},
        {'id': 'scdp',                   'nome': 'Comprovante de lançamento do SCDP', 'doc_tipo': 'autorizacao_scdp'},
        {'id': 'nci',                    'nome': 'Análise do NCI',               'doc_tipo': 'analise_pagamento'},
    ],
    DiariasEtapaID.CONCESSAO_DIARIAS: [
        {'id': 'nl',                     'nome': 'Nota de liquidação',           'doc_tipo': 'nl'},
        {'id': 'pd',                     'nome': 'Programação de Desembolso',    'doc_tipo': 'pd'},
        {'id': 'ob',                     'nome': 'Ordem bancária',               'doc_tipo': 'ob'},
    ],
    DiariasEtapaID.PRESTACAO_CONTAS: [
        {'id': 'relatorio',              'nome': 'Relatório de viagem',          'doc_tipo': 'relatorio_viagem'},
        {'id': 'np',                     'nome': 'Nota patrimonial',             'doc_tipo': 'np'},
        {'id': 'prestacao_scdp',         'nome': 'Comprovante de prestação de contas no SCDP', 'doc_tipo': 'prestacao_scdp'},
    ],
}

# Tipos de solicitação que incluem passagens aéreas
TIPOS_COM_PASSAGENS = {2, 3}  # Diárias+Passagens, Apenas Passagens


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
