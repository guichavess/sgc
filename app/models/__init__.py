"""
Módulo de Models - Exporta todos os modelos do banco de dados.
"""
from app.models.usuario import Usuario, load_user
from app.models.contrato import Contrato
from app.models.etapa import Etapa, StatusEmpenho
from app.models.empenho import Empenho
from app.models.liquidacao import Liquidacao
from app.models.ob import OB
from app.models.saldo import SaldoEmpenho
from app.models.solicitacao import Solicitacao, SolicitacaoEmpenho
from app.models.historico import HistoricoMovimentacao
from app.models.sei import SeiMovimentacao

# Models do módulo Prestações de Contratos
from app.models.prestacao import Prestacao
from app.models.nat_despesa import NatDespesa
from app.models.fiscal_contrato import FiscalContrato
from app.models.saldo_contrato import SaldoContrato, SaldoContratoItem, MovimentacaoSaldo
from app.models.empenho_contrato import EmpenhoContrato, LiquidacaoContrato, PagamentoContrato

# Models do Catálogo de Serviços (CATSERV)
from app.models.catserv import CatservSecao, CatservDivisao, CatservGrupo, CatservClasse, CatservServico

# Models do Catálogo de Materiais (CATMAT)
from app.models.catmat import CatmatGrupo, CatmatClasse, CatmatPdm, CatmatItem

# Model de Itens do Contrato (tabela criada pelas usuárias)
from app.models.item_contrato import ItemContrato

# Model de Vinculação de Itens ao Contrato
from app.models.item_vinculado import ItemVinculado

# Models de Centro de Custo e Tipo de Execução
from app.models.centro_de_custo import CentroDeCusto
from app.models.tipo_execucao import TipoExecucao

# Models de Empenho Itens (normalizada) e Classificadores
from app.models.empenho_item import EmpenhoItem, ClassTipoPatrimonial, ClassSubItemDespesa

# Classificador de Fonte de Recurso
from app.models.class_fonte import ClassFonte

# Model de PD (Programacao de Desembolso)
from app.models.pd import PD

# Model de Tipo de Pagamento
from app.models.tipo_pagamento import TipoPagamento

# Models de Perfil e Permissões
from app.models.perfil import Perfil, PerfilPermissao

# Models de Notificacoes
from app.models.notificacao import (
    NotificacaoTipo, Notificacao,
    NotificacaoCriticaConfirmacao, NotificacaoPreferencia,
)

# Models do módulo Diárias
from app.models.diaria import (
    Estado, Municipio, Orgao, Setor,
    DiariasStatusViagem, DiariasTipoItinerario, DiariasTipoSolicitacao,
    DiariasCargo, DiariasValorCargo, DiariasNatureza, DiariasServidor,
    DiariasAgencia, DiariasItinerario, DiariasItemItinerario, DiariasParada,
    DiariasJustificativa, DiariasCotacao,
)

# Planejamento Orçamentário
from app.models.planejamento_orcamentario import PlanejamentoOrcamentario

# CGFR (Consultoria de Gestão Financeira)
from app.cgfr.models import CgfrProcessoEnviado, Acao

# Exporta db para manter compatibilidade com imports existentes
from app.extensions import db

__all__ = [
    'db',
    'Usuario',
    'load_user',
    'Contrato',
    'Etapa',
    'StatusEmpenho',
    'Empenho',
    'Liquidacao',
    'OB',
    'SaldoEmpenho',
    'Solicitacao',
    'SolicitacaoEmpenho',
    'HistoricoMovimentacao',
    'SeiMovimentacao',
    # Prestações de Contratos
    'Prestacao',
    'NatDespesa',
    'FiscalContrato',
    'SaldoContrato',
    'SaldoContratoItem',
    'MovimentacaoSaldo',
    'EmpenhoContrato',
    'LiquidacaoContrato',
    'PagamentoContrato',
    # Catálogo de Serviços (CATSERV)
    'CatservSecao',
    'CatservDivisao',
    'CatservGrupo',
    'CatservClasse',
    'CatservServico',
    # Catálogo de Materiais (CATMAT)
    'CatmatGrupo',
    'CatmatClasse',
    'CatmatPdm',
    'CatmatItem',
    # Itens do Contrato e Vinculação
    'ItemContrato',
    'ItemVinculado',
    # Centro de Custo e Tipo de Execução
    'CentroDeCusto',
    'TipoExecucao',
    # Empenho Itens (normalizada) e Classificadores
    'EmpenhoItem',
    'ClassTipoPatrimonial',
    'ClassSubItemDespesa',
    # PD (Programacao de Desembolso)
    'PD',
    # Tipo de Pagamento
    'TipoPagamento',
    # Perfil e Permissões
    'Perfil',
    'PerfilPermissao',
    # Diárias (Referência)
    'Estado',
    'Municipio',
    'Orgao',
    'Setor',
    # Notificacoes
    'NotificacaoTipo',
    'Notificacao',
    'NotificacaoCriticaConfirmacao',
    'NotificacaoPreferencia',
    # Diárias (Módulo)
    'DiariasStatusViagem',
    'DiariasTipoItinerario',
    'DiariasTipoSolicitacao',
    'DiariasCargo',
    'DiariasValorCargo',
    'DiariasNatureza',
    'DiariasServidor',
    'DiariasAgencia',
    'DiariasItinerario',
    'DiariasItemItinerario',
    'DiariasParada',
    'DiariasJustificativa',
    'DiariasCotacao',
    # Planejamento Orçamentário
    'PlanejamentoOrcamentario',
    # Classificador de Fonte
    'ClassFonte',
    # CGFR
    'CgfrProcessoEnviado',
    'Acao',
]
