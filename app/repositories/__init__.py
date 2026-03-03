"""
Módulo de Repositórios - Camada de acesso a dados.
"""
from app.repositories.base import BaseRepository
from app.repositories.solicitacao_repository import SolicitacaoRepository
from app.repositories.contrato_repository import ContratoRepository
from app.repositories.empenho_repository import EmpenhoRepository
from app.repositories.liquidacao_repository import LiquidacaoRepository
from app.repositories.saldo_repository import SaldoRepository
from app.repositories.notificacao_repository import NotificacaoRepository

__all__ = [
    'BaseRepository',
    'SolicitacaoRepository',
    'ContratoRepository',
    'EmpenhoRepository',
    'LiquidacaoRepository',
    'SaldoRepository',
    'NotificacaoRepository',
]
