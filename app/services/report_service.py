"""
Serviço de Relatórios - Geração de dados para relatórios.
"""
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import func

from app.extensions import db
from app.models import Solicitacao, HistoricoMovimentacao, SaldoEmpenho


class ReportService:
    """Serviço para geração de dados de relatórios.

    Filtros, contagens por fase e métricas de tempo vivem em
    `painel_pagamentos_service`.
    """

    @staticmethod
    def obter_timestamps_atualizacao() -> Dict[str, Optional[datetime]]:
        """Retorna os timestamps da última atualização de Etapas SEI e Saldo de Empenho."""
        ultima_atualizacao_sei = db.session.query(
            func.max(HistoricoMovimentacao.data_movimentacao)
        ).scalar()

        ultima_atualizacao_saldo = db.session.query(
            func.max(SaldoEmpenho.data)
        ).scalar()

        return {
            'etapas_sei': ultima_atualizacao_sei,
            'saldo_empenho': ultima_atualizacao_saldo
        }

    @staticmethod
    def contar_por_etapa(solicitacoes: List[Solicitacao]) -> Dict[str, int]:
        """Conta a quantidade de processos agrupados por nome de etapa."""
        contagem = {}
        for sol in solicitacoes:
            nome_etapa = sol.etapa.nome if sol.etapa else 'Sem Etapa'
            contagem[nome_etapa] = contagem.get(nome_etapa, 0) + 1
        return contagem

    @staticmethod
    def listar_competencias() -> List[str]:
        """Retorna lista de competências únicas ordenadas."""
        result = db.session.query(
            Solicitacao.competencia
        ).distinct().filter(
            Solicitacao.competencia.isnot(None)
        ).all()

        competencias = [c[0] for c in result if c[0]]

        # Ordena por ano e mês
        meses = {
            'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4,
            'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8,
            'Setembro': 9, 'Outubro': 10, 'Novembro': 11, 'Dezembro': 12
        }

        def chave_ordenacao(comp):
            try:
                partes = comp.split('/')
                if len(partes) == 2:
                    return (int(partes[1]), meses.get(partes[0].capitalize(), 0))
            except (ValueError, IndexError, AttributeError):
                pass
            return (0, 0)

        competencias.sort(key=chave_ordenacao, reverse=True)
        return competencias
