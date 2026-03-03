"""
Serviço de Relatórios - Geração de dados para relatórios.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy import func

from app.extensions import db
from app.models import Solicitacao, Contrato, Etapa, HistoricoMovimentacao, SeiMovimentacao, SaldoEmpenho
from app.constants import CHECKPOINTS_RELATORIO


class ReportService:
    """Serviço para geração de dados de relatórios."""

    @staticmethod
    def gerar_relatorio_geral(
        competencia: Optional[str] = None,
        contratado: Optional[str] = None,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Gera relatório geral de solicitações.

        Returns:
            Dicionário com dados do relatório
        """
        query = db.session.query(Solicitacao).join(Contrato)

        # Aplica filtros
        if competencia:
            query = query.filter(Solicitacao.competencia == competencia)
        if contratado:
            query = query.filter(Contrato.nomeContratado.ilike(f'%{contratado}%'))
        if data_inicio:
            query = query.filter(Solicitacao.data_solicitacao >= data_inicio)
        if data_fim:
            query = query.filter(Solicitacao.data_solicitacao <= data_fim)

        solicitacoes = query.order_by(Solicitacao.data_solicitacao.desc()).all()

        # Estatísticas gerais
        total = len(solicitacoes)
        pagos = len([s for s in solicitacoes if s.status_geral == 'PAGO'])
        em_andamento = len([s for s in solicitacoes if s.status_geral == 'EM ANDAMENTO'])
        em_liquidacao = len([s for s in solicitacoes if s.status_geral == 'EM LIQUIDAÇÃO'])

        return {
            'solicitacoes': solicitacoes,
            'total': total,
            'pagos': pagos,
            'em_andamento': em_andamento,
            'em_liquidacao': em_liquidacao,
            'percentual_pagos': round((pagos / total * 100) if total > 0 else 0, 1),
            'filtros': {
                'competencia': competencia,
                'contratado': contratado,
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }
        }

    @staticmethod
    def gerar_relatorio_metricas(
        competencia: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Gera relatório com métricas e estatísticas.

        Returns:
            Dicionário com métricas
        """
        query = db.session.query(Solicitacao)

        if competencia:
            query = query.filter(Solicitacao.competencia == competencia)

        solicitacoes = query.all()

        # Tempo médio por etapa
        tempos_etapa = ReportService._calcular_tempos_etapa(solicitacoes)

        # Contagem por status
        por_status = {}
        for sol in solicitacoes:
            status = sol.status_geral or 'SEM STATUS'
            por_status[status] = por_status.get(status, 0) + 1

        # Contagem por etapa
        por_etapa = {}
        for sol in solicitacoes:
            etapa_id = sol.etapa_atual_id or 0
            por_etapa[etapa_id] = por_etapa.get(etapa_id, 0) + 1

        # Tempo médio total (apenas processos finalizados)
        tempos_totais = []
        for sol in solicitacoes:
            if sol.tempo_total and 'dias' in sol.tempo_total:
                try:
                    dias = int(sol.tempo_total.replace(' dias', ''))
                    tempos_totais.append(dias)
                except ValueError:
                    pass

        tempo_medio_total = round(sum(tempos_totais) / len(tempos_totais), 1) if tempos_totais else 0

        return {
            'total_solicitacoes': len(solicitacoes),
            'por_status': por_status,
            'por_etapa': por_etapa,
            'tempos_etapa': tempos_etapa,
            'tempo_medio_total': tempo_medio_total,
            'tempo_minimo': min(tempos_totais) if tempos_totais else 0,
            'tempo_maximo': max(tempos_totais) if tempos_totais else 0
        }

    @staticmethod
    def _calcular_tempos_etapa(solicitacoes: List[Solicitacao]) -> Dict[int, float]:
        """Calcula tempo médio em cada etapa."""
        tempos = {}
        contagens = {}

        for sol in solicitacoes:
            historico = HistoricoMovimentacao.query.filter_by(
                id_solicitacao=sol.id
            ).order_by(HistoricoMovimentacao.data_movimentacao).all()

            for i in range(len(historico) - 1):
                etapa_id = historico[i].id_etapa_nova
                data_inicio = historico[i].data_movimentacao
                data_fim = historico[i + 1].data_movimentacao

                if data_inicio and data_fim:
                    dias = (data_fim - data_inicio).days
                    if etapa_id not in tempos:
                        tempos[etapa_id] = 0
                        contagens[etapa_id] = 0
                    tempos[etapa_id] += dias
                    contagens[etapa_id] += 1

        # Calcula médias
        return {
            etapa_id: round(tempos[etapa_id] / contagens[etapa_id], 1)
            for etapa_id in tempos
            if contagens[etapa_id] > 0
        }

    @staticmethod
    def gerar_relatorio_checkpoints(
        competencia: Optional[str] = None,
        contratado: Optional[str] = None,
        data_inicio: Optional[datetime] = None,
        data_fim: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Gera relatório baseado nos checkpoints definidos.

        Returns:
            Dicionário com dados por checkpoint
        """
        query = db.session.query(Solicitacao).join(Contrato)

        if competencia:
            query = query.filter(Solicitacao.competencia == competencia)
        if contratado:
            query = query.filter(Contrato.nomeContratado.ilike(f'%{contratado}%'))
        if data_inicio:
            query = query.filter(Solicitacao.data_solicitacao >= data_inicio)
        if data_fim:
            query = query.filter(Solicitacao.data_solicitacao <= data_fim)

        solicitacoes = query.all()

        # Mapeia etapa para checkpoint
        mapa_checkpoint = {}
        for idx, cp in enumerate(CHECKPOINTS_RELATORIO):
            for etapa_id in cp['ids']:
                mapa_checkpoint[etapa_id] = idx

        # Conta solicitações por checkpoint
        contagem = [0] * len(CHECKPOINTS_RELATORIO)
        for sol in solicitacoes:
            idx = mapa_checkpoint.get(sol.etapa_atual_id, 0)
            contagem[idx] += 1

        # Monta resultado
        checkpoints_data = []
        for idx, cp in enumerate(CHECKPOINTS_RELATORIO):
            checkpoints_data.append({
                'label': cp['label'],
                'cor': cp['cor'],
                'quantidade': contagem[idx],
                'percentual': round((contagem[idx] / len(solicitacoes) * 100) if solicitacoes else 0, 1)
            })

        return {
            'checkpoints': checkpoints_data,
            'total': len(solicitacoes)
        }

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
