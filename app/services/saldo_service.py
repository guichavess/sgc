"""
Serviço de Saldos - Lógica de negócio para cálculos de saldo.
"""
from typing import Optional, Dict, Any
from datetime import datetime
from flask import current_app

from app.extensions import db
from app.models import Solicitacao, SaldoEmpenho, SolicitacaoEmpenho
from app.repositories import SaldoRepository, EmpenhoRepository
from app.repositories.liquidacao_repository import LiquidacaoRepository
from app.constants import UG_CODE


class SaldoService:
    """Serviço para operações de cálculo e gerenciamento de saldos."""

    @staticmethod
    def calcular_saldo_disponivel(
        codigo_contrato: str,
        competencia: str,
        ano: int = None
    ) -> float:
        """
        Calcula o saldo disponível para um contrato.

        Fórmula: Total Empenhado - Total Liquidado

        Args:
            codigo_contrato: Código do contrato
            competencia: Competência (ex: "01/2026")
            ano: Ano do exercício (extrai da competência se não informado)

        Returns:
            Valor do saldo disponível
        """
        # Extrai ano da competência se não informado
        if not ano and competencia and '/' in competencia:
            try:
                ano = int(competencia.split('/')[-1])
            except (ValueError, IndexError):
                ano = datetime.now().year

        if not ano:
            ano = datetime.now().year

        # Total empenhado: SUM(vlr) da tabela empenho
        # Filtra por YEAR(dataEmissao) = ano, codContrato, codigoUG, CONTABILIZADO
        total_empenhado = EmpenhoRepository.calcular_total_empenhado(
            codigo_contrato=codigo_contrato,
            ano=ano,
            codigo_ug=UG_CODE
        )

        # Total liquidado: SUM(CASE WHEN ANULADO THEN valor*-1 ELSE valor END)
        # Filtra por YEAR(dataEmissao) = ano, codContrato, codigoUG
        total_liquidado = LiquidacaoRepository.calcular_total_liquidado(
            codigo_contrato=codigo_contrato,
            ano=ano,
            codigo_ug=UG_CODE
        )

        saldo = total_empenhado - total_liquidado
        return max(0.0, saldo)  # Não retorna saldo negativo

    @staticmethod
    def _calcular_total_solicitado(
        codigo_contrato: str,
        competencia: str
    ) -> float:
        """Calcula total já solicitado para o contrato na competência."""
        from sqlalchemy import func

        result = db.session.query(
            func.sum(SolicitacaoEmpenho.valor)
        ).join(
            Solicitacao,
            SolicitacaoEmpenho.id_solicitacao == Solicitacao.id
        ).filter(
            Solicitacao.codigo_contrato == codigo_contrato,
            Solicitacao.competencia == competencia
        ).scalar()

        return float(result) if result else 0.0

    @staticmethod
    def registrar_e_atualizar_saldo(
        solicitacao: Solicitacao,
        usuario_id: int,
        valor_solicitado: float = 0.0
    ) -> Dict[str, Any]:
        """
        Calcula saldo e registra solicitação de empenho.

        Validação: verifica se valor_solicitado <= saldo_disponivel
        """
        try:
            # Calcula saldo disponível (Empenhado - Liquidado)
            saldo_disponivel = SaldoService.calcular_saldo_disponivel(
                codigo_contrato=solicitacao.codigo_contrato,
                competencia=solicitacao.competencia
            )

            # Verifica se há saldo suficiente
            if valor_solicitado > saldo_disponivel:
                return {
                    'sucesso': False,
                    'msg': f'Saldo insuficiente. Disponível: R$ {saldo_disponivel:,.2f}',
                    'saldo': saldo_disponivel
                }

            # Registra a solicitação de empenho
            sol_empenho = SolicitacaoEmpenho(
                id_solicitacao=solicitacao.id,
                valor=valor_solicitado,
                competencia=solicitacao.competencia,
                id_user=usuario_id,
                saldo_momento=saldo_disponivel,
                data=datetime.now()
            )

            db.session.add(sol_empenho)

            # Atualiza o registro de saldo (cache)
            SaldoRepository.atualizar_ou_criar(
                codigo_contrato=solicitacao.codigo_contrato,
                competencia=solicitacao.competencia,
                valor=saldo_disponivel
            )

            db.session.commit()

            return {
                'sucesso': True,
                'msg': 'Empenho registrado com sucesso',
                'saldo_anterior': saldo_disponivel,
                'valor_solicitado': valor_solicitado,
                'saldo_atual': saldo_disponivel
            }

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Erro ao registrar saldo: {e}")
            return {
                'sucesso': False,
                'msg': f'Erro ao processar: {str(e)}'
            }

    @staticmethod
    def atualizar_saldo_contrato(
        codigo_contrato: str,
        competencia: str
    ) -> Optional[float]:
        """
        Recalcula e atualiza o saldo de um contrato.
        """
        try:
            saldo = SaldoService.calcular_saldo_disponivel(
                codigo_contrato, competencia
            )

            SaldoRepository.atualizar_ou_criar(
                codigo_contrato=codigo_contrato,
                competencia=competencia,
                valor=saldo
            )

            return saldo

        except Exception as e:
            current_app.logger.error(f"Erro ao atualizar saldo: {e}")
            return None

    @staticmethod
    def obter_resumo_saldo(
        codigo_contrato: str,
        competencia: str
    ) -> Dict[str, Any]:
        """
        Obtém resumo completo do saldo de um contrato.

        Returns:
            Dicionário com informações de saldo
        """
        # Extrai ano
        ano = datetime.now().year
        if competencia and '/' in competencia:
            try:
                ano = int(competencia.split('/')[-1])
            except (ValueError, IndexError):
                pass

        total_empenhado = EmpenhoRepository.calcular_total_empenhado(
            codigo_contrato, ano
        )

        total_liquidado = LiquidacaoRepository.calcular_total_liquidado(
            codigo_contrato, ano
        )

        saldo_disponivel = max(0.0, total_empenhado - total_liquidado)

        return {
            'total_empenhado': total_empenhado,
            'total_liquidado': total_liquidado,
            'saldo_disponivel': saldo_disponivel,
            'ano': ano,
            'competencia': competencia
        }
