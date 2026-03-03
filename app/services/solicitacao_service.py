"""
Serviço de Solicitações - Lógica de negócio para solicitações de pagamento.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from flask import current_app

from app.extensions import db
from app.models import (
    Solicitacao, HistoricoMovimentacao, Etapa,
    SolicitacaoEmpenho, SeiMovimentacao
)
from app.repositories import SolicitacaoRepository, ContratoRepository
from app.constants import (
    SerieDocumentoSEI, EtapaID, MAPA_ORDEM_ETAPAS, StatusGeral
)


def _notificar_solicitacao_criada(solicitacao, usuario_id):
    """Dispara notificacoes apos criacao de solicitacao."""
    try:
        from app.services.notification_engine import NotificationEngine

        ref_url = f'/solicitacoes/solicitacao/{solicitacao.id}'
        contrato_cod = solicitacao.codigo_contrato or ''

        # Notifica criador (silenciosa)
        NotificationEngine.notificar(
            tipo_codigo='solicitacao.criada',
            destinatarios=[usuario_id],
            titulo='Solicitacao criada',
            mensagem=f'Solicitacao #{solicitacao.id} - Contrato {contrato_cod}',
            ref_modulo='solicitacoes',
            ref_id=str(solicitacao.id),
            ref_url=ref_url,
        )

        # Notifica financeiro sobre nova solicitacao
        dest_fin = NotificationEngine.resolver_destinatarios(
            'financeiro.nova_solicitacao',
            codigo_contrato=contrato_cod,
        )
        if dest_fin:
            NotificationEngine.notificar(
                tipo_codigo='financeiro.nova_solicitacao',
                destinatarios=dest_fin,
                titulo='Nova solicitacao de pagamento',
                mensagem=f'Solicitacao #{solicitacao.id} - Contrato {contrato_cod}',
                ref_modulo='solicitacoes',
                ref_id=str(solicitacao.id),
                ref_url=ref_url,
            )
    except Exception as e:
        current_app.logger.warning(f'Erro ao notificar criacao: {e}')


def _notificar_etapa_avancou(solicitacao, nova_etapa_id, etapa_anterior_id):
    """Dispara notificacoes apos avanco de etapa."""
    try:
        from app.services.notification_engine import NotificationEngine

        ref_url = f'/solicitacoes/solicitacao/{solicitacao.id}'
        contrato_cod = solicitacao.codigo_contrato or ''

        # Busca nome da etapa
        from app.models import Etapa
        etapa = Etapa.query.get(nova_etapa_id)
        nome_etapa = etapa.nome if etapa else f'Etapa {nova_etapa_id}'

        # Notifica usuarios do modulo solicitacoes
        dest = NotificationEngine.resolver_destinatarios(
            'solicitacao.etapa_avancou',
            codigo_contrato=contrato_cod,
            usuario_criador_id=solicitacao.id_usuario_solicitante,
        )
        if dest:
            NotificationEngine.notificar(
                tipo_codigo='solicitacao.etapa_avancou',
                destinatarios=dest,
                titulo=f'Etapa atualizada: {nome_etapa}',
                mensagem=f'Solicitacao #{solicitacao.id} - Contrato {contrato_cod}',
                ref_modulo='solicitacoes',
                ref_id=str(solicitacao.id),
                ref_url=ref_url,
            )

        # Se etapa >= LIQUIDADO, notifica financeiro tambem
        ordem_nova = MAPA_ORDEM_ETAPAS.get(nova_etapa_id, 0)
        ordem_liquidado = MAPA_ORDEM_ETAPAS.get(EtapaID.LIQUIDADO, 9)
        if ordem_nova >= ordem_liquidado:
            dest_fin = NotificationEngine.resolver_destinatarios(
                'financeiro.etapa_avancou',
                codigo_contrato=contrato_cod,
            )
            if dest_fin:
                NotificationEngine.notificar(
                    tipo_codigo='financeiro.etapa_avancou',
                    destinatarios=dest_fin,
                    titulo=f'Financeiro: {nome_etapa}',
                    mensagem=f'Solicitacao #{solicitacao.id} - Contrato {contrato_cod}',
                    ref_modulo='financeiro',
                    ref_id=str(solicitacao.id),
                    ref_url=ref_url,
                )

        # Se PAGO, notifica especificamente
        if nova_etapa_id == EtapaID.PAGO:
            dest_pago = NotificationEngine.resolver_destinatarios(
                'solicitacao.paga',
                codigo_contrato=contrato_cod,
                usuario_criador_id=solicitacao.id_usuario_solicitante,
            )
            if dest_pago:
                NotificationEngine.notificar(
                    tipo_codigo='solicitacao.paga',
                    destinatarios=dest_pago,
                    titulo='Pagamento efetuado',
                    mensagem=f'Solicitacao #{solicitacao.id} - Contrato {contrato_cod} foi paga.',
                    ref_modulo='solicitacoes',
                    ref_id=str(solicitacao.id),
                    ref_url=ref_url,
                )
    except Exception as e:
        current_app.logger.warning(f'Erro ao notificar avanco: {e}')


class SolicitacaoService:
    """Serviço para operações de negócio com Solicitações."""

    @staticmethod
    def criar_solicitacao(
        codigo_contrato: str,
        usuario_id: int,
        competencia: str,
        descricao: str = None,
        especificacao: str = None
    ) -> Solicitacao:
        """
        Cria uma nova solicitação de pagamento.

        Args:
            codigo_contrato: Código do contrato
            usuario_id: ID do usuário solicitante
            competencia: Competência (MM/YYYY)
            descricao: Descrição opcional
            especificacao: Especificação opcional

        Returns:
            Solicitação criada
        """
        solicitacao = Solicitacao(
            codigo_contrato=codigo_contrato,
            id_usuario_solicitante=usuario_id,
            competencia=competencia,
            descricao=descricao,
            especificacao=especificacao,
            etapa_atual_id=EtapaID.SOLICITACAO_CRIADA,
            status_geral=StatusGeral.EM_ANDAMENTO,
            data_solicitacao=datetime.now()
        )

        db.session.add(solicitacao)
        db.session.flush()  # Gera o ID sem commitar

        # Registra no histórico na mesma transação
        historico = HistoricoMovimentacao(
            id_solicitacao=solicitacao.id,
            id_etapa_nova=EtapaID.SOLICITACAO_CRIADA,
            id_usuario_responsavel=usuario_id,
            data_movimentacao=datetime.now(),
            comentario="Solicitação criada"
        )
        db.session.add(historico)
        db.session.commit()

        # Hook: notificacoes
        _notificar_solicitacao_criada(solicitacao, usuario_id)

        return solicitacao

    @staticmethod
    def registrar_historico(
        solicitacao_id: int,
        etapa_nova_id: int,
        usuario_id: int,
        etapa_anterior_id: int = None,
        comentario: str = None,
        data: datetime = None
    ) -> HistoricoMovimentacao:
        """
        Registra uma movimentação no histórico.

        Args:
            solicitacao_id: ID da solicitação
            etapa_nova_id: ID da nova etapa
            usuario_id: ID do usuário responsável
            etapa_anterior_id: ID da etapa anterior (opcional)
            comentario: Comentário da movimentação
            data: Data da movimentação (default: agora)

        Returns:
            Registro de histórico criado
        """
        historico = HistoricoMovimentacao(
            id_solicitacao=solicitacao_id,
            id_etapa_anterior=etapa_anterior_id,
            id_etapa_nova=etapa_nova_id,
            id_usuario_responsavel=usuario_id,
            data_movimentacao=data or datetime.now(),
            comentario=comentario
        )

        db.session.add(historico)
        db.session.commit()

        return historico

    @staticmethod
    def avancar_etapa(
        solicitacao: Solicitacao,
        nova_etapa_id: int,
        usuario_id: int,
        comentario: str = None
    ) -> bool:
        """
        Avança a solicitação para uma nova etapa (se válido).

        Args:
            solicitacao: Objeto da solicitação
            nova_etapa_id: ID da nova etapa
            usuario_id: ID do usuário responsável
            comentario: Comentário opcional

        Returns:
            True se avançou, False se não foi possível
        """
        ordem_atual = MAPA_ORDEM_ETAPAS.get(solicitacao.etapa_atual_id, 0)
        ordem_nova = MAPA_ORDEM_ETAPAS.get(nova_etapa_id, 0)

        # Só avança se a nova etapa for maior na ordem
        if ordem_nova <= ordem_atual:
            return False

        etapa_anterior_id = solicitacao.etapa_atual_id
        solicitacao.etapa_atual_id = nova_etapa_id

        SolicitacaoService.registrar_historico(
            solicitacao_id=solicitacao.id,
            etapa_nova_id=nova_etapa_id,
            etapa_anterior_id=etapa_anterior_id,
            usuario_id=usuario_id,
            comentario=comentario
        )

        db.session.commit()

        # Hook: notificacoes
        _notificar_etapa_avancou(solicitacao, nova_etapa_id, etapa_anterior_id)

        return True

    @staticmethod
    def atualizar_status_geral(solicitacao: Solicitacao, novo_status: str) -> None:
        """Atualiza o status geral da solicitação."""
        if solicitacao.status_geral != novo_status:
            solicitacao.status_geral = novo_status
            db.session.commit()

    @staticmethod
    def processar_documentos_sei(
        solicitacao: Solicitacao,
        usuario_id: int
    ) -> bool:
        """
        Processa documentos do SEI e atualiza etapas automaticamente.

        Args:
            solicitacao: Solicitação a processar
            usuario_id: ID do usuário responsável

        Returns:
            True se houve alterações, False caso contrário
        """
        if not solicitacao.protocolo_gerado_sei:
            return False

        # Busca documentos do SEI
        docs = SeiMovimentacao.query.filter_by(
            protocolo_procedimento=solicitacao.protocolo_gerado_sei
        ).order_by(SeiMovimentacao.id_documento.asc()).all()

        if not docs:
            return False

        mudou = False
        ids_series = [str(d.id_serie) for d in docs]
        emails = [d for d in docs if str(d.id_serie) == SerieDocumentoSEI.EMAIL]

        # Processa documentos financeiros (OB > PD > NL)
        doc_nl = next((d for d in docs if str(d.id_serie) == SerieDocumentoSEI.LIQUIDACAO), None)
        doc_pd = next((d for d in docs if str(d.id_serie) == SerieDocumentoSEI.PD), None)
        doc_ob = next((d for d in docs if str(d.id_serie) == SerieDocumentoSEI.OB), None)

        # Atualiza números dos documentos
        if doc_nl and solicitacao.num_nl != str(doc_nl.id_documento):
            solicitacao.num_nl = str(doc_nl.id_documento)
            mudou = True

        if doc_pd and solicitacao.num_pd != str(doc_pd.id_documento):
            solicitacao.num_pd = str(doc_pd.id_documento)
            mudou = True

        if doc_ob and solicitacao.num_ob != str(doc_ob.id_documento):
            solicitacao.num_ob = str(doc_ob.id_documento)
            mudou = True

        # Atualiza status baseado em OB
        if doc_ob:
            if solicitacao.status_geral != StatusGeral.PAGO:
                solicitacao.status_geral = StatusGeral.PAGO
                mudou = True

            if SolicitacaoService.avancar_etapa(
                solicitacao, EtapaID.PAGO, usuario_id, "Pago (OB detectada)"
            ):
                mudou = True

        elif doc_pd or doc_nl:
            if solicitacao.status_geral != StatusGeral.EM_LIQUIDACAO:
                solicitacao.status_geral = StatusGeral.EM_LIQUIDACAO
                mudou = True

            if SolicitacaoService.avancar_etapa(
                solicitacao, EtapaID.LIQUIDADO, usuario_id, "Liquidado"
            ):
                mudou = True

        if mudou:
            db.session.commit()

        return mudou

    @staticmethod
    def calcular_tempo_total(solicitacao: Solicitacao, data_fim: datetime) -> str:
        """Calcula e retorna o tempo total do processo."""
        if not solicitacao.data_solicitacao or not data_fim:
            return None

        diferenca = data_fim - solicitacao.data_solicitacao
        return f"{diferenca.days} dias"

    @staticmethod
    def obter_timeline(solicitacao: Solicitacao) -> List[Dict[str, Any]]:
        """
        Monta os dados da timeline para exibição.

        Returns:
            Lista de dicionários com dados de cada etapa
        """
        todas_etapas = Etapa.query.order_by(Etapa.ordem).all()
        historico = HistoricoMovimentacao.query.filter_by(
            id_solicitacao=solicitacao.id
        ).order_by(HistoricoMovimentacao.data_movimentacao.asc()).all()

        # Mapeia histórico por etapa
        hist_por_etapa = {}
        for h in historico:
            hist_por_etapa[h.id_etapa_nova] = h

        # IDs do grupo Atesto e Fiscalização
        grupo_atesto = {12, 13, 14}

        timeline = []
        data_anterior = None
        for etapa in todas_etapas:
            # Pula etapas 13 e 14 (agrupadas no bloco 12)
            if etapa.id in (13, 14):
                continue

            hist = hist_por_etapa.get(etapa.id)
            data_atual = hist.data_movimentacao if hist else None

            # Calcula tempo decorrido entre etapas
            tempo_decorrido = None
            if data_anterior and data_atual:
                diff = data_atual - data_anterior
                dias = diff.days
                if dias > 0:
                    tempo_decorrido = f"{dias}d"
                else:
                    horas = diff.seconds // 3600
                    if horas > 0:
                        tempo_decorrido = f"{horas}h"
                    else:
                        minutos = diff.seconds // 60
                        tempo_decorrido = f"{minutos}min"

            # Subetapas para o bloco Atesto (etapa 12)
            subetapas = None
            if etapa.id == 12:
                subetapas = []
                for sub_id in [12, 13, 14]:
                    sub_etapa = next((e for e in todas_etapas if e.id == sub_id), None)
                    sub_hist = hist_por_etapa.get(sub_id)
                    if sub_etapa:
                        subetapas.append({
                            'nome': sub_etapa.nome,
                            'data': sub_hist.data_movimentacao if sub_hist else None,
                            'concluida': sub_hist is not None
                        })

            # Marca como concluída se qualquer sub do grupo concluiu (para etapa 12)
            concluida = hist is not None
            if etapa.id == 12:
                concluida = any(hist_por_etapa.get(sid) for sid in grupo_atesto)

            # Atual: se a etapa atual está neste grupo
            atual = etapa.id == solicitacao.etapa_atual_id
            if etapa.id == 12:
                atual = solicitacao.etapa_atual_id in grupo_atesto

            # Pula etapa 6 (Pago) - agrupada com Financeiro (5)
            if etapa.id == 6:
                continue

            timeline.append({
                'etapa': etapa,
                'concluida': concluida,
                'data': data_atual,
                'atual': atual,
                'comentario': hist.comentario if hist else None,
                'tempo_decorrido': tempo_decorrido,
                'subetapas': subetapas
            })

            if data_atual:
                data_anterior = data_atual

        return timeline
