"""
Serviço de Sincronização do módulo de Pagamentos (SEI + Etapas + Saldos).

Centraliza a lógica das 3 fases da rotina "Sincronizar Tudo" para que ela possa
ser executada de duas formas:

- **Manual**: pelo botão do dashboard, via endpoints SSE (que apenas reutilizam
  ``atualizar_saldos_em_lote`` e registram o log no fim).
- **Agendada**: pelo job do APScheduler, via ``executar_sincronizacao_completa``.

A "última atualização" exibida no dashboard vem da tabela ``sis_sincronizacao_log``
(model ``SincronizacaoLog``), que é a fonte única e confiável — independente de
paginação ou de existência de saldo por item.
"""
from datetime import datetime
from typing import Iterable, Optional, Tuple, List

import concurrent.futures

from flask import current_app

from app.extensions import db
from app.models import Solicitacao, SaldoEmpenho, Etapa, SincronizacaoLog
from app.services.saldo_service import SaldoService


# Endpoint de download de documentos do SEI (mesmo usado na Fase 1 SSE).
SEI_DOCUMENTOS_URL = (
    "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
)


# =============================================================================
# FASE 3 — SALDOS EM LOTE (corrige cascata de erros + 1 único commit)
# =============================================================================

def atualizar_saldos_em_lote(
    combinacoes: Iterable[Tuple[str, str]],
    progress_cb=None,
) -> Tuple[int, int, List[str]]:
    """
    Recalcula o saldo de cada (contrato, competência) e grava em lote.

    Estratégia em duas etapas para nunca "envenenar" a sessão:
      1. Cálculo (somente leitura): erro de uma combinação é isolado e apenas
         registrado — não afeta as demais nem deixa a sessão em estado inválido.
      2. Gravação: faz upsert de todos os saldos calculados e UM único
         ``commit()`` no final (atomicidade + performance).

    Args:
        combinacoes: iterável de tuplas ``(codigo_contrato, competencia)``.
        progress_cb: callback opcional ``(indice, total)`` para progresso.

    Returns:
        ``(atualizados, erros, lista_erros)``
    """
    combinacoes = list(combinacoes)
    total = len(combinacoes)
    lista_erros: List[str] = []
    calculados: List[Tuple[str, str, float]] = []

    # --- Etapa 1: cálculo (read-only, falha isolada por item) ---
    for i, (contrato, competencia) in enumerate(combinacoes):
        try:
            saldo = SaldoService.calcular_saldo_disponivel(contrato, competencia)
            calculados.append((contrato, competencia, saldo))
        except Exception as e:  # noqa: BLE001 — erro de um item não pode parar o lote
            lista_erros.append(f"{contrato}/{competencia}: {e}")
            current_app.logger.warning(
                f"[SYNC-SALDOS] Falha ao calcular {contrato}/{competencia}: {e}"
            )
        if progress_cb:
            progress_cb(i + 1, total)

    if not calculados:
        return 0, len(lista_erros), lista_erros

    # --- Etapa 2: gravação em lote (upsert) + 1 commit ---
    try:
        # Pré-carrega os saldos existentes mais recentes (evita N+1)
        pares = [(c, comp) for c, comp, _ in calculados]
        existentes = {}
        for s in (
            SaldoEmpenho.query
            .filter(db.tuple_(SaldoEmpenho.cod_contrato, SaldoEmpenho.competencia).in_(pares))
            .order_by(SaldoEmpenho.data.asc())
            .all()
        ):
            # asc(): o último a sobrescrever é o mais recente
            existentes[(s.cod_contrato, s.competencia)] = s

        agora = datetime.now()
        for contrato, competencia, saldo in calculados:
            registro = existentes.get((contrato, competencia))
            if registro is not None:
                registro.saldo = saldo
                registro.data = agora
            else:
                db.session.add(SaldoEmpenho(
                    cod_contrato=contrato,
                    competencia=competencia,
                    saldo=saldo,
                    data=agora,
                ))

        db.session.commit()
        atualizados = len(calculados)
    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        current_app.logger.error(f"[SYNC-SALDOS] Erro ao gravar lote de saldos: {e}")
        lista_erros.append(f"Gravação em lote: {e}")
        atualizados = 0

    return atualizados, len(lista_erros), lista_erros


# =============================================================================
# LOG DE SINCRONIZAÇÃO — fonte da "última atualização"
# =============================================================================

def registrar_log(
    *,
    origem: str = 'manual',
    status: str = 'sucesso',
    docs: int = 0,
    etapas: int = 0,
    saldos: int = 0,
    erros: int = 0,
    usuario_id: Optional[int] = None,
    iniciado_em: Optional[datetime] = None,
) -> SincronizacaoLog:
    """Cria e persiste uma linha de log já finalizada (``finalizado_em=now``)."""
    log = SincronizacaoLog(
        iniciado_em=iniciado_em or datetime.now(),
        finalizado_em=datetime.now(),
        status=status,
        origem=origem,
        docs_atualizados=docs or 0,
        etapas_avancadas=etapas or 0,
        saldos_atualizados=saldos or 0,
        erros=erros or 0,
        usuario_id=usuario_id,
    )
    db.session.add(log)
    db.session.commit()
    return log


def obter_ultima_sincronizacao() -> Optional[SincronizacaoLog]:
    """Retorna a execução concluída mais recente (ou ``None`` se não houver)."""
    return (
        SincronizacaoLog.query
        .filter(SincronizacaoLog.finalizado_em.isnot(None))
        .order_by(SincronizacaoLog.finalizado_em.desc())
        .first()
    )


# =============================================================================
# ORQUESTRAÇÃO COMPLETA (usada pelo job agendado, server-side)
# =============================================================================

def executar_sincronizacao_completa(
    usuario_id: Optional[int] = None,
    origem: str = 'agendada',
) -> dict:
    """
    Executa as 3 fases server-side (sem streaming) e registra o log.

    Reutiliza os helpers já existentes em ``app.solicitacoes.routes.api``
    (``baixar_documentos_thread`` e ``processar_item_sei``) — import tardio para
    evitar import circular (api.py importa serviços).

    Returns:
        Resumo dict com contadores e status.
    """
    # Import tardio: quebra o ciclo api -> services -> api
    from app.solicitacoes.routes.api import (
        baixar_documentos_thread, processar_item_sei
    )
    from app.services.sei_auth import gerar_token_sei_admin

    inicio = datetime.now()
    docs_ok = etapas_ok = saldos_ok = 0
    erros_total = 0

    app_real = current_app._get_current_object()
    token_sei = gerar_token_sei_admin()

    # --- FASE 1: Download de documentos do SEI ---
    if not token_sei:
        current_app.logger.error('[SYNC] Token SEI indisponível — abortando sincronização.')
        log = registrar_log(
            origem=origem, status='erro', usuario_id=usuario_id, iniciado_em=inicio,
        )
        return {'status': 'erro', 'msg': 'Token SEI indisponível', 'log_id': log.id}

    pendentes = Solicitacao.query.filter(
        Solicitacao.protocolo_gerado_sei.isnot(None),
        Solicitacao.etapa_atual_id != 6,
        Solicitacao.status_geral != 'CANCELADO',
    ).all()
    protocolos = [s.protocolo_gerado_sei for s in pendentes]

    if protocolos:
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            futures = {
                executor.submit(
                    baixar_documentos_thread, app_real, prot, token_sei, SEI_DOCUMENTOS_URL
                ): prot for prot in protocolos
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    sucesso, _msg = future.result()
                    if sucesso:
                        docs_ok += 1
                    else:
                        erros_total += 1
                except Exception as exc:  # noqa: BLE001
                    erros_total += 1
                    app_real.logger.warning(f'[SYNC] Erro download {futures[future]}: {exc}')

    # --- FASE 2: Avança etapas a partir das movimentações baixadas ---
    mapa_ordem = {e.id: e.ordem for e in Etapa.query.all()}
    ids_pendentes = [
        s.id for s in Solicitacao.query.filter(
            Solicitacao.etapa_atual_id != 6,
            Solicitacao.status_geral != 'CANCELADO',
        ).all()
    ]
    if ids_pendentes:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(
                    processar_item_sei, app_real, sid, token_sei, usuario_id, mapa_ordem
                ): sid for sid in ids_pendentes
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    if future.result():
                        etapas_ok += 1
                except Exception as exc:  # noqa: BLE001
                    erros_total += 1
                    app_real.logger.warning(f'[SYNC] Erro etapa {futures[future]}: {exc}')

    # --- FASE 3: Saldos em lote ---
    combinacoes = [
        (c, comp) for (c, comp) in db.session.query(
            Solicitacao.codigo_contrato, Solicitacao.competencia
        ).distinct().filter(Solicitacao.competencia.isnot(None)).all()
    ]
    saldos_ok, erros_saldo, _ = atualizar_saldos_em_lote(combinacoes)
    erros_total += erros_saldo

    status = 'sucesso' if erros_total == 0 else 'parcial'
    log = registrar_log(
        origem=origem, status=status, docs=docs_ok, etapas=etapas_ok,
        saldos=saldos_ok, erros=erros_total, usuario_id=usuario_id, iniciado_em=inicio,
    )

    return {
        'status': status,
        'docs': docs_ok,
        'etapas': etapas_ok,
        'saldos': saldos_ok,
        'erros': erros_total,
        'log_id': log.id,
    }
