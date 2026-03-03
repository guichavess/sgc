"""
Scheduler de tarefas periodicas do SGC.

Usa APScheduler (BackgroundScheduler) integrado ao Flask.

Jobs:
    1. verificar_vigencias_contratos - diario 08:00
    2. lembrete_ne_pendentes - diario 09:00
    3. limpar_notificacoes_expiradas - semanal (domingo 03:00)
"""
from datetime import datetime, timedelta
from flask import Flask


def init_scheduler(app: Flask):
    """
    Inicializa o APScheduler com 3 jobs periodicos.

    Chamado em create_app() apos todas as extensoes e blueprints.
    Protege contra dupla inicializacao (Werkzeug reloader).
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        app.logger.warning(
            'APScheduler nao instalado. Scheduler desabilitado. '
            'Instale com: pip install APScheduler'
        )
        return

    # Evita dupla execucao no reloader do Flask (debug mode)
    import os
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true' and app.debug:
        return

    scheduler = BackgroundScheduler(daemon=True)

    # Job 1: Verificar vigencias de contratos - diario as 08:00
    scheduler.add_job(
        func=_job_verificar_vigencias,
        trigger=CronTrigger(hour=8, minute=0),
        id='verificar_vigencias',
        name='Verificar vigencias de contratos',
        kwargs={'app': app},
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Job 2: Lembrete de NEs pendentes - diario as 09:00
    scheduler.add_job(
        func=_job_lembrete_ne_pendentes,
        trigger=CronTrigger(hour=9, minute=0),
        id='lembrete_ne_pendentes',
        name='Lembrete de NEs pendentes',
        kwargs={'app': app},
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Job 3: Limpar notificacoes expiradas - domingo as 03:00
    scheduler.add_job(
        func=_job_limpar_expiradas,
        trigger=CronTrigger(day_of_week='sun', hour=3, minute=0),
        id='limpar_expiradas',
        name='Limpar notificacoes expiradas',
        kwargs={'app': app},
        replace_existing=True,
        misfire_grace_time=7200,
    )

    scheduler.start()
    app.logger.info('Scheduler inicializado com 3 jobs.')


# =============================================================================
# JOB 1: VERIFICAR VIGENCIAS DE CONTRATOS
# =============================================================================

def _job_verificar_vigencias(app: Flask):
    """
    Verifica contratos com vigencia expirando nos proximos 90 dias.

    Escalacao dinamica de nivel:
        90-61 dias: lembrete
        60-31 dias: alerta
        30-0  dias: critica
        Expirado:   critica (contrato.vigencia_expirada)
    """
    with app.app_context():
        try:
            from app.extensions import db
            from app.models.contrato import Contrato
            from app.services.notification_engine import NotificationEngine

            hoje = datetime.now().date()
            limite_90 = hoje + timedelta(days=90)

            # Busca contratos com vigencia entre hoje e 90 dias
            contratos = Contrato.query.filter(
                Contrato.dataFimVigenciaTotal.isnot(None),
                Contrato.dataFimVigenciaTotal <= limite_90,
                Contrato.vigencia_notificacao_silenciada == False,
            ).all()

            total_notificados = 0

            for contrato in contratos:
                try:
                    fim_vigencia = contrato.dataFimVigenciaTotal
                    if hasattr(fim_vigencia, 'date'):
                        fim_vigencia = fim_vigencia.date()

                    dias_restantes = (fim_vigencia - hoje).days

                    # Determinar tipo e nivel
                    if dias_restantes < 0:
                        tipo_codigo = 'contrato.vigencia_expirada'
                        nivel = 'critica'
                        titulo = f'Vigencia EXPIRADA: {contrato.codigo}'
                        mensagem = (
                            f'O contrato {contrato.codigo} '
                            f'({contrato.nomeContratado or "N/A"}) '
                            f'expirou em {fim_vigencia.strftime("%d/%m/%Y")}.'
                        )
                    elif dias_restantes <= 30:
                        tipo_codigo = 'contrato.vigencia_expirando'
                        nivel = 'critica'
                        titulo = f'URGENTE: Vigencia em {dias_restantes} dias'
                        mensagem = (
                            f'O contrato {contrato.codigo} '
                            f'({contrato.nomeContratado or "N/A"}) '
                            f'expira em {fim_vigencia.strftime("%d/%m/%Y")} '
                            f'({dias_restantes} dias restantes).'
                        )
                    elif dias_restantes <= 60:
                        tipo_codigo = 'contrato.vigencia_expirando'
                        nivel = 'alerta'
                        titulo = f'Vigencia expirando: {contrato.codigo}'
                        mensagem = (
                            f'O contrato {contrato.codigo} '
                            f'({contrato.nomeContratado or "N/A"}) '
                            f'expira em {fim_vigencia.strftime("%d/%m/%Y")} '
                            f'({dias_restantes} dias restantes).'
                        )
                    else:
                        tipo_codigo = 'contrato.vigencia_expirando'
                        nivel = 'lembrete'
                        titulo = f'Lembrete de vigencia: {contrato.codigo}'
                        mensagem = (
                            f'O contrato {contrato.codigo} '
                            f'({contrato.nomeContratado or "N/A"}) '
                            f'expira em {fim_vigencia.strftime("%d/%m/%Y")} '
                            f'({dias_restantes} dias restantes).'
                        )

                    # Resolver destinatarios
                    dest = NotificationEngine.resolver_destinatarios(
                        tipo_codigo,
                        codigo_contrato=contrato.codigo,
                    )

                    if dest:
                        ref_url = f'/prestacoes-contratos/contrato/{contrato.codigo}'
                        NotificationEngine.notificar(
                            tipo_codigo=tipo_codigo,
                            destinatarios=dest,
                            titulo=titulo,
                            mensagem=mensagem,
                            ref_modulo='prestacoes_contratos',
                            ref_id=str(contrato.codigo),
                            ref_url=ref_url,
                            nivel_override=nivel,
                        )
                        total_notificados += 1

                except Exception as e:
                    app.logger.warning(
                        f'Erro ao verificar vigencia contrato {contrato.codigo}: {e}'
                    )

            app.logger.info(
                f'Job verificar_vigencias: {len(contratos)} contratos analisados, '
                f'{total_notificados} notificados.'
            )

        except Exception as e:
            app.logger.error(f'Erro no job verificar_vigencias: {e}')


# =============================================================================
# JOB 2: LEMBRETE DE NES PENDENTES
# =============================================================================

def _job_lembrete_ne_pendentes(app: Flask):
    """
    Verifica solicitacoes com NE pendente de insercao no financeiro.
    Notifica usuarios com permissao financeiro.editar.
    """
    with app.app_context():
        try:
            from app.extensions import db
            from app.models import Solicitacao
            from app.services.notification_engine import NotificationEngine
            from app.constants import EtapaID

            # Busca solicitacoes liquidadas (etapa >= LIQUIDADO) sem NE
            solicitacoes = Solicitacao.query.filter(
                Solicitacao.etapa_atual_id.in_([
                    EtapaID.LIQUIDADO, EtapaID.PAGO
                ]),
                Solicitacao.status_geral != 'CANCELADO',
            ).all()

            # Filtra apenas as que nao tem NE inserida
            pendentes = [
                s for s in solicitacoes
                if not s.num_nl and not s.num_pd
            ]

            if not pendentes:
                app.logger.info('Job lembrete_ne: nenhuma NE pendente.')
                return

            dest = NotificationEngine.resolver_destinatarios(
                'financeiro.ne_pendente',
            )

            if dest:
                NotificationEngine.notificar(
                    tipo_codigo='financeiro.ne_pendente',
                    destinatarios=dest,
                    titulo=f'{len(pendentes)} NEs pendentes de insercao',
                    mensagem=(
                        f'Existem {len(pendentes)} solicitacoes aguardando '
                        f'insercao de NE no modulo financeiro.'
                    ),
                    ref_modulo='financeiro',
                    ref_url='/financeiro/pendencias-ne',
                )

            app.logger.info(
                f'Job lembrete_ne: {len(pendentes)} pendentes, {len(dest)} notificados.'
            )

        except Exception as e:
            app.logger.error(f'Erro no job lembrete_ne: {e}')


# =============================================================================
# JOB 3: LIMPAR NOTIFICACOES EXPIRADAS
# =============================================================================

def _job_limpar_expiradas(app: Flask):
    """
    Remove notificacoes expiradas (expires_at ultrapassado)
    e notificacoes lidas com mais de 90 dias.
    """
    with app.app_context():
        try:
            from app.repositories.notificacao_repository import NotificacaoRepository

            removidas = NotificacaoRepository.limpar_expiradas()

            app.logger.info(
                f'Job limpar_expiradas: {removidas} notificacoes removidas.'
            )

        except Exception as e:
            app.logger.error(f'Erro no job limpar_expiradas: {e}')
