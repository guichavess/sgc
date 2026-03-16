"""
Serviço de sincronização Trino → MySQL para processos CGFR.
Busca dados do Data Lake e faz UPSERT no banco local,
NUNCA sobrescrevendo campos editáveis do usuário.
"""
import logging
from datetime import datetime

from sqlalchemy import text
from app.extensions import db
from app.cgfr.repositories.processo_trino_repo import ProcessoTrinoRepository

logger = logging.getLogger(__name__)

# Colunas sync que serão atualizadas no UPDATE (nunca editáveis)
# Baseado nas colunas reais da view Trino: sei_consolidado_sead_sefaz_cgfr
SYNC_COLUMNS = [
    'especificacao', 'tipo_processo', 'data_hora_processo',
    'tramitado_sead_cgfr', 'recebido_cgfr', 'data_recebido_cgfr',
    'devolvido_cgfr_sead', 'data_devolvido_cgfr_sead',
]


class SyncService:
    """Sincroniza processos do Data Lake Trino para o MySQL local."""

    @staticmethod
    def sync():
        """Executa sincronização completa.

        Busca todos os registros do Trino e faz UPSERT no MySQL:
        - INSERT novos com data_inclusao = now(), editáveis = NULL
        - UPDATE existentes: APENAS campos sync, NUNCA sobrescreve editáveis

        Returns:
            dict: {inserted, updated, skipped, errors, total}
        """
        resultado = {'inserted': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'total': 0}

        try:
            registros = ProcessoTrinoRepository.fetch_all()
            resultado['total'] = len(registros)
        except Exception as e:
            logger.exception('Erro ao buscar dados do Trino')
            return {'error': str(e), **resultado}

        # Processa em lotes para reduzir round-trips ao MySQL
        BATCH_SIZE = 500
        batch = []

        for reg in registros:
            protocolo = reg.get('protocolo_formatado')
            if not protocolo:
                resultado['skipped'] += 1
                continue
            try:
                dados_sync = SyncService._extrair_dados_sync(reg)
                batch.append(dados_sync)
            except Exception:
                logger.exception(f'Erro ao extrair dados {reg.get("protocolo_formatado", "?")}')
                resultado['errors'] += 1
                continue

            if len(batch) >= BATCH_SIZE:
                SyncService._flush_batch(batch, resultado)
                batch = []

        if batch:
            SyncService._flush_batch(batch, resultado)

        try:
            db.session.commit()
        except Exception:
            logger.exception('Erro ao commit da sincronização')
            db.session.rollback()
            resultado['errors'] += 1

        logger.info(
            f'Sync CGFR concluído: {resultado["inserted"]} inseridos, '
            f'{resultado["updated"]} atualizados, {resultado["errors"]} erros'
        )
        return resultado

    @staticmethod
    def _flush_batch(batch, resultado):
        """Executa um lote de upserts no MySQL."""
        for dados_sync in batch:
            try:
                update_parts_list = [
                    f'{col} = VALUES({col})' for col in SYNC_COLUMNS if col in dados_sync
                ]
                if dados_sync.get('valor_solicitado') is not None:
                    update_parts_list.append(
                        'valor_solicitado = COALESCE(valor_solicitado, VALUES(valor_solicitado))'
                    )
                update_parts = ', '.join(update_parts_list)

                colunas = list(dados_sync.keys())
                placeholders = ', '.join(f':{col}' for col in colunas)
                col_names = ', '.join(colunas)

                sql = text(f"""
                    INSERT INTO cgfr_processo_enviado ({col_names})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_parts}
                """)

                result = db.session.execute(sql, dados_sync)

                if result.rowcount == 1:
                    resultado['inserted'] += 1
                elif result.rowcount == 2:
                    resultado['updated'] += 1
                else:
                    resultado['skipped'] += 1
            except Exception:
                logger.exception(f'Erro upsert {dados_sync.get("processo_formatado", "?")}')
                resultado['errors'] += 1

        # Flush intermediário para não acumular tudo na transaction
        try:
            db.session.flush()
        except Exception:
            logger.exception('Erro no flush intermediário')
            db.session.rollback()

    @staticmethod
    def _fmt_datetime(dt):
        """Formata datetime do Trino para string dd/mm/yyyy HH:MM:SS."""
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.strftime('%d/%m/%Y %H:%M:%S')
        return str(dt)

    @staticmethod
    def _to_decimal(value):
        """Converte valor numérico do Trino para string decimal MySQL."""
        if value is None:
            return None
        try:
            return f'{float(value):.2f}'
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _extrair_dados_sync(reg):
        """Extrai e mapeia dados do registro Trino para colunas MySQL sync.

        Mapeamento baseado no sistema original:
        - tipo_procedimento → especificacao
        - tipo_processo → tipo_processo (fallback)
        - data_hora_processo → data_hora_processo
        - foi_enviado_cgfr + dt_enviado → tramitado_sead_cgfr
        - foi_recebido_cgfr + dt_recebido → recebido_cgfr + data_recebido_cgfr
        - foi_devolvido_cgfr + dt_devolvido → devolvido_cgfr_sead + data_devolvido_cgfr_sead
        - valor_solicitado → valor_solicitado (somente se MySQL estiver NULL)
        """
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        dados = {
            'processo_formatado': reg.get('protocolo_formatado'),
            'especificacao': reg.get('tipo_procedimento') or '',
            'tipo_processo': reg.get('tipo_processo') or '',
            'data_hora_processo': str(reg['data_hora_processo']) if reg.get('data_hora_processo') else None,
            'tramitado_sead_cgfr': SyncService._fmt_datetime(reg.get('dt_enviado')) if reg.get('foi_enviado_cgfr') else None,
            'recebido_cgfr': 1 if reg.get('foi_recebido_cgfr') else 0,
            'data_recebido_cgfr': SyncService._fmt_datetime(reg.get('dt_recebido')) if reg.get('foi_recebido_cgfr') else None,
            'devolvido_cgfr_sead': 1 if reg.get('foi_devolvido_cgfr') else 0,
            'data_devolvido_cgfr_sead': SyncService._fmt_datetime(reg.get('dt_devolvido')) if reg.get('foi_devolvido_cgfr') else None,
            'data_inclusao': now,
        }

        # valor_solicitado do Trino (só insere se presente, UPDATE condicional via COALESCE)
        val_solic = SyncService._to_decimal(reg.get('valor_solicitado'))
        if val_solic is not None:
            dados['valor_solicitado'] = val_solic

        return {k: v for k, v in dados.items() if k is not None}
