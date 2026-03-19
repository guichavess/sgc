"""
Cliente Trino para consulta ao Data Lake SEI.
Conexão read-only via HTTPS com BasicAuthentication.
Inclui retry automático e timeout configurável.
"""
import logging
import time
from flask import current_app

try:
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
    HAS_TRINO = True
except ImportError:
    HAS_TRINO = False

logger = logging.getLogger(__name__)

# Configurações de retry
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10  # delay entre tentativas
DEFAULT_TIMEOUT = 120     # timeout em segundos (2 min)


def get_trino_connection():
    """Cria e retorna uma conexão Trino usando config da aplicação Flask."""
    if not HAS_TRINO:
        raise ImportError(
            'Módulo trino não instalado. Execute: pip install trino[external-authentication]'
        )
    cfg = current_app.config
    timeout = cfg.get('TRINO_TIMEOUT', DEFAULT_TIMEOUT)
    return connect(
        host=cfg['TRINO_HOST'],
        port=cfg['TRINO_PORT'],
        user=cfg['TRINO_USER'],
        catalog=cfg['TRINO_CATALOG'],
        schema=cfg['TRINO_SCHEMA'],
        http_scheme='https',
        auth=BasicAuthentication(cfg['TRINO_USER'], cfg['TRINO_PASSWORD']),
        verify=False,
        request_timeout=timeout,
    )


def _is_retryable_error(e):
    """Verifica se o erro é de conexão/timeout (vale retry) ou de query (não vale)."""
    error_name = type(e).__name__
    # Erros de query do Trino (SQL inválido, coluna não existe, etc.) — NÃO fazer retry
    non_retryable = ('TrinoUserError', 'TrinoQueryError')
    if error_name in non_retryable:
        return False
    # Erros de conexão, timeout, rede — FAZER retry
    return True


def execute_query(sql, params=None):
    """Executa query no Trino e retorna lista de dicts.

    Inclui retry automático em caso de timeout/erro de conexão.
    Erros de query (SQL inválido, coluna inexistente) falham imediatamente.

    Args:
        sql: Query SQL a executar.
        params: Parâmetros opcionais (não suportado pelo Trino, reservado).

    Returns:
        Lista de dicts com nomes de colunas como chaves.
    """
    last_error = None

    for tentativa in range(1, MAX_RETRIES + 1):
        conn = None
        try:
            conn = get_trino_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if tentativa > 1:
                logger.info(f'Query Trino bem-sucedida na tentativa {tentativa}')
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            last_error = e
            error_name = type(e).__name__
            logger.warning(
                f'Trino tentativa {tentativa}/{MAX_RETRIES} falhou ({error_name}): {e}'
            )
            # Se é erro de query (não de conexão), não adianta tentar de novo
            if not _is_retryable_error(e):
                logger.error('Erro de query Trino (não retentável) — abortando.')
                raise
            if tentativa < MAX_RETRIES:
                logger.info(f'Aguardando {RETRY_DELAY_SECONDS}s antes de tentar novamente...')
                time.sleep(RETRY_DELAY_SECONDS)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    logger.exception(f'Erro ao executar query Trino após {MAX_RETRIES} tentativas')
    raise last_error


def describe_table(table_name='sei.sei_processo.sei_consolidado_sead_sefaz_cgfr'):
    """Lista as colunas disponíveis em uma tabela/view do Trino.

    Returns:
        Lista de dicts com Column, Type, Extra, Comment.
    """
    sql = f"DESCRIBE {table_name}"
    return execute_query(sql)


def fetch_processos_cgfr(limit=5000):
    """Busca todos os processos CGFR do Data Lake.

    Usa SELECT * para não depender de nomes de colunas fixos,
    já que a view pode mudar no Data Lake.

    Returns:
        Lista de dicts com dados dos processos.
    """
    sql = f"""
        SELECT *
        FROM sei.sei_processo.sei_consolidado_sead_sefaz_cgfr
        LIMIT {int(limit)}
    """
    return execute_query(sql)
