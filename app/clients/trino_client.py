"""
Cliente Trino para consulta ao Data Lake SEI.
Conexão read-only via HTTPS com BasicAuthentication.
"""
import logging
from flask import current_app

try:
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
    HAS_TRINO = True
except ImportError:
    HAS_TRINO = False

logger = logging.getLogger(__name__)


def get_trino_connection():
    """Cria e retorna uma conexão Trino usando config da aplicação Flask."""
    if not HAS_TRINO:
        raise ImportError(
            'Módulo trino não instalado. Execute: pip install trino[external-authentication]'
        )
    cfg = current_app.config
    return connect(
        host=cfg['TRINO_HOST'],
        port=cfg['TRINO_PORT'],
        user=cfg['TRINO_USER'],
        catalog=cfg['TRINO_CATALOG'],
        schema=cfg['TRINO_SCHEMA'],
        http_scheme='https',
        auth=BasicAuthentication(cfg['TRINO_USER'], cfg['TRINO_PASSWORD']),
        verify=False,
    )


def execute_query(sql, params=None):
    """Executa query no Trino e retorna lista de dicts.

    Args:
        sql: Query SQL a executar.
        params: Parâmetros opcionais (não suportado pelo Trino, reservado).

    Returns:
        Lista de dicts com nomes de colunas como chaves.
    """
    conn = get_trino_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception:
        logger.exception('Erro ao executar query Trino')
        raise
    finally:
        conn.close()


def fetch_processos_cgfr(limit=5000):
    """Busca todos os processos CGFR do Data Lake.

    Returns:
        Lista de dicts com dados dos processos.
    """
    sql = f"""
        SELECT *
        FROM sei.sei_processo.sei_consolidado_sead_sefaz_cgfr
        LIMIT {int(limit)}
    """
    return execute_query(sql)
