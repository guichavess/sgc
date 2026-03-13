"""
Repository Trino (read-only) para processos CGFR.
Consulta o Data Lake SEI.
"""
import logging

from app.clients.trino_client import fetch_processos_cgfr, execute_query

logger = logging.getLogger(__name__)


class ProcessoTrinoRepository:
    """Repository read-only para consulta ao Data Lake Trino."""

    @staticmethod
    def fetch_all(limit=5000):
        """Busca todos os processos do Data Lake.

        Returns:
            Lista de dicts com dados dos processos.
        """
        return fetch_processos_cgfr(limit=limit)

    @staticmethod
    def fetch_by_protocolo(protocolo):
        """Busca um processo específico no Data Lake.

        Args:
            protocolo: Protocolo formatado do processo.

        Returns:
            Dict com dados do processo ou None.
        """
        sql = """
            SELECT *
            FROM sei.sei_processo.sei_consolidado_sead_sefaz_cgfr
            WHERE protocolo_formatado = '{}'
            LIMIT 1
        """.format(protocolo.replace("'", "''"))

        resultados = execute_query(sql)
        return resultados[0] if resultados else None
