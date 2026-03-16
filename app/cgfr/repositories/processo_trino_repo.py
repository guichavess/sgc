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
        import re
        # Sanitizar: protocolo deve ser apenas dígitos, pontos e barras
        if not re.match(r'^[\d./ -]+$', protocolo):
            logger.warning(f'Protocolo com caracteres inválidos: {protocolo}')
            return None

        sql = """
            SELECT protocolo_formatado, tipo_procedimento, tipo_processo,
                   data_hora_processo, foi_enviado_cgfr, dt_enviado,
                   foi_recebido_cgfr, dt_recebido, foi_devolvido_cgfr,
                   dt_devolvido, valor_solicitado
            FROM sei.sei_processo.sei_consolidado_sead_sefaz_cgfr
            WHERE protocolo_formatado = '{}'
            LIMIT 1
        """.format(protocolo.replace("'", "''"))

        resultados = execute_query(sql)
        return resultados[0] if resultados else None
