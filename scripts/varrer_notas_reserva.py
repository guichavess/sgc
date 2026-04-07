"""
Script batch para varrer Notas de Reserva (NR) em processos de diárias.
Baixa PDFs do SEI (IdSerie=425), extrai o código NR via pypdfium2 e salva.

Uso:
    python scripts/varrer_notas_reserva.py                    # Todos sem NR
    python scripts/varrer_notas_reserva.py --protocolo X      # Um específico
    python scripts/varrer_notas_reserva.py --workers 6        # Controlar paralelismo
    python scripts/varrer_notas_reserva.py --force            # Re-varrer mesmo os que já têm NR
"""
import os
import sys
import time
import argparse
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

MAX_WORKERS_DEFAULT = 4
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # segundos (multiplica por tentativa)

# Lock global para pypdfium2 (lib C não é thread-safe)
_pdf_lock = threading.Lock()


def get_token():
    """Gera token SEI admin."""
    from app.services.sei_auth import gerar_token_sei_admin
    token = gerar_token_sei_admin()
    if not token:
        raise RuntimeError("Falha ao gerar token SEI")
    return token


def buscar_itinerarios(protocolo=None, force=False):
    """
    Retorna lista de (itinerario_id, sei_protocolo, doc_formatado_nr)
    para itinerários que têm NR no SEI mas não têm código NR extraído.

    Pré-filtra via JOIN com diarias_movimentacao (IdSerie=425) para
    evitar processar itinerários sem documento de NR.
    """
    from app.models.diaria import DiariasItinerario, DiariasMovimentacao, DiariasDocumentoSei
    from app.extensions import db
    from sqlalchemy import and_

    # Query base: itinerários com processo SEI e que têm NR na movimentação
    query = (
        db.session.query(
            DiariasItinerario.id,
            DiariasItinerario.sei_protocolo,
            DiariasMovimentacao.documento_formatado
        )
        .join(
            DiariasMovimentacao,
            and_(
                DiariasMovimentacao.protocolo_procedimento == DiariasItinerario.sei_protocolo,
                DiariasMovimentacao.id_serie == 425
            )
        )
        .filter(DiariasItinerario.sei_protocolo.isnot(None))
    )

    if protocolo:
        query = query.filter(DiariasItinerario.sei_protocolo == protocolo)

    if not force:
        # Exclui itinerários que já têm NR preenchida (codigo != NULL)
        subq_com_nr = (
            db.session.query(DiariasDocumentoSei.itinerario_id)
            .filter(
                DiariasDocumentoSei.tipo_documento == 'nota_reserva',
                DiariasDocumentoSei.codigo.isnot(None)
            )
            .subquery()
            .select()
        )
        query = query.filter(~DiariasItinerario.id.in_(subq_com_nr))

    return query.all()


def processar_nr(item, token, app):
    """
    Processa um itinerário: baixa PDF da NR e extrai o código.
    Thread-safe: cria app_context próprio e remove session no finally.

    Args:
        item: tuple (itinerario_id, sei_protocolo, doc_formatado)
        token: token SEI compartilhado
        app: Flask app (para criar app_context na thread)

    Returns:
        dict com resultado do processamento
    """
    itin_id, protocolo, doc_formatado = item
    resultado = {
        'itinerario_id': itin_id,
        'protocolo': protocolo,
        'doc_formatado': doc_formatado,
        'nr_codigo': None,
        'sucesso': False,
        'erro': None,
    }

    with app.app_context():
        from app.extensions import db
        try:
            from app.services.diarias_sei_integration import (
                _baixar_documento_com_token, extrair_nr_de_pdf
            )
            from app.models.diaria import DiariasItinerario

            # Baixa PDF com retry
            pdf_bytes = None
            for tentativa in range(1, MAX_RETRIES + 1):
                pdf_bytes = _baixar_documento_com_token(doc_formatado, token)
                if pdf_bytes:
                    break
                if tentativa < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * tentativa)
                    logger.warning(
                        f"  [{protocolo}] Retry {tentativa}/{MAX_RETRIES} download {doc_formatado}"
                    )

            if not pdf_bytes:
                resultado['erro'] = f'Falha download PDF {doc_formatado}'
                return resultado

            # Extrai código NR (lock: pypdfium2 não é thread-safe)
            with _pdf_lock:
                nr_codigo = extrair_nr_de_pdf(pdf_bytes)
            if not nr_codigo:
                resultado['erro'] = f'NR não encontrada no PDF {doc_formatado}'
                return resultado

            # Salva no banco
            itinerario = db.session.get(DiariasItinerario, itin_id)
            if itinerario:
                itinerario.set_doc('nota_reserva', sei_formatado=doc_formatado, codigo=nr_codigo)
                db.session.commit()
                resultado['sucesso'] = True
                resultado['nr_codigo'] = nr_codigo
            else:
                resultado['erro'] = f'Itinerário {itin_id} não encontrado'

        except Exception as e:
            resultado['erro'] = str(e)[:200]
            try:
                db.session.rollback()
            except Exception:
                pass
        finally:
            try:
                db.session.remove()
            except Exception:
                pass

    return resultado


def main():
    parser = argparse.ArgumentParser(description='Varrer Notas de Reserva em processos de diárias')
    parser.add_argument('--protocolo', type=str, help='Protocolo específico (ex: 00002.001588/2026-46)')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS_DEFAULT, help='Número de workers paralelos')
    parser.add_argument('--force', action='store_true', help='Re-varrer mesmo os que já têm NR')
    args = parser.parse_args()

    # Cria app context
    from app import create_app
    app = create_app()

    with app.app_context():
        # Gera token 1x (reutiliza em todas as threads)
        logger.info("Gerando token SEI...")
        token = get_token()
        logger.info("Token SEI obtido.")

        # Busca itinerários elegíveis
        logger.info("Buscando itinerários com NR no SEI...")
        itens = buscar_itinerarios(protocolo=args.protocolo, force=args.force)
        total = len(itens)

        if total == 0:
            logger.info("Nenhum itinerário para processar.")
            return

        logger.info(f"{total} itinerário(s) para processar com {args.workers} workers.")

        # Processamento paralelo
        t0 = time.time()
        sucessos = 0
        erros = 0
        nrs_extraidas = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(processar_nr, item, token, app): item
                for item in itens
            }

            for i, future in enumerate(as_completed(futures), 1):
                item = futures[future]
                try:
                    resultado = future.result()
                except Exception as e:
                    logger.error(f"  [{item[1]}] Exceção: {e}")
                    erros += 1
                    continue

                if resultado['sucesso']:
                    sucessos += 1
                    nrs_extraidas.append(resultado['nr_codigo'])
                    logger.info(
                        f"  [{i}/{total}] {resultado['protocolo']} → "
                        f"{resultado['nr_codigo']} ({resultado['doc_formatado']})"
                    )
                else:
                    erros += 1
                    logger.warning(
                        f"  [{i}/{total}] {resultado['protocolo']} → ERRO: {resultado['erro']}"
                    )

        elapsed = time.time() - t0

        # Relatório final
        logger.info("=" * 60)
        logger.info(f"RELATÓRIO FINAL")
        logger.info(f"  Total processados: {total}")
        logger.info(f"  NRs extraídas:     {sucessos}")
        logger.info(f"  Erros:             {erros}")
        logger.info(f"  Tempo:             {elapsed:.1f}s")
        if nrs_extraidas:
            logger.info(f"  NRs: {', '.join(nrs_extraidas)}")
        logger.info("=" * 60)


if __name__ == '__main__':
    main()
