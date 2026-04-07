"""
Script para sincronizar documentos SEI dos processos de diárias.
Busca documentos via API SEI e salva em diarias_movimentacao.

Uso:
    python scripts/sync_diarias_movimentacao.py                # Sync todos com processo SEI
    python scripts/sync_diarias_movimentacao.py --protocolo 00002.001588/2026-46  # Um específico
    python scripts/sync_diarias_movimentacao.py --workers 4    # Controlar paralelismo
"""
import os
import sys
import time
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Constantes
SEI_BASE_URL = "https://api.sei.pi.gov.br/v1/unidades/110006213/procedimentos/documentos"
SEI_TIMEOUT = 60
MAX_WORKERS = 6


def get_token():
    """Gera token SEI admin."""
    from app.services.sei_auth import gerar_token_sei_admin
    token = gerar_token_sei_admin()
    if not token:
        raise RuntimeError("Falha ao gerar token SEI")
    return token


def fetch_docs_sei(protocolo, token):
    """Busca documentos de um processo na API SEI. Retorna lista de docs ou None."""
    headers = {'token': token, 'Accept': 'application/json'}
    params = {
        "protocolo_procedimento": protocolo,
        "pagina": 1, "quantidade": 1000,
    }

    for tentativa in range(1, 4):
        try:
            resp = requests.get(SEI_BASE_URL, headers=headers, params=params,
                                timeout=SEI_TIMEOUT, verify=False)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    docs = data.get('Documentos', [])
                    if not docs and 'resultados' in data:
                        docs = data['resultados']
                    return docs
                elif isinstance(data, list):
                    return data
                return []
            else:
                logger.warning(f"  [{protocolo}] API retornou {resp.status_code} (tentativa {tentativa})")
        except requests.exceptions.ReadTimeout:
            logger.warning(f"  [{protocolo}] Timeout (tentativa {tentativa}/3)")
        except Exception as e:
            logger.warning(f"  [{protocolo}] Erro: {e} (tentativa {tentativa}/3)")

        if tentativa < 3:
            time.sleep(tentativa * 5)

    return None


def save_docs(protocolo, docs, db, DiariasMovimentacao):
    """Salva documentos no banco, substituindo os antigos do processo."""
    DiariasMovimentacao.query.filter_by(protocolo_procedimento=protocolo).delete()

    count = 0
    for doc in docs:
        serie = doc.get('Serie', {})
        unidade = doc.get('UnidadeElaboradora', {})
        id_serie_val = None
        if serie.get('IdSerie') and str(serie.get('IdSerie')).isdigit():
            id_serie_val = int(serie['IdSerie'])

        mov = DiariasMovimentacao(
            id_documento=str(doc.get('IdDocumento', '')),
            protocolo_procedimento=protocolo,
            id_procedimento=str(doc.get('IdProcedimento', '')),
            procedimento_formatado=str(doc.get('ProcedimentoFormatado', '')),
            documento_formatado=str(doc.get('DocumentoFormatado', '')),
            link_acesso=doc.get('LinkAcesso'),
            descricao=doc.get('Descricao'),
            data=doc.get('Data') or doc.get('DataGeracao'),
            numero=doc.get('Numero'),
            id_serie=id_serie_val,
            serie_nome=serie.get('Nome'),
            serie_aplicabilidade=serie.get('Aplicabilidade'),
            unidade_id=str(unidade.get('IdUnidade', '')),
            unidade_sigla=unidade.get('Sigla'),
            unidade_descricao=unidade.get('Descricao'),
            obs='',
        )
        db.session.add(mov)
        count += 1

    db.session.commit()
    return count


def sync_one(protocolo, token, app):
    """Sincroniza um processo. Retorna (success, protocolo, count, elapsed)."""
    t0 = time.time()
    docs = fetch_docs_sei(protocolo, token)
    elapsed = time.time() - t0

    if docs is None:
        return (False, protocolo, 0, elapsed)

    with app.app_context():
        from app.extensions import db
        from app.models.diaria import DiariasMovimentacao
        try:
            count = save_docs(protocolo, docs, db, DiariasMovimentacao)
        except Exception as e:
            db.session.rollback()
            logger.error(f"  [{protocolo}] Erro DB: {e}")
            return (False, protocolo, 0, elapsed)
        finally:
            db.session.remove()

    return (True, protocolo, count, elapsed)


def main():
    parser = argparse.ArgumentParser(description='Sync documentos SEI para diarias_movimentacao')
    parser.add_argument('--protocolo', type=str, help='Sincronizar um processo específico')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help=f'Threads paralelas (default: {MAX_WORKERS})')
    args = parser.parse_args()

    from app import create_app
    app = create_app()

    with app.app_context():
        from app.extensions import db
        from app.models.diaria import DiariasItinerario

        logger.info("Gerando token SEI...")
        token = get_token()
        logger.info("Token obtido.")

        if args.protocolo:
            protocolos = [args.protocolo]
            logger.info(f"Sincronizando 1 processo: {args.protocolo}")
        else:
            # Todos os itinerários que têm processo SEI
            protocolos = [
                it.n_processo for it in
                DiariasItinerario.query.filter(
                    DiariasItinerario.n_processo.isnot(None),
                    DiariasItinerario.n_processo != '',
                ).all()
            ]
            logger.info(f"Sincronizando {len(protocolos)} processos de diárias")

        if not protocolos:
            logger.info("Nenhum processo para sincronizar.")
            return

    # Executar em paralelo
    total = len(protocolos)
    ok = 0
    erros = 0
    total_docs = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(sync_one, p, token, app): p for p in protocolos}

        for future in as_completed(futures):
            success, proto, count, elapsed = future.result()
            if success:
                ok += 1
                total_docs += count
                logger.info(f"  OK: {proto} ({count} docs, {elapsed:.1f}s) [{ok + erros}/{total}]")
            else:
                erros += 1
                logger.warning(f"  ERRO: {proto} ({elapsed:.1f}s) [{ok + erros}/{total}]")

    elapsed_total = time.time() - t_start
    logger.info(f"\n{'='*60}")
    logger.info(f"RESULTADO: {ok}/{total} sincronizados, {erros} erros")
    logger.info(f"Total de documentos: {total_docs}")
    logger.info(f"Tempo total: {elapsed_total:.1f}s")
    logger.info(f"{'='*60}")


if __name__ == '__main__':
    main()
