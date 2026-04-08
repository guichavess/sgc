"""
Fase 1: Download de documentos SEI para analise pelo Claude Code.

Itera sobre processos de diarias, lista documentos via API SEI,
baixa cada PDF e salva em pasta organizada por processo.

Uso:
    # Baixar um processo especifico
    python scripts/baixar_documentos_diarias.py --processo 00002.003034/2026-83

    # Baixar todos (a partir do inicio)
    python scripts/baixar_documentos_diarias.py

    # Retomar a partir do processo N
    python scripts/baixar_documentos_diarias.py --start 50

    # Forcar re-download
    python scripts/baixar_documentos_diarias.py --force
"""

import sys
import os
import json
import time
import argparse
import base64
from datetime import datetime

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Ajustar path para importar modulos da app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.services.sei_auth import gerar_token_sei_admin

# ── Constantes ────────────────────────────────────────────────────────────
BASE_URL = "https://api.sei.pi.gov.br"
UNIDADE_SEAD = "110006213"

# Diretorio destino dos documentos
DEST_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'diarias')
)

# Mapeamento IdSerie → nome legivel para nomeacao de arquivos
SERIE_NOMES = {
    '532': 'requisicao_diarias',
    '2975': 'requisicao_passagens',
    '272': 'cotacao',
    '723': 'quadro_orcamentario',
    '425': 'nota_reserva',
    '419': 'nota_empenho',
    '420': 'nl',
    '421': 'pd',
    '422': 'ob',
    '423': 'np',
    '574': 'autorizacao_secretario',
    '269': 'autorizacao_generica',
    '2986': 'memorando_sga',
    '534': 'memorando_sead',
    '1907': 'memorando_diaria',
    '12': 'memorando_generico',
    '2977': 'escolha_passagens',
    '543': 'requisicao_interna',
    '2987': 'despacho_sga',
    '461': 'analise_pagamento',
    '5': 'despacho_nci',
    '754': 'despacho',
    '1908': 'relatorio_viagem',
    '261': 'relatorio_viagem_alt',
    '264': 'documento_externo',
    '35': 'comprovante',
    '127': 'convite',
    '263': 'anexo',
    '7': 'analise_habilitacao',
}

# Lista de processos (inline, conforme fornecido pelo usuario)
PROCESSOS = [
    "00002.003034/2026-83", "00002.001950/2026-89", "00002.002831/2026-43",
    "00002.003023/2026-01", "00002.012970/2025-02", "00002.013017/2025-73",
    "00002.000065/2026-82", "00002.000129/2026-45", "00002.000140/2026-13",
    "00002.000084/2026-17", "00002.000088/2026-97", "00002.000091/2026-19",
    "00002.000655/2026-13", "00002.000657/2026-02", "00002.000845/2026-22",
    "00002.000853/2026-79", "00002.001330/2026-40", "00002.001350/2026-11",
    "00002.001352/2026-18", "00002.001353/2026-54", "00002.001502/2026-85",
    "00002.001657/2026-11", "00002.001683/2026-40", "00002.001691/2026-96",
    "00002.001693/2026-85", "00002.001697/2026-63", "00002.001790/2026-78",
    "00002.001863/2026-21", "00002.001879/2026-34", "00002.002092/2026-90",
    "00002.002270/2026-82", "00002.002391/2026-24", "00002.002539/2026-21",
    "00002.002637/2026-68", "00002.002657/2026-39", "00002.002714/2026-80",
    "00002.002715/2026-24", "00002.002795/2026-18", "00002.002838/2026-65",
    "00002.002844/2026-12", "00002.002846/2026-10", "00002.002853/2026-11",
    "00002.002855/2026-01", "00002.002858/2026-36", "00002.002860/2026-13",
    "00002.002861/2026-50", "00002.002865/2026-38", "00002.002967/2026-53",
    "00002.003013/2026-68", "00002.003036/2026-72", "00002.003092/2026-15",
    "00002.003101/2026-60", "00002.003258/2026-95", "00002.001488/2026-10",
    "00002.002792/2026-84", "00002.003166/2026-13", "00002.001618/2026-14",
    "00002.003003/2026-22", "00002.002724/2026-15", "00002.002800/2026-92",
    "00002.003041/2026-85", "00002.002964/2026-10", "00002.002711/2026-46",
    "00002.002842/2026-23", "00002.002725/2026-60", "00002.002620/2026-19",
    "00002.002708/2026-22", "00002.002717/2026-13", "00002.002731/2026-17",
    "00002.002719/2026-11", "00002.002702/2026-55", "00002.000914/2026-06",
    "00002.002118/2026-08", "00002.002427/2026-70", "00002.002433/2026-27",
    "00002.002475/2026-68", "00002.002509/2026-14", "00002.002836/2026-76",
    "00002.002031/2026-22", "00002.002437/2026-13", "00002.002489/2026-81",
    "00002.001759/2026-37", "00002.001788/2026-07", "00002.001793/2026-10",
    "00002.002569/2026-37", "00002.000189/2026-68", "00002.001627/2026-13",
    "00002.001645/2026-97", "00002.001869/2026-07", "00002.001091/2026-28",
    "00002.001775/2026-20", "00002.001588/2026-46", "00002.001901/2026-46",
    "00002.001476/2026-95", "00002.001653/2026-33", "00002.001710/2026-84",
    "00002.001781/2026-87", "00002.001783/2026-76", "00002.002990/2026-48",
    "00002.001021/2026-70", "00002.001370/2026-91", "00002.001113/2026-50",
    "00002.001037/2026-82", "00002.001188/2026-31", "00002.001280/2026-09",
    "00002.000461/2026-18", "00002.000727/2026-14", "00002.000747/2026-95",
    "00002.001240/2026-59", "00002.001134/2026-75", "00002.001074/2026-91",
    "00002.001169/2026-12", "00002.000837/2026-86", "00002.000786/2026-92",
    "00002.000610/2026-31", "00002.000633/2026-45", "00002.000665/2026-41",
    "00002.000634/2026-90", "00002.000751/2026-53", "00002.000663/2026-51",
    "00002.000596/2026-75", "00002.000325/2026-10", "00002.000303/2026-50",
    "00002.000141/2026-50", "00002.000304/2026-02", "00002.012771/2025-96",
    "00002.009305/2025-23", "00002.001571/2026-99",
]


# ═══════════════════════════════════════════════════════════════════════
# FUNCOES API SEI (reutilizadas de importar_diarias_sei.py)
# ═══════════════════════════════════════════════════════════════════════

def listar_documentos(token, protocolo_processo):
    """Lista todos os documentos de um processo SEI."""
    protocolo_limpo = "".join(filter(str.isdigit, protocolo_processo))
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos"
    params = {'protocolo_procedimento': protocolo_limpo, 'quantidade': 1000}
    headers = {'token': token, 'Accept': 'application/json'}

    resp = requests.get(url, params=params, headers=headers, timeout=180, verify=False)
    if resp.status_code != 200:
        print(f"  ERRO ao listar documentos ({resp.status_code}): {resp.text[:200]}")
        return []

    data = resp.json()
    if isinstance(data, dict) and 'Documentos' in data:
        return data['Documentos']
    elif isinstance(data, list):
        return data
    return [data] if data else []


def baixar_documento_pdf(token, id_documento):
    """Baixa o PDF de um documento SEI."""
    url = f"{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/documentos/baixar"
    params = {'protocolo_documento': str(id_documento)}
    headers = {'token': token, 'Accept': 'application/pdf'}

    resp = requests.get(url, params=params, headers=headers, timeout=300, verify=False)
    if resp.status_code != 200:
        print(f"    ERRO download {id_documento} ({resp.status_code})")
        return None

    content_type = resp.headers.get('Content-Type', '')

    # Se retorna JSON com base64
    if 'json' in content_type.lower():
        try:
            data = resp.json()
            for campo in ('Conteudo', 'conteudo', 'Content', 'content', 'Arquivo', 'arquivo'):
                if campo in data:
                    return base64.b64decode(data[campo])
            print(f"    AVISO: JSON sem campo de conteudo. Chaves: {list(data.keys())}")
            return None
        except Exception as e:
            print(f"    ERRO base64: {e}")
            return None

    return resp.content


# ═══════════════════════════════════════════════════════════════════════
# LOGICA PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════

def nome_pasta(protocolo):
    """Converte protocolo em nome de pasta seguro (troca / por _)."""
    return protocolo.replace('/', '_')


def detectar_extensao(content_bytes):
    """Detecta extensao real do conteudo baixado."""
    if not content_bytes or len(content_bytes) < 10:
        return '.bin'
    header = content_bytes[:20]
    if header[:5] == b'%PDF-':
        return '.pdf'
    if header[:9] == b'<!DOCTYPE' or header[:5] == b'<html' or header[:1] == b'<':
        return '.html'
    if header[:3] == b'\xff\xd8\xff':
        return '.jpg'
    if header[:8] == b'\x89PNG\r\n\x1a\n':
        return '.png'
    return '.pdf'  # fallback


def nome_arquivo(doc, extensao='.pdf'):
    """Gera nome de arquivo a partir de metadados do documento SEI."""
    id_serie = doc.get('Serie', {}).get('IdSerie', 'desconhecido')
    tipo = SERIE_NOMES.get(str(id_serie), f'serie_{id_serie}')
    doc_formatado = doc.get('DocumentoFormatado', doc.get('IdDocumento', 'sem_id'))
    # Limpar caracteres invalidos
    doc_formatado = doc_formatado.replace('/', '_').replace('\\', '_')
    return f"{id_serie}_{tipo}_{doc_formatado}{extensao}"


def processar_processo(token, protocolo, dest_base, force=False):
    """Baixa todos os documentos de um processo SEI."""
    pasta = os.path.join(dest_base, nome_pasta(protocolo))
    metadata_path = os.path.join(pasta, '_metadata.json')

    # Listar documentos
    print(f"\n{'='*60}")
    print(f"Processo: {protocolo}")
    documentos = listar_documentos(token, protocolo)

    if not documentos:
        print("  Nenhum documento encontrado.")
        return {'protocolo': protocolo, 'status': 'sem_documentos', 'docs': 0}

    print(f"  {len(documentos)} documento(s) encontrado(s)")

    # Verificar se ja foi baixado (incremental)
    if not force and os.path.exists(metadata_path):
        with open(metadata_path, 'r', encoding='utf-8') as f:
            meta_existente = json.load(f)
        if meta_existente.get('total_documentos') == len(documentos):
            print(f"  SKIP: ja baixado ({len(documentos)} docs)")
            return {'protocolo': protocolo, 'status': 'skip', 'docs': len(documentos)}

    # Criar pasta
    os.makedirs(pasta, exist_ok=True)

    # Salvar metadata
    metadata = {
        'protocolo': protocolo,
        'total_documentos': len(documentos),
        'download_timestamp': datetime.now().isoformat(),
        'documentos': [],
    }

    baixados = 0
    erros = 0

    for doc in documentos:
        id_doc = doc.get('IdDocumento') or doc.get('ProtocoloDocumento')
        id_serie = doc.get('Serie', {}).get('IdSerie', '?')
        serie_nome = doc.get('Serie', {}).get('Nome', '?')
        doc_fmt = doc.get('DocumentoFormatado', '?')

        tipo_legivel = SERIE_NOMES.get(str(id_serie), f'serie_{id_serie}')

        # Verificar se ja existe (com qualquer extensao)
        base_nome = f"{id_serie}_{tipo_legivel}_{doc_fmt.replace('/', '_').replace(chr(92), '_')}"
        arquivos_existentes = [
            f for f in os.listdir(pasta)
            if f.startswith(base_nome) and not f.startswith('_')
        ] if os.path.exists(pasta) else []

        if not force and arquivos_existentes:
            arquivo = arquivos_existentes[0]
            caminho = os.path.join(pasta, arquivo)
            doc_meta = {
                'id_documento': id_doc,
                'id_serie': id_serie,
                'serie_nome': serie_nome,
                'documento_formatado': doc_fmt,
                'tipo': tipo_legivel,
                'arquivo': arquivo,
                'baixado': True,
                'tamanho': os.path.getsize(caminho),
            }
            metadata['documentos'].append(doc_meta)
            baixados += 1
            print(f"    [{id_serie}] {tipo_legivel} ({doc_fmt}) - ja existe")
            continue

        print(f"    [{id_serie}] {tipo_legivel} ({doc_fmt}) - baixando...", end=' ')

        # API SEI usa DocumentoFormatado como protocolo_documento (nao IdDocumento)
        content_bytes = baixar_documento_pdf(token, doc_fmt)
        if content_bytes and len(content_bytes) > 100:
            extensao = detectar_extensao(content_bytes)
            arquivo = nome_arquivo(doc, extensao)
            caminho = os.path.join(pasta, arquivo)

            # Remover arquivo antigo com extensao errada (se existir do --force)
            for old in arquivos_existentes:
                old_path = os.path.join(pasta, old)
                if old_path != caminho and os.path.exists(old_path):
                    os.remove(old_path)

            with open(caminho, 'wb') as f:
                f.write(content_bytes)

            doc_meta = {
                'id_documento': id_doc,
                'id_serie': id_serie,
                'serie_nome': serie_nome,
                'documento_formatado': doc_fmt,
                'tipo': tipo_legivel,
                'arquivo': arquivo,
                'baixado': True,
                'tamanho': len(content_bytes),
                'formato': extensao.replace('.', ''),
            }
            metadata['documentos'].append(doc_meta)
            baixados += 1
            print(f"OK ({len(content_bytes)} bytes, {extensao})")
        else:
            doc_meta = {
                'id_documento': id_doc,
                'id_serie': id_serie,
                'serie_nome': serie_nome,
                'documento_formatado': doc_fmt,
                'tipo': tipo_legivel,
                'arquivo': None,
                'baixado': False,
            }
            metadata['documentos'].append(doc_meta)
            erros += 1
            print("FALHA")

        time.sleep(0.5)  # Rate limiting leve (paralelismo entre workers ja espaca)

    metadata['baixados'] = baixados
    metadata['erros'] = erros

    # Salvar metadata atualizado
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print(f"  Resultado: {baixados}/{len(documentos)} baixados, {erros} erros")
    return {
        'protocolo': protocolo,
        'status': 'ok' if erros == 0 else 'parcial',
        'docs': len(documentos),
        'baixados': baixados,
        'erros': erros,
    }


def _worker(args_tuple):
    """Worker para ThreadPoolExecutor. Processa um processo SEI."""
    protocolo, dest_base, force, token = args_tuple

    try:
        return processar_processo(token, protocolo, dest_base, force=force)
    except Exception as e:
        return {'protocolo': protocolo, 'status': 'erro', 'erro': str(e)}


def main():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    parser = argparse.ArgumentParser(description='Baixar documentos SEI de processos de diarias')
    parser.add_argument('--processo', help='Processar apenas este protocolo')
    parser.add_argument('--start', type=int, default=0, help='Iniciar a partir do processo N (0-based)')
    parser.add_argument('--force', action='store_true', help='Re-baixar mesmo se ja existe')
    parser.add_argument('--workers', type=int, default=6, help='Numero de workers paralelos (default: 6)')
    args = parser.parse_args()

    print(f"Destino: {DEST_DIR}")
    os.makedirs(DEST_DIR, exist_ok=True)

    app = create_app()
    with app.app_context():
        # Definir lista de processos
        if args.processo:
            lista = [args.processo]
        else:
            lista = PROCESSOS[args.start:]

        workers = min(args.workers, len(lista))
        print(f"Total de processos: {len(lista)}")
        print(f"Workers paralelos: {workers}")

        # Gerar token no contexto da app (compartilhado entre workers)
        # Retry com backoff para lidar com timeouts da API SEI
        token = None
        for tentativa in range(1, 4):
            print(f"Autenticando no SEI (tentativa {tentativa}/3)...")
            token = gerar_token_sei_admin()
            if token:
                break
            print(f"  Falha. Aguardando {tentativa * 10}s...")
            time.sleep(tentativa * 10)
        if not token:
            print("ERRO: Falha na autenticacao SEI apos 3 tentativas")
            sys.exit(1)
        print("Token obtido com sucesso.\n")

        # Preparar argumentos para cada worker (token compartilhado)
        tarefas = [
            (protocolo, DEST_DIR, args.force, token)
            for protocolo in lista
        ]

        resultados = []
        inicio = time.time()
        concluidos = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_worker, tarefa): tarefa[0]
                for tarefa in tarefas
            }

            for future in as_completed(futures):
                protocolo = futures[future]
                concluidos += 1
                try:
                    resultado = future.result()
                    resultados.append(resultado)
                    status = resultado.get('status', '?')
                    docs = resultado.get('docs', 0)
                    baixados = resultado.get('baixados', 0)
                    elapsed = time.time() - inicio
                    print(f"  [{concluidos}/{len(lista)}] {protocolo} -> {status} "
                          f"({baixados}/{docs} docs) [{elapsed:.0f}s]")
                except Exception as e:
                    print(f"  [{concluidos}/{len(lista)}] {protocolo} -> ERRO: {e}")
                    resultados.append({'protocolo': protocolo, 'status': 'erro', 'erro': str(e)})

        duracao = time.time() - inicio

        # Salvar resumo
        resumo = {
            'timestamp': datetime.now().isoformat(),
            'duracao_segundos': round(duracao, 1),
            'total_processos': len(lista),
            'workers': workers,
            'sucesso': sum(1 for r in resultados if r.get('status') in ('ok', 'skip')),
            'parcial': sum(1 for r in resultados if r.get('status') == 'parcial'),
            'erros': sum(1 for r in resultados if r.get('status') == 'erro'),
            'sem_documentos': sum(1 for r in resultados if r.get('status') == 'sem_documentos'),
            'resultados': resultados,
        }

        resumo_path = os.path.join(DEST_DIR, '_download_summary.json')
        with open(resumo_path, 'w', encoding='utf-8') as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)

        print(f"\n{'='*60}")
        print(f"CONCLUIDO em {duracao:.0f}s ({workers} workers)")
        print(f"  Sucesso: {resumo['sucesso']}")
        print(f"  Parcial: {resumo['parcial']}")
        print(f"  Erros:   {resumo['erros']}")
        print(f"  Sem docs: {resumo['sem_documentos']}")
        print(f"  Resumo salvo em: {resumo_path}")


if __name__ == '__main__':
    main()
