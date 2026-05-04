"""
Auxiliar Fase 2: Extrai texto limpo dos documentos HTML/PDF de um processo.

Usado pelo Claude Code para analisar os documentos sem precisar ler HTMLs brutos.

Uso:
    # Extrair texto de um processo
    python scripts/extrair_texto_diarias.py 00002.003034_2026-83

    # Extrair de todos os pendentes (sem _extracted.json)
    python scripts/extrair_texto_diarias.py --pendentes --limit 10
"""

import sys
import os
import json
import argparse

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

DEST_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'diarias')
)

# Documentos prioritarios para analise (ordem de importancia)
DOCS_PRIORITARIOS = [
    '532',   # Requisicao de Diarias (dados principais)
    '2975',  # Requisicao de Passagens
    '723',   # Quadro Orcamentario
    '425',   # Nota de Reserva
    '2986',  # Memorando SGA
    '534',   # Memorando SEAD
    '12',    # Memorando generico
    '1907',  # Memorando diaria
    '574',   # Autorizacao Secretario
    '269',   # Autorizacao generica
    '461',   # Analise NCI
    '419',   # Nota Empenho
    '420',   # NL
    '421',   # PD
    '422',   # OB
    '423',   # NP
    '1908',  # Relatorio viagem
    '261',   # Relatorio viagem alt
    '2977',  # Escolha passagens
    '543',   # Requisicao interna
    '5',     # Despacho NCI
    '2987',  # Despacho SGA
    '7',     # Analise habilitacao
]


def extrair_texto_html(caminho):
    """Extrai texto limpo de um arquivo HTML."""
    if not BeautifulSoup:
        with open(caminho, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()[:5000]

    with open(caminho, 'r', encoding='utf-8', errors='replace') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    for tag in soup(['style', 'script', 'head']):
        tag.decompose()

    text = soup.get_text(separator='\n', strip=True)

    # Limpar linhas vazias excessivas
    lines = [l for l in text.split('\n') if l.strip()]
    return '\n'.join(lines)


def extrair_texto_pdf_info(caminho):
    """Para PDFs retorna info basica (tamanho, tipo)."""
    size = os.path.getsize(caminho)
    return f"[PDF: {size} bytes - requer leitura via Read tool]"


def processar_pasta(pasta_path, protocolo):
    """Extrai texto de todos os documentos prioritarios de um processo."""
    metadata_path = os.path.join(pasta_path, '_metadata.json')
    if not os.path.exists(metadata_path):
        return None

    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    print(f"{'='*70}")
    print(f"PROCESSO: {protocolo}")
    print(f"Total documentos: {metadata.get('total_documentos', '?')}")
    print(f"{'='*70}")

    docs = metadata.get('documentos', [])

    # Ordenar por prioridade
    def prioridade(doc):
        serie = str(doc.get('id_serie', ''))
        try:
            return DOCS_PRIORITARIOS.index(serie)
        except ValueError:
            return 999

    docs_ordenados = sorted(docs, key=prioridade)

    for doc in docs_ordenados:
        arquivo = doc.get('arquivo')
        if not arquivo or not doc.get('baixado'):
            continue

        caminho = os.path.join(pasta_path, arquivo)
        if not os.path.exists(caminho):
            continue

        serie = doc.get('id_serie', '?')
        tipo = doc.get('tipo', '?')
        fmt = doc.get('documento_formatado', '?')

        print(f"\n{'─'*70}")
        print(f"[{serie}] {tipo} ({fmt})")
        print(f"Arquivo: {arquivo}")
        print(f"{'─'*70}")

        if arquivo.endswith('.html'):
            texto = extrair_texto_html(caminho)
            # Limitar a 3000 chars por documento para nao sobrecarregar
            if len(texto) > 3000:
                print(texto[:3000])
                print(f"\n... [TRUNCADO - {len(texto)} chars total]")
            else:
                print(texto)
        elif arquivo.endswith('.pdf'):
            print(extrair_texto_pdf_info(caminho))
        elif arquivo.endswith(('.jpg', '.png')):
            print(f"[IMAGEM: {os.path.getsize(caminho)} bytes]")
        else:
            print(f"[ARQUIVO: {os.path.getsize(caminho)} bytes]")

    print(f"\n{'='*70}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Extrair texto dos documentos de diarias')
    parser.add_argument('pasta', nargs='?', help='Nome da pasta do processo (ex: 00002.003034_2026-83)')
    parser.add_argument('--pendentes', action='store_true', help='Processar apenas pendentes')
    parser.add_argument('--limit', type=int, default=1, help='Limite de processos')
    args = parser.parse_args()

    if args.pasta:
        pasta_path = os.path.join(DEST_DIR, args.pasta)
        protocolo = args.pasta.replace('_', '/')
        if not os.path.isdir(pasta_path):
            print(f"Pasta nao encontrada: {pasta_path}")
            return
        processar_pasta(pasta_path, protocolo)
    elif args.pendentes:
        count = 0
        for nome in sorted(os.listdir(DEST_DIR)):
            if nome.startswith('_') or not os.path.isdir(os.path.join(DEST_DIR, nome)):
                continue
            extracted = os.path.join(DEST_DIR, nome, '_extracted.json')
            if os.path.exists(extracted):
                continue
            processar_pasta(os.path.join(DEST_DIR, nome), nome.replace('_', '/'))
            count += 1
            if count >= args.limit:
                break
        if count == 0:
            print("Nenhum processo pendente.")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
