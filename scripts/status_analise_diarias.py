"""
Status da analise de documentos de diarias.

Lista processos pendentes/concluidos (quem tem _extracted.json vs nao).
Usado para coordenar as sessoes de analise com o Claude Code.

Uso:
    # Ver status geral
    python scripts/status_analise_diarias.py

    # Listar apenas pendentes (proximo lote)
    python scripts/status_analise_diarias.py --pendentes

    # Listar pendentes com limite
    python scripts/status_analise_diarias.py --pendentes --limit 10

    # Validar JSONs extraidos
    python scripts/status_analise_diarias.py --validar
"""

import sys
import os
import json
import argparse

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Diretorio dos documentos
DEST_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'diarias')
)

# Campos obrigatorios no _extracted.json
CAMPOS_OBRIGATORIOS = {
    'processo', 'confidence', 'itinerario',
}
CAMPOS_ITINERARIO = {
    'tipo_solicitacao_id', 'tipo_itinerario', 'data_viagem', 'data_retorno',
}


def listar_pastas_processos():
    """Retorna lista de pastas de processos (que tem _metadata.json)."""
    pastas = []
    if not os.path.isdir(DEST_DIR):
        return pastas

    for nome in sorted(os.listdir(DEST_DIR)):
        caminho = os.path.join(DEST_DIR, nome)
        if not os.path.isdir(caminho):
            continue
        metadata = os.path.join(caminho, '_metadata.json')
        if os.path.exists(metadata):
            pastas.append({
                'nome': nome,
                'caminho': caminho,
                'protocolo': nome.replace('_', '/'),
                'tem_extracted': os.path.exists(os.path.join(caminho, '_extracted.json')),
                'metadata_path': metadata,
                'extracted_path': os.path.join(caminho, '_extracted.json'),
            })
    return pastas


def validar_extracted(caminho_json):
    """Valida schema do _extracted.json. Retorna lista de erros."""
    erros = []
    try:
        with open(caminho_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        return [f'Erro ao ler JSON: {e}']

    for campo in CAMPOS_OBRIGATORIOS:
        if campo not in data:
            erros.append(f'Campo obrigatorio ausente: {campo}')

    itinerario = data.get('itinerario', {})
    for campo in CAMPOS_ITINERARIO:
        if campo not in itinerario:
            erros.append(f'Campo itinerario.{campo} ausente')

    if data.get('confidence') not in ('high', 'medium', 'low'):
        erros.append(f"confidence invalido: {data.get('confidence')}")

    return erros


def main():
    parser = argparse.ArgumentParser(description='Status da analise de diarias')
    parser.add_argument('--pendentes', action='store_true', help='Listar apenas pendentes')
    parser.add_argument('--concluidos', action='store_true', help='Listar apenas concluidos')
    parser.add_argument('--limit', type=int, default=0, help='Limitar quantidade listada')
    parser.add_argument('--validar', action='store_true', help='Validar JSONs extraidos')
    args = parser.parse_args()

    pastas = listar_pastas_processos()

    if not pastas:
        print(f"Nenhuma pasta de processo encontrada em: {DEST_DIR}")
        print("Execute primeiro: python scripts/baixar_documentos_diarias.py")
        return

    total = len(pastas)
    concluidos = [p for p in pastas if p['tem_extracted']]
    pendentes = [p for p in pastas if not p['tem_extracted']]

    print(f"{'='*60}")
    print(f"STATUS DA ANALISE DE DIARIAS")
    print(f"{'='*60}")
    print(f"  Diretorio: {DEST_DIR}")
    print(f"  Total processos baixados: {total}")
    print(f"  Analisados (com _extracted.json): {len(concluidos)}")
    print(f"  Pendentes: {len(pendentes)}")
    print(f"  Progresso: {len(concluidos)}/{total} ({100*len(concluidos)/total:.0f}%)")
    print()

    # Validacao
    if args.validar:
        print("VALIDACAO DOS JSONs EXTRAIDOS:")
        print("-" * 40)
        total_erros = 0
        for p in concluidos:
            erros = validar_extracted(p['extracted_path'])
            if erros:
                total_erros += 1
                print(f"  {p['protocolo']} - {len(erros)} erro(s):")
                for e in erros:
                    print(f"    - {e}")
            else:
                print(f"  {p['protocolo']} - OK")
        print(f"\n{len(concluidos) - total_erros}/{len(concluidos)} JSONs validos")
        return

    # Listar
    if args.pendentes:
        lista = pendentes
        titulo = "PENDENTES"
    elif args.concluidos:
        lista = concluidos
        titulo = "CONCLUIDOS"
    else:
        # Mostrar ambos
        if pendentes:
            print(f"PENDENTES ({len(pendentes)}):")
            for p in pendentes[:10]:
                print(f"  - {p['protocolo']}")
            if len(pendentes) > 10:
                print(f"  ... e mais {len(pendentes) - 10}")
        if concluidos:
            print(f"\nCONCLUIDOS ({len(concluidos)}):")
            for p in concluidos[:5]:
                print(f"  - {p['protocolo']}")
            if len(concluidos) > 5:
                print(f"  ... e mais {len(concluidos) - 5}")
        return

    if args.limit > 0:
        lista = lista[:args.limit]

    print(f"{titulo} ({len(lista)}):")
    for p in lista:
        # Carregar metadata para mostrar qtd docs
        try:
            with open(p['metadata_path'], 'r', encoding='utf-8') as f:
                meta = json.load(f)
            n_docs = meta.get('total_documentos', '?')
        except Exception:
            n_docs = '?'
        print(f"  {p['protocolo']}  ({n_docs} docs)")


if __name__ == '__main__':
    main()
