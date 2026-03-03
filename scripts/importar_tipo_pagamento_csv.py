"""
Script para gerar SQL de importação de tipo_pagamento a partir do CSV de contratos.
Lê o CSV e gera UPDATE para sis_solicitacoes.id_tipo_pagamento baseado no codigo_contrato.

Uso:
    python scripts/importar_tipo_pagamento_csv.py
    python scripts/importar_tipo_pagamento_csv.py --executar
"""
import csv
import sys
import os
import argparse

# Adiciona o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CSV_PATH = r'C:\Users\guilh\OneDrive\Área de Trabalho\contratos tipo pag.csv'


def gerar_sql():
    """Lê o CSV e gera os UPDATEs."""
    mapa = {}  # codigo_contrato → tipo_execucao_id

    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';', quotechar='"')
        for row in reader:
            codigo = row.get('codigo', '').strip()
            tipo = row.get('tipo_execucao_id', '').strip()
            if codigo and tipo and tipo.isdigit():
                mapa[codigo] = int(tipo)

    print(f"-- CSV lido: {len(mapa)} contratos com tipo_execucao_id definido")
    print(f"-- Tipo 1 (Pagamento Regular): {sum(1 for v in mapa.values() if v == 1)}")
    print(f"-- Tipo 2 (DEA: Indenizatório): {sum(1 for v in mapa.values() if v == 2)}")
    print(f"-- Tipo 3 (DEA: Pagamento Regular): {sum(1 for v in mapa.values() if v == 3)}")
    print()

    # Agrupa por tipo para gerar UPDATEs eficientes (IN clause)
    por_tipo = {}
    for codigo, tipo in mapa.items():
        por_tipo.setdefault(tipo, []).append(codigo)

    for tipo_id, codigos in sorted(por_tipo.items()):
        lista = ", ".join(f"'{c}'" for c in sorted(codigos))
        print(f"-- Tipo {tipo_id}: {len(codigos)} contratos")
        print(f"UPDATE sis_solicitacoes")
        print(f"SET id_tipo_pagamento = {tipo_id}")
        print(f"WHERE codigo_contrato IN ({lista})")
        print(f"AND (id_tipo_pagamento IS NULL OR id_tipo_pagamento != {tipo_id});")
        print()

    return mapa, por_tipo


def executar_no_banco(mapa, por_tipo):
    """Executa os UPDATEs diretamente no banco."""
    from app import create_app
    from app.extensions import db

    app = create_app()
    with app.app_context():
        total_atualizado = 0
        for tipo_id, codigos in sorted(por_tipo.items()):
            result = db.session.execute(
                db.text(
                    "UPDATE sis_solicitacoes "
                    "SET id_tipo_pagamento = :tipo "
                    "WHERE codigo_contrato IN :codigos "
                    "AND (id_tipo_pagamento IS NULL OR id_tipo_pagamento != :tipo)"
                ),
                {"tipo": tipo_id, "codigos": tuple(codigos)}
            )
            count = result.rowcount
            total_atualizado += count
            print(f"[OK] Tipo {tipo_id}: {count} solicitações atualizadas ({len(codigos)} contratos)")

        db.session.commit()
        print(f"\n=== TOTAL: {total_atualizado} solicitações atualizadas ===")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--executar', action='store_true', help='Executa no banco (sem flag = DRY-RUN/gera SQL)')
    args = parser.parse_args()

    mapa, por_tipo = gerar_sql()

    if args.executar:
        print("\n=== EXECUTANDO NO BANCO ===\n")
        executar_no_banco(mapa, por_tipo)
    else:
        print("-- DRY-RUN: copie o SQL acima para executar em produção")
        print("-- Ou rode: python scripts/importar_tipo_pagamento_csv.py --executar")
