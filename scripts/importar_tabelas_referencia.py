"""
Importa as tabelas de referência (estados, municipios, orgao, setor) do dump
do banco 'solicitacoes' para o banco 'sgc'.

Uso:
    python scripts/importar_tabelas_referencia.py CAMINHO_DO_DUMP.sql
    python scripts/importar_tabelas_referencia.py CAMINHO_DO_DUMP.sql --dry-run

Exemplo:
    python scripts/importar_tabelas_referencia.py "C:\\Users\\guilh\\OneDrive\\Documentos\\dumps\\Dump20260219 (1).sql"
"""
import argparse
import re
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db

# Tabelas a importar, na ordem correta (respeita FKs)
TABELAS = ['estados', 'municipios', 'orgao', 'setor']


def extrair_sql_tabela(dump_lines, tabela):
    """
    Extrai CREATE TABLE e INSERT INTO de uma tabela específica do dump.

    Returns:
        tuple: (create_sql, insert_statements)
    """
    create_sql = None
    inserts = []

    i = 0
    while i < len(dump_lines):
        line = dump_lines[i]

        # Captura CREATE TABLE completo
        if f'CREATE TABLE `{tabela}`' in line:
            create_lines = []
            while i < len(dump_lines):
                create_lines.append(dump_lines[i])
                if dump_lines[i].strip().startswith(')'):
                    # Pega a linha de fechamento completa (com ENGINE=...)
                    break
                i += 1
            create_sql = ''.join(create_lines)

        # Captura INSERT INTO
        if line.startswith(f'INSERT INTO `{tabela}`'):
            inserts.append(line.strip())

        i += 1

    return create_sql, inserts


def importar_tabela(conn, tabela, create_sql, inserts, dry_run=False):
    """Importa uma tabela: cria se não existir, insere dados."""
    print(f"\n{'='*60}")
    print(f"  Tabela: {tabela}")
    print(f"{'='*60}")

    # Verifica se já existe
    exists = db.engine.dialect.has_table(conn, tabela)

    if exists:
        # Verifica se tem dados
        result = conn.execute(db.text(f"SELECT COUNT(*) FROM `{tabela}`"))
        count = result.scalar()
        if count > 0:
            print(f"  ✓ Tabela já existe com {count} registros. Pulando.")
            return
        else:
            print(f"  ⚠ Tabela existe mas está vazia. Inserindo dados...")
    else:
        if not create_sql:
            print(f"  ✗ CREATE TABLE não encontrado no dump!")
            return
        print(f"  Criando tabela...")
        if not dry_run:
            # Remove DROP TABLE IF EXISTS se houver
            conn.execute(db.text(f"DROP TABLE IF EXISTS `{tabela}`"))
            conn.execute(db.text(create_sql))
        print(f"  ✓ Tabela criada.")

    # Insere dados
    if not inserts:
        print(f"  ⚠ Nenhum INSERT encontrado no dump.")
        return

    for insert_sql in inserts:
        # Conta registros estimando pelo número de parênteses
        count_est = insert_sql.count('),(') + 1
        print(f"  Inserindo ~{count_est} registros...")
        if not dry_run:
            conn.execute(db.text(insert_sql))

    print(f"  ✓ Dados inseridos com sucesso.")


def main():
    parser = argparse.ArgumentParser(
        description='Importa tabelas de referência do dump do banco solicitacoes'
    )
    parser.add_argument('dump_file', help='Caminho do arquivo .sql de dump')
    parser.add_argument('--dry-run', action='store_true',
                        help='Apenas mostra o que seria feito, sem executar')
    args = parser.parse_args()

    if not os.path.exists(args.dump_file):
        print(f"Erro: arquivo não encontrado: {args.dump_file}")
        sys.exit(1)

    # Lê o dump
    print(f"\n📄 Lendo dump: {args.dump_file}")
    with open(args.dump_file, 'r', encoding='utf-8') as f:
        dump_lines = f.readlines()
    print(f"   {len(dump_lines)} linhas lidas.")

    if args.dry_run:
        print("\n⚠️  MODO DRY-RUN: nenhuma alteração será feita no banco.\n")

    app = create_app()
    with app.app_context():
        with db.engine.connect() as conn:
            # Desabilita FK checks durante a importação
            if not args.dry_run:
                conn.execute(db.text("SET FOREIGN_KEY_CHECKS=0"))

            for tabela in TABELAS:
                create_sql, inserts = extrair_sql_tabela(dump_lines, tabela)

                if create_sql:
                    print(f"\n  [CREATE TABLE encontrado para '{tabela}']")
                else:
                    print(f"\n  [CREATE TABLE NÃO encontrado para '{tabela}']")

                if inserts:
                    print(f"  [{len(inserts)} INSERT(s) encontrado(s) para '{tabela}']")
                else:
                    print(f"  [Nenhum INSERT encontrado para '{tabela}']")

                importar_tabela(conn, tabela, create_sql, inserts, args.dry_run)

            if not args.dry_run:
                conn.execute(db.text("SET FOREIGN_KEY_CHECKS=1"))
                conn.commit()

        # Verifica resultado final
        if not args.dry_run:
            print(f"\n{'='*60}")
            print("  Verificação final:")
            print(f"{'='*60}")
            with db.engine.connect() as conn:
                for tabela in TABELAS:
                    if db.engine.dialect.has_table(conn, tabela):
                        result = conn.execute(db.text(f"SELECT COUNT(*) FROM `{tabela}`"))
                        count = result.scalar()
                        print(f"  ✓ {tabela}: {count} registros")
                    else:
                        print(f"  ✗ {tabela}: NÃO EXISTE")

    print(f"\n✅ Importação concluída!")


if __name__ == '__main__':
    main()
