"""
Script de migração: Atualiza tabela setor.

1. Adiciona coluna 'sigla' VARCHAR(20) se não existir
2. Atualiza siglas dos setores existentes (match por identidade)
3. Insere novos setores do CSV que não existem no banco

Fonte: setores.csv (1837 registros, colunas: identidade, nome, idorgao, sigla)

Uso:
  python scripts/atualizar_setor_sigla.py               # DRY-RUN
  python scripts/atualizar_setor_sigla.py --executar     # APLICA
"""
import os
import sys
import csv
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')

CSV_PATH = r'C:\Users\guilh\OneDrive\Documentos\setores.csv'

DRY_RUN = '--executar' not in sys.argv


def get_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
    )


def column_exists(cursor, table, column):
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (DB_NAME, table, column))
    return cursor.fetchone()['cnt'] > 0


def run():
    conn = get_connection()
    cursor = conn.cursor()

    modo = "DRY-RUN (use --executar para aplicar)" if DRY_RUN else "EXECUTANDO"
    print("=" * 65)
    print(f"Atualização tabela setor — {modo}")
    print("=" * 65)

    # 1. Adicionar coluna sigla
    if column_exists(cursor, 'setor', 'sigla'):
        print("\n[1] Coluna 'sigla' já existe. SKIP.")
    else:
        print("\n[1] Adicionando coluna 'sigla' VARCHAR(20)...")
        if not DRY_RUN:
            cursor.execute("ALTER TABLE setor ADD COLUMN sigla VARCHAR(20) NULL")
            print("    OK — coluna adicionada.")
        else:
            print("    (DRY-RUN) ALTER TABLE setor ADD COLUMN sigla VARCHAR(20) NULL")

    # 2. Ler CSV
    print(f"\n[2] Lendo CSV: {CSV_PATH}")
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        csv_rows = list(reader)
    print(f"    {len(csv_rows)} registros no CSV.")

    # 3. Buscar IDs existentes no banco
    cursor.execute("SELECT identidade FROM setor")
    db_ids = set(r['identidade'] for r in cursor.fetchall())
    print(f"    {len(db_ids)} registros no banco.")

    # 4. Separar: update (existentes) vs insert (novos)
    updates = []
    inserts = []
    for row in csv_rows:
        ident = int(row['identidade'])
        sigla = row.get('sigla', '').strip() or None
        nome = row.get('nome', '').strip()
        idorgao_raw = row.get('idorgao', '').strip()
        idorgao = int(idorgao_raw) if idorgao_raw and idorgao_raw != 'NULL' else None

        if ident in db_ids:
            if sigla:  # só atualiza se CSV tem sigla
                updates.append((sigla, ident))
        else:
            inserts.append((ident, nome, idorgao, sigla))

    print(f"\n[3] Updates (sigla): {len(updates)} registros")
    print(f"    Inserts (novos): {len(inserts)} registros")

    # 5. Executar updates
    if updates:
        print("\n[4] Atualizando siglas...")
        if not DRY_RUN:
            cursor.executemany(
                "UPDATE setor SET sigla = %s WHERE identidade = %s",
                updates,
            )
            print(f"    OK — {cursor.rowcount} linhas atualizadas.")
        else:
            # Mostra amostra
            for sigla, ident in updates[:5]:
                print(f"    (DRY-RUN) UPDATE setor SET sigla='{sigla}' WHERE identidade={ident}")
            if len(updates) > 5:
                print(f"    ... e mais {len(updates) - 5} updates")

    # 6. Executar inserts
    if inserts:
        print("\n[5] Inserindo novos setores...")
        if not DRY_RUN:
            cursor.executemany(
                "INSERT INTO setor (identidade, nome, idorgao, sigla) VALUES (%s, %s, %s, %s)",
                inserts,
            )
            print(f"    OK — {cursor.rowcount} linhas inseridas.")
        else:
            for ident, nome, idorgao, sigla in inserts[:5]:
                print(f"    (DRY-RUN) INSERT setor ({ident}, '{nome[:40]}', {idorgao}, '{sigla}')")
            if len(inserts) > 5:
                print(f"    ... e mais {len(inserts) - 5} inserts")

    # 7. Commit
    if not DRY_RUN:
        conn.commit()
        # Verificação
        cursor.execute("SELECT COUNT(*) as cnt FROM setor")
        total = cursor.fetchone()['cnt']
        cursor.execute("SELECT COUNT(*) as cnt FROM setor WHERE sigla IS NOT NULL")
        com_sigla = cursor.fetchone()['cnt']
        print(f"\n[OK] Total setores: {total} | Com sigla: {com_sigla}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 65)
    print("Concluído!" + (" (DRY-RUN)" if DRY_RUN else ""))
    print("=" * 65)


if __name__ == '__main__':
    run()
