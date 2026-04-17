"""
Script de exportação: copia dados de diárias do banco LOCAL para PRODUÇÃO.

Lê variáveis de conexão de .env (local) e aceita override via CLI para produção.
DRY-RUN por default. Use --executar para aplicar.

Uso:
    python scripts/exportar_diarias_producao.py                          # dry-run (contagens)
    python scripts/exportar_diarias_producao.py --executar               # aplica
    python scripts/exportar_diarias_producao.py --executar --prod-host X --prod-user Y --prod-pass Z --prod-db W
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

import pymysql

# ── Tabelas na ordem de dependência FK (TRUNCATE reverso, INSERT direto) ──────
TABELAS_ORDENADAS = [
    # Nível 0
    'diarias_servidores',
    # Nível 1
    'diarias_itinerario',
    # Nível 2
    'diarias_itinerario_documentos',
    'diarias_itinerario_quadro_orcamentario',
    'diarias_itens_itinerario',
    'diarias_paradas',
    'diarias_justificativa',
    'diarias_historico_movimentacoes',
    'diarias_cotacoes',
    'diarias_controle_viagens',
    'diarias_movimentacao',
    # Nível 3
    'diarias_cotacoes_voos',
    'diarias_notas_reserva',
    'diarias_notas_empenho',
    'diarias_notas_liquidacao',
    'diarias_programacoes_desembolso',
    'diarias_ordens_bancarias',
    'diarias_notas_patrimoniais',
    'diarias_controle_servidores',
    # Nível 4
    'diarias_controle_prestacao',
]

BATCH_SIZE = 500


def get_connection(host, user, password, db_name, port=3306):
    """Cria conexão PyMySQL."""
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        port=port,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def tabela_existe(cursor, tabela):
    """Verifica se tabela existe no banco."""
    cursor.execute(f"SHOW TABLES LIKE %s", (tabela,))
    return cursor.fetchone() is not None


def contar_registros(cursor, tabela):
    """Conta registros na tabela."""
    cursor.execute(f"SELECT COUNT(*) as cnt FROM `{tabela}`")
    return cursor.fetchone()['cnt']


def exportar(args):
    # ── Conexão LOCAL ─────────────────────────────────────────────────────
    local_host = os.getenv('DB_HOST', 'localhost')
    local_user = os.getenv('DB_USER', 'root')
    local_pass = os.getenv('DB_PASS', 'root')
    local_db = os.getenv('DB_NAME', 'sgc')

    # ── Conexão PRODUÇÃO ──────────────────────────────────────────────────
    prod_host = args.prod_host or os.getenv('PROD_DB_HOST', '')
    prod_user = args.prod_user or os.getenv('PROD_DB_USER', '')
    prod_pass = args.prod_pass or os.getenv('PROD_DB_PASS', '')
    prod_db = args.prod_db or os.getenv('PROD_DB_NAME', '')
    prod_port = args.prod_port or int(os.getenv('PROD_DB_PORT', '3306'))

    if not all([prod_host, prod_user, prod_pass, prod_db]):
        print("ERRO: Credenciais de produção não definidas.")
        print("Use --prod-host, --prod-user, --prod-pass, --prod-db")
        print("Ou defina PROD_DB_HOST, PROD_DB_USER, PROD_DB_PASS, PROD_DB_NAME no .env")
        sys.exit(1)

    executar = args.executar
    modo = "EXECUTANDO" if executar else "DRY-RUN"

    print(f"\n{'='*60}")
    print(f"  Exportação Diárias Local → Produção — {modo}")
    print(f"{'='*60}")
    print(f"\n  Local:     {local_user}@{local_host}/{local_db}")
    print(f"  Produção:  {prod_user}@{prod_host}/{prod_db}")
    print()

    # ── Conectar ──────────────────────────────────────────────────────────
    conn_local = get_connection(local_host, local_user, local_pass, local_db)
    conn_prod = get_connection(prod_host, prod_user, prod_pass, prod_db, prod_port)

    cur_local = conn_local.cursor()
    cur_prod = conn_prod.cursor()

    try:
        # ── Fase 1: Contagem no local ─────────────────────────────────────
        print("=" * 60)
        print("CONTAGEM NO BANCO LOCAL")
        print("=" * 60)

        tabelas_com_dados = []
        for tabela in TABELAS_ORDENADAS:
            if not tabela_existe(cur_local, tabela):
                print(f"  {tabela}: NÃO EXISTE (pulando)")
                continue
            cnt = contar_registros(cur_local, tabela)
            print(f"  {tabela}: {cnt} registros")
            if cnt > 0:
                tabelas_com_dados.append((tabela, cnt))

        total_registros = sum(c for _, c in tabelas_com_dados)
        print(f"\n  Total: {total_registros} registros em {len(tabelas_com_dados)} tabelas")

        if not executar:
            print(f"\n⚠️  DRY-RUN: nenhuma alteração feita. Use --executar para aplicar.")

            # Mostrar contagem em produção também
            print(f"\n{'='*60}")
            print("CONTAGEM NO BANCO DE PRODUÇÃO (atual)")
            print("=" * 60)
            for tabela in TABELAS_ORDENADAS:
                if tabela_existe(cur_prod, tabela):
                    cnt_prod = contar_registros(cur_prod, tabela)
                    print(f"  {tabela}: {cnt_prod} registros")
                else:
                    print(f"  {tabela}: NÃO EXISTE")
            return

        # ── Fase 2: TRUNCATE em produção ──────────────────────────────────
        print(f"\n{'='*60}")
        print("LIMPANDO TABELAS EM PRODUÇÃO")
        print("=" * 60)

        cur_prod.execute("SET FOREIGN_KEY_CHECKS = 0")
        for tabela in reversed(TABELAS_ORDENADAS):
            if tabela_existe(cur_prod, tabela):
                cur_prod.execute(f"TRUNCATE TABLE `{tabela}`")
                print(f"  TRUNCATE {tabela} ✓")
            else:
                print(f"  {tabela}: NÃO EXISTE (pulando)")
        cur_prod.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn_prod.commit()

        # ── Fase 3: Copiar dados ──────────────────────────────────────────
        print(f"\n{'='*60}")
        print("COPIANDO DADOS LOCAL → PRODUÇÃO")
        print("=" * 60)

        resumo = []
        for tabela, cnt_local in tabelas_com_dados:
            if not tabela_existe(cur_prod, tabela):
                print(f"  {tabela}: NÃO EXISTE em produção (pulando)")
                resumo.append((tabela, cnt_local, 0, 'SKIP'))
                continue

            # Obter colunas
            cur_local.execute(f"SHOW COLUMNS FROM `{tabela}`")
            colunas_local = [row['Field'] for row in cur_local.fetchall()]

            cur_prod.execute(f"SHOW COLUMNS FROM `{tabela}`")
            colunas_prod = {row['Field'] for row in cur_prod.fetchall()}

            # Usar apenas colunas que existem em AMBOS
            colunas = [c for c in colunas_local if c in colunas_prod]
            if not colunas:
                print(f"  {tabela}: nenhuma coluna em comum (pulando)")
                resumo.append((tabela, cnt_local, 0, 'NO_COLS'))
                continue

            colunas_str = ', '.join(f'`{c}`' for c in colunas)
            placeholders = ', '.join(['%s'] * len(colunas))
            insert_sql = f"INSERT INTO `{tabela}` ({colunas_str}) VALUES ({placeholders})"

            # Ler em batches
            offset = 0
            total_inserido = 0
            cur_prod.execute("SET FOREIGN_KEY_CHECKS = 0")

            while True:
                cur_local.execute(
                    f"SELECT {colunas_str} FROM `{tabela}` LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
                rows = cur_local.fetchall()
                if not rows:
                    break

                values = [tuple(row[c] for c in colunas) for row in rows]
                cur_prod.executemany(insert_sql, values)
                total_inserido += len(rows)
                offset += BATCH_SIZE

            cur_prod.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn_prod.commit()

            status = '✓' if total_inserido == cnt_local else f'⚠️ {total_inserido}/{cnt_local}'
            print(f"  {tabela}: {total_inserido} registros {status}")
            resumo.append((tabela, cnt_local, total_inserido, 'OK'))

        # ── Fase 4: Resumo ────────────────────────────────────────────────
        print(f"\n{'='*60}")
        print("RESUMO")
        print("=" * 60)
        total_local = 0
        total_prod = 0
        for tabela, cnt_l, cnt_p, status in resumo:
            total_local += cnt_l
            total_prod += cnt_p
            mark = '✓' if status == 'OK' and cnt_l == cnt_p else status
            print(f"  {tabela:45s}  local={cnt_l:>6}  prod={cnt_p:>6}  {mark}")

        print(f"\n  TOTAL: {total_prod}/{total_local} registros copiados")

        if total_local == total_prod:
            print("\n✅ Exportação concluída com sucesso!")
        else:
            print("\n⚠️  Exportação concluída com divergências — verificar tabelas marcadas.")

    finally:
        cur_local.close()
        cur_prod.close()
        conn_local.close()
        conn_prod.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Exportar dados de diárias do banco local para produção')
    parser.add_argument('--executar', action='store_true', help='Aplicar (sem flag = dry-run)')
    parser.add_argument('--prod-host', type=str, help='Host do banco de produção')
    parser.add_argument('--prod-user', type=str, help='Usuário do banco de produção')
    parser.add_argument('--prod-pass', type=str, help='Senha do banco de produção')
    parser.add_argument('--prod-db', type=str, help='Nome do banco de produção')
    parser.add_argument('--prod-port', type=int, help='Porta do banco de produção (default: 3306)')
    args = parser.parse_args()
    exportar(args)
