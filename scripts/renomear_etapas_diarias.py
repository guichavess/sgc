"""
Script de migração: Renomeia etapas 2 e 3 do fluxo de Diárias e remove etapa 4.

Alterações:
  - diarias_etapas id=2: "Solicitação Autorizada" → "Financeiro"
  - diarias_etapas id=3: "Análise de Disponibilidade Orçamentária" → "Aquisição de Passagens"
  - diarias_etapas id=4: REMOVIDA (redundante)

Motivo:
  A análise do fluxo real (JSON de documentos) mostrou que o Financeiro (NR + Quadro)
  acontece ANTES da Aquisição de Passagens (Cotações + Escolha). Os nomes das etapas
  2 e 3 não correspondiam ao que realmente acontece em cada fase.

Uso:
  python scripts/renomear_etapas_diarias.py
"""
import os
import sys
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def run_migration():
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 60)
    print("Migração: Renomear Etapas 2 e 3 do Fluxo de Diárias")
    print("=" * 60)

    # 1. Verificar estado atual
    print("\n[1/4] Verificando estado atual das etapas...")
    cursor.execute("SELECT id, nome, alias, cor_hex, icone FROM diarias_etapas ORDER BY id")
    etapas = cursor.fetchall()
    for e in etapas:
        print(f"   id={e['id']}: {e['nome']} ({e['alias']})")

    # 2. Verificar se existem registros com etapa_atual_id = 4
    print("\n[2/4] Verificando registros com etapa_atual_id = 4...")
    cursor.execute("SELECT COUNT(*) as cnt FROM diarias_itinerario WHERE etapa_atual_id = 4")
    count_etapa4 = cursor.fetchone()['cnt']
    if count_etapa4 > 0:
        print(f"   ATENÇÃO: {count_etapa4} registro(s) com etapa_atual_id = 4!")
        print("   Esses registros serão migrados para etapa_atual_id = 3 (Aquisição de Passagens).")
        cursor.execute("UPDATE diarias_itinerario SET etapa_atual_id = 3 WHERE etapa_atual_id = 4")
        print(f"   OK - {count_etapa4} registro(s) migrado(s).")
    else:
        print("   OK - Nenhum registro com etapa_atual_id = 4.")

    # 3. Verificar historico com referencia a etapa 4
    print("\n[3/4] Verificando histórico com referência a etapa 4...")
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM diarias_historico_movimentacoes
        WHERE id_etapa_nova = 4 OR id_etapa_anterior = 4
    """)
    count_hist4 = cursor.fetchone()['cnt']
    if count_hist4 > 0:
        print(f"   ATENÇÃO: {count_hist4} registro(s) no histórico referenciando etapa 4!")
        print("   Atualizando referências para etapa 3...")
        cursor.execute("UPDATE diarias_historico_movimentacoes SET id_etapa_nova = 3 WHERE id_etapa_nova = 4")
        cursor.execute("UPDATE diarias_historico_movimentacoes SET id_etapa_anterior = 3 WHERE id_etapa_anterior = 4")
        print("   OK - Histórico atualizado.")
    else:
        print("   OK - Nenhum registro no histórico referenciando etapa 4.")

    # 4. Renomear etapas e remover etapa 4
    print("\n[4/4] Renomeando etapas e removendo etapa 4...")

    # Etapa 2: "Solicitação Autorizada" → "Financeiro"
    cursor.execute("""
        UPDATE diarias_etapas
        SET nome = 'Financeiro',
            alias = 'financeiro',
            cor_hex = '#fd7e14',
            icone = 'fas fa-search-dollar'
        WHERE id = 2
    """)
    print("   [UPDATE] id=2: Financeiro (alias=financeiro, cor=#fd7e14, icone=fas fa-search-dollar)")

    # Etapa 3: "Análise de Disponibilidade Orçamentária" → "Aquisição de Passagens"
    cursor.execute("""
        UPDATE diarias_etapas
        SET nome = 'Aquisição de Passagens',
            alias = 'aquisicao_passagens',
            cor_hex = '#6f42c1',
            icone = 'fas fa-plane-departure'
        WHERE id = 3
    """)
    print("   [UPDATE] id=3: Aquisição de Passagens (alias=aquisicao_passagens, cor=#6f42c1, icone=fas fa-plane-departure)")

    # Remover etapa 4
    cursor.execute("DELETE FROM diarias_etapas WHERE id = 4")
    print("   [DELETE] id=4: Removida")

    conn.commit()

    # Verificação final
    print("\n--- Estado final das etapas ---")
    cursor.execute("SELECT id, nome, alias, cor_hex, icone FROM diarias_etapas ORDER BY id")
    for e in cursor.fetchall():
        print(f"   id={e['id']}: {e['nome']} ({e['alias']}) cor={e['cor_hex']} icone={e['icone']}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("Migração concluída com sucesso!")
    print("=" * 60)


if __name__ == '__main__':
    run_migration()
