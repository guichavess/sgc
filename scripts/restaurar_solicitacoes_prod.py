"""
Script para restaurar a tabela sis_solicitacoes a partir do CSV exportado da PROD.
Substitui todos os registros locais pelos dados da produção.

Uso:
  python scripts/restaurar_solicitacoes_prod.py              (DRY-RUN - apenas mostra o que faria)
  python scripts/restaurar_solicitacoes_prod.py --executar    (EXECUTA de fato)
"""
import os
import sys
import csv

from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# =============================================================================
# 1. CARREGAR VARIÁVEIS DE AMBIENTE E CONEXÃO
# =============================================================================

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO CRÍTICO: Variáveis de banco de dados ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =============================================================================
# 2. CONFIGURAÇÕES
# =============================================================================

CSV_FILE = r"C:\Users\guilh\OneDrive\Documentos\sis_solicitacao_prod.csv"
TABELA = "sis_solicitacoes"

EXECUTAR = '--executar' in sys.argv

# Colunas do CSV (na ordem exata do header)
COLUNAS = [
    'id', 'codigo_contrato', 'id_usuario_solicitante', 'data_solicitacao',
    'protocolo_gerado_sei', 'id_procedimento_sei', 'link_processo_sei',
    'competencia', 'especificacao', 'descricao', 'etapa_atual_id',
    'status_geral', 'id_caixa_sei', 'status_empenho_id', 'num_nl',
    'num_pd', 'num_ob', 'tempo_total', 'status_empenho', 'id_tipo_pagamento'
]

# Colunas inteiras (converter ou NULL)
COLUNAS_INT = {
    'id', 'id_usuario_solicitante', 'id_procedimento_sei',
    'etapa_atual_id', 'id_caixa_sei', 'status_empenho_id', 'id_tipo_pagamento'
}

# =============================================================================
# 3. LEITURA DO CSV
# =============================================================================

def ler_csv():
    """Lê o CSV da PROD e retorna lista de dicts com valores tratados."""
    registros = []

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            registro = {}
            for col in COLUNAS:
                val = row.get(col, '').strip()

                # Tratar NULL
                if val == '' or val.upper() == 'NULL':
                    registro[col] = None
                elif col in COLUNAS_INT:
                    registro[col] = int(val)
                else:
                    registro[col] = val

            registros.append(registro)

    return registros


# =============================================================================
# 4. EXECUÇÃO
# =============================================================================

def main():
    print("=" * 60)
    print("  RESTAURAR sis_solicitacoes A PARTIR DA PROD")
    print("=" * 60)

    if not EXECUTAR:
        print("\n  *** MODO DRY-RUN — nenhuma alteração será feita ***")
        print("  Use --executar para aplicar de fato.\n")
    else:
        print("\n  *** MODO EXECUÇÃO — alterações serão aplicadas ***\n")

    # Ler CSV
    if not os.path.exists(CSV_FILE):
        print(f"ERRO: Arquivo não encontrado: {CSV_FILE}")
        sys.exit(1)

    registros = ler_csv()
    print(f"  CSV lido: {len(registros)} registros")

    # Verificar IDs
    ids = [r['id'] for r in registros]
    print(f"  IDs: {min(ids)} a {max(ids)}")

    if not EXECUTAR:
        # Mostrar resumo
        print(f"\n  O que será feito:")
        print(f"    1. TRUNCATE TABLE {TABELA}")
        print(f"    2. INSERT de {len(registros)} registros")
        print(f"    3. Reset AUTO_INCREMENT para {max(ids) + 1}")
        print(f"\n  Para executar: python scripts/restaurar_solicitacoes_prod.py --executar")
        return

    # Executar
    with ENGINE.begin() as conn:
        # Desabilitar FK checks temporariamente
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))

        # Truncar tabela
        print(f"\n  [1/3] TRUNCATE TABLE {TABELA}...")
        conn.execute(text(f"TRUNCATE TABLE {TABELA}"))
        print("        OK")

        # Inserir registros em lotes
        print(f"  [2/3] Inserindo {len(registros)} registros...")

        placeholders = ', '.join([f':{col}' for col in COLUNAS])
        cols_str = ', '.join(COLUNAS)
        insert_sql = text(f"INSERT INTO {TABELA} ({cols_str}) VALUES ({placeholders})")

        lote = 100
        inseridos = 0
        for i in range(0, len(registros), lote):
            batch = registros[i:i + lote]
            for reg in batch:
                conn.execute(insert_sql, reg)
                inseridos += 1

            print(f"        {inseridos}/{len(registros)} inseridos...")

        print(f"        OK — {inseridos} registros inseridos")

        # Reset auto_increment
        next_id = max(ids) + 1
        print(f"  [3/3] ALTER TABLE AUTO_INCREMENT = {next_id}...")
        conn.execute(text(f"ALTER TABLE {TABELA} AUTO_INCREMENT = {next_id}"))
        print("        OK")

        # Reabilitar FK checks
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    # Verificação
    print("\n  Verificando...")
    with ENGINE.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA}"))
        total = result.scalar()
        print(f"  Total no banco: {total} registros")

        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA} WHERE id_tipo_pagamento IS NOT NULL"))
        com_tipo = result.scalar()
        print(f"  Com tipo_pagamento: {com_tipo}")

        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABELA} WHERE tempo_total LIKE '-%'"))
        negativos = result.scalar()
        print(f"  Com tempo_total negativo: {negativos}")

    print("\n" + "=" * 60)
    print("  CONCLUÍDO!")
    print("=" * 60)


if __name__ == '__main__':
    main()
