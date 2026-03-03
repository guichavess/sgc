"""
Migra diarias_cotacoes: troca agencia_id (FK diarias_agencias) por contrato_codigo (FK contratos).
- Converte registros existentes usando o campo 'siafe' da diarias_agencias como contrato_codigo.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

def migrate():
    with app.app_context():
        conn = db.session.connection()

        # 1. Verifica se coluna contrato_codigo ja existe
        cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM diarias_cotacoes")).fetchall()]

        if 'contrato_codigo' not in cols:
            print("[1/4] Adicionando coluna contrato_codigo...")
            conn.execute(text("ALTER TABLE diarias_cotacoes ADD COLUMN contrato_codigo VARCHAR(20) NULL"))
        else:
            print("[1/4] Coluna contrato_codigo ja existe.")

        # 2. Converte dados existentes: agencia_id -> contrato_codigo via diarias_agencias.siafe
        if 'agencia_id' in cols:
            print("[2/4] Convertendo dados existentes (agencia_id -> contrato_codigo via siafe)...")
            conn.execute(text("""
                UPDATE diarias_cotacoes c
                INNER JOIN diarias_agencias a ON c.agencia_id = a.id
                SET c.contrato_codigo = a.siafe
                WHERE c.contrato_codigo IS NULL AND a.siafe IS NOT NULL
            """))
            rows = conn.execute(text("SELECT COUNT(*) FROM diarias_cotacoes WHERE contrato_codigo IS NOT NULL")).scalar()
            print(f"       {rows} registros convertidos.")
        else:
            print("[2/4] Coluna agencia_id nao encontrada, pulando conversao.")

        # 3. Remove FK e coluna agencia_id
        if 'agencia_id' in cols:
            print("[3/4] Removendo FK e coluna agencia_id...")
            # Descobre o nome da FK constraint
            fks = conn.execute(text("""
                SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'diarias_cotacoes'
                  AND COLUMN_NAME = 'agencia_id'
                  AND REFERENCED_TABLE_NAME IS NOT NULL
            """)).fetchall()
            for fk in fks:
                print(f"       Removendo FK: {fk[0]}")
                conn.execute(text(f"ALTER TABLE diarias_cotacoes DROP FOREIGN KEY {fk[0]}"))
            conn.execute(text("ALTER TABLE diarias_cotacoes DROP COLUMN agencia_id"))
        else:
            print("[3/4] agencia_id ja foi removida.")

        # 4. Adiciona FK para contratos.codigo (opcional, nao bloqueia se contrato nao existe)
        print("[4/4] Adicionando FK contrato_codigo -> contratos.codigo...")
        try:
            conn.execute(text("""
                ALTER TABLE diarias_cotacoes
                ADD CONSTRAINT fk_cotacoes_contrato
                FOREIGN KEY (contrato_codigo) REFERENCES contratos(codigo)
                ON DELETE SET NULL
            """))
            print("       FK adicionada com sucesso.")
        except Exception as e:
            print(f"       Aviso: FK nao adicionada ({e}). Continuando sem FK.")

        db.session.commit()
        print("\nMigracao concluida com sucesso!")


if __name__ == '__main__':
    migrate()
