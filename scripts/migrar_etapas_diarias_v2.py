"""
Script de migração: Atualiza etapas de diárias de 11 → 5 etapas
conforme a nova Lista de Verificação (Concessão de Diárias e Passagens).

NOVA ESTRUTURA:
  1. Solicitação inicial
  2. Escolha do voo (condicional: só com passagens)
  3. Análise da solicitação
  4. Concessão das diárias
  5. Prestação de contas

Uso:
    python scripts/migrar_etapas_diarias_v2.py              # DRY-RUN (mostra o que faria)
    python scripts/migrar_etapas_diarias_v2.py --executar    # Aplica as alterações
"""
import argparse
import sys
import os
import io

# Fix Windows console encoding for unicode
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db

# Mapeamento: etapa_atual_id antiga → etapa_atual_id nova
MAPA_ETAPAS = {
    1: 1,   # Solicitação Iniciada → Solicitação Inicial
    2: 3,   # Financeiro (NR) → Análise da Solicitação
    3: 2,   # Aquisição de Passagens → Escolha do Voo
    4: 3,   # Despacho CCDP → Análise da Solicitação
    5: 3,   # Ciência SGA → Análise da Solicitação
    6: 3,   # Análise NCI → Análise da Solicitação
    7: 4,   # Despacho APOIO → Concessão das Diárias
    8: 4,   # Despacho Diretor → Concessão das Diárias
    9: 4,   # Despacho GEO → Concessão das Diárias
    10: 5,  # Comprovante Viagem → Prestação de Contas
    11: 5,  # Prestação Contas CCDP → Prestação de Contas
}

# Novas 5 etapas
NOVAS_ETAPAS = [
    {'id': 1, 'nome': 'Solicitação Inicial',     'alias': 'solicitacao_inicial',   'ordem': 1, 'cor_hex': '#0d6efd', 'icone': 'fas fa-file-alt'},
    {'id': 2, 'nome': 'Escolha do Voo',          'alias': 'escolha_voo',           'ordem': 2, 'cor_hex': '#6f42c1', 'icone': 'fas fa-plane-departure'},
    {'id': 3, 'nome': 'Análise da Solicitação',  'alias': 'analise_solicitacao',   'ordem': 3, 'cor_hex': '#fd7e14', 'icone': 'fas fa-clipboard-check'},
    {'id': 4, 'nome': 'Concessão das Diárias',   'alias': 'concessao_diarias',     'ordem': 4, 'cor_hex': '#198754', 'icone': 'fas fa-money-check-alt'},
    {'id': 5, 'nome': 'Prestação de Contas',     'alias': 'prestacao_contas',      'ordem': 5, 'cor_hex': '#dc3545', 'icone': 'fas fa-balance-scale'},
]


def migrar(executar=False):
    app = create_app()
    with app.app_context():
        modo = "EXECUTANDO" if executar else "DRY-RUN"
        print(f"\n{'='*60}")
        print(f"  Migração Etapas Diárias v2 — {modo}")
        print(f"{'='*60}\n")

        # 1. Buscar itinerários com etapas antigas
        result = db.session.execute(
            db.text("SELECT id, etapa_atual_id FROM diarias_itinerario ORDER BY id")
        )
        itinerarios = result.fetchall()
        print(f"📋 {len(itinerarios)} itinerários encontrados\n")

        # 2. Mostrar mapeamento que será aplicado
        atualizacoes = []
        for it_id, etapa_old in itinerarios:
            etapa_new = MAPA_ETAPAS.get(etapa_old, 1)
            if etapa_old != etapa_new:
                atualizacoes.append((it_id, etapa_old, etapa_new))
                print(f"   Itinerário #{it_id}: etapa {etapa_old} → {etapa_new}")

        if not atualizacoes:
            print("   Nenhum itinerário precisa de atualização.")

        print(f"\n   Total a atualizar: {len(atualizacoes)}")

        if not executar:
            print(f"\n⚠️  DRY-RUN: nenhuma alteração foi feita. Use --executar para aplicar.")
            return

        # 3. Remover FK temporariamente para poder atualizar etapas
        print("\n🔧 Removendo FK constraint de diarias_itinerario...")
        try:
            db.session.execute(db.text(
                "ALTER TABLE diarias_itinerario DROP FOREIGN KEY diarias_itinerario_ibfk_etapa"
            ))
        except Exception:
            # Tenta nome genérico do FK
            try:
                # Busca nome real da FK
                fk_result = db.session.execute(db.text("""
                    SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = 'diarias_itinerario'
                    AND COLUMN_NAME = 'etapa_atual_id'
                    AND REFERENCED_TABLE_NAME = 'diarias_etapas'
                """))
                fk_name = fk_result.scalar()
                if fk_name:
                    db.session.execute(db.text(
                        f"ALTER TABLE diarias_itinerario DROP FOREIGN KEY `{fk_name}`"
                    ))
                    print(f"   FK removida: {fk_name}")
            except Exception as e:
                print(f"   ⚠️  Não conseguiu remover FK (pode não existir): {e}")

        # 3b. Remover FK do historico também
        print("🔧 Removendo FK constraint de diarias_historico_movimentacoes...")
        try:
            fk_result = db.session.execute(db.text("""
                SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = 'diarias_historico_movimentacoes'
                AND COLUMN_NAME = 'id_etapa_nova'
                AND REFERENCED_TABLE_NAME = 'diarias_etapas'
            """))
            fk_name = fk_result.scalar()
            if fk_name:
                db.session.execute(db.text(
                    f"ALTER TABLE diarias_historico_movimentacoes DROP FOREIGN KEY `{fk_name}`"
                ))
                print(f"   FK removida: {fk_name}")
        except Exception as e:
            print(f"   ⚠️  {e}")

        # 4. Atualizar etapa_atual_id nos itinerários
        print("\n📝 Atualizando etapa_atual_id dos itinerários...")
        for it_id, etapa_old, etapa_new in atualizacoes:
            db.session.execute(db.text(
                "UPDATE diarias_itinerario SET etapa_atual_id = :nova WHERE id = :id"
            ), {'nova': etapa_new, 'id': it_id})

        # 5. Limpar e reinserir etapas
        print("\n🗑️  Removendo etapas antigas...")
        db.session.execute(db.text("DELETE FROM diarias_etapas"))

        print("✅ Inserindo 5 novas etapas...")
        for e in NOVAS_ETAPAS:
            db.session.execute(db.text("""
                INSERT INTO diarias_etapas (id, nome, alias, ordem, cor_hex, icone)
                VALUES (:id, :nome, :alias, :ordem, :cor_hex, :icone)
            """), e)
            print(f"   {e['id']}. {e['nome']} ({e['alias']})")

        # 6. Recriar FK
        print("\n🔧 Recriando FK constraints...")
        try:
            db.session.execute(db.text("""
                ALTER TABLE diarias_itinerario
                ADD CONSTRAINT fk_itinerario_etapa
                FOREIGN KEY (etapa_atual_id) REFERENCES diarias_etapas(id)
            """))
            print("   FK diarias_itinerario.etapa_atual_id → diarias_etapas.id ✓")
        except Exception as e:
            print(f"   ⚠️  FK itinerario: {e}")

        # Não recria FK do historico (dados antigos referenciam IDs 1-11)
        print("   ℹ️  FK do historico NÃO recriada (dados antigos referenciam IDs 1-11)")

        db.session.commit()
        print(f"\n✅ Migração concluída com sucesso!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrar etapas de diárias para v2 (5 etapas)')
    parser.add_argument('--executar', action='store_true', help='Aplicar alterações (sem flag = dry-run)')
    args = parser.parse_args()
    migrar(executar=args.executar)
