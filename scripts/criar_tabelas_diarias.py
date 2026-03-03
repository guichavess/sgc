"""
Script de migração: cria tabelas do módulo Diárias e popula dados iniciais.

Os dados de seed são extraídos do dump do banco original (solicitacoes).

Uso:
    python scripts/criar_tabelas_diarias.py          # Cria tabelas + seed
    python scripts/criar_tabelas_diarias.py --seed    # Apenas seed (tabelas já existem)
    python scripts/criar_tabelas_diarias.py --drop    # Drop + recria + seed
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import inspect

from app import create_app
from app.extensions import db
from app.models.diaria import (
    DiariasStatusViagem, DiariasTipoItinerario, DiariasTipoSolicitacao,
    DiariasCargo, DiariasValorCargo, DiariasNatureza, DiariasServidor,
    DiariasAgencia, DiariasItinerario, DiariasItemItinerario, DiariasParada,
    DiariasJustificativa, DiariasCotacao,
)

DIARIAS_TABLES = [
    DiariasStatusViagem, DiariasTipoItinerario, DiariasTipoSolicitacao,
    DiariasCargo, DiariasValorCargo, DiariasNatureza, DiariasServidor,
    DiariasAgencia, DiariasItinerario, DiariasItemItinerario, DiariasParada,
    DiariasJustificativa, DiariasCotacao,
]

REFERENCE_TABLES = ['estados', 'municipios', 'orgao', 'setor']


def _get_existing_tables():
    """Retorna set com nomes das tabelas que já existem no banco."""
    inspector = inspect(db.engine)
    return set(inspector.get_table_names())


def check_reference_tables():
    """Verifica se as tabelas de referência existem no banco sgc."""
    print("\n🔍 Verificando tabelas de referência...")
    existing = _get_existing_tables()
    for table_name in REFERENCE_TABLES:
        if table_name in existing:
            print(f"   ✓ {table_name} (existe)")
        else:
            print(f"   ✗ {table_name} (NÃO ENCONTRADA)")
            print(f"     ⚠  Importe do banco 'solicitacoes' antes de prosseguir.")


def drop_tables():
    """Remove todas as tabelas do módulo Diárias."""
    print("\n🗑️  Removendo tabelas do módulo Diárias...")
    existing = _get_existing_tables()
    with db.engine.begin() as conn:
        conn.execute(db.text("SET FOREIGN_KEY_CHECKS=0"))
        for model in reversed(DIARIAS_TABLES):
            tname = model.__tablename__
            if tname in existing:
                print(f"   DROP TABLE {tname}")
                conn.execute(db.text(f"DROP TABLE IF EXISTS `{tname}`"))
            else:
                print(f"   • {tname} (não existe)")
        conn.execute(db.text("SET FOREIGN_KEY_CHECKS=1"))
    print("   Tabelas removidas com sucesso.")


def create_tables():
    """Cria todas as tabelas do módulo Diárias."""
    print("\n📦 Criando tabelas do módulo Diárias...")
    existing = _get_existing_tables()
    tables_to_create = []
    for model in DIARIAS_TABLES:
        tname = model.__tablename__
        if tname not in existing:
            tables_to_create.append(model.__table__)
            print(f"   CREATE TABLE {tname}")
        else:
            print(f"   ✓ {tname} (já existe)")

    if tables_to_create:
        db.metadata.create_all(db.engine, tables=tables_to_create)
    print("   Tabelas criadas com sucesso.")


def migrate_columns():
    """Adiciona colunas novas em tabelas existentes (migração incremental)."""
    print("\n🔄 Verificando colunas novas...")
    inspector = inspect(db.engine)
    changes = 0

    for model in DIARIAS_TABLES:
        tname = model.__tablename__
        if tname not in inspector.get_table_names():
            continue

        existing_cols = {col['name'] for col in inspector.get_columns(tname)}
        model_cols = {col.name for col in model.__table__.columns}
        missing = model_cols - existing_cols

        for col_name in missing:
            col = model.__table__.columns[col_name]
            col_type = col.type.compile(dialect=db.engine.dialect)
            nullable = "NULL" if col.nullable else "NOT NULL"
            default = ""
            if col.default is not None:
                default = f" DEFAULT '{col.default.arg}'"

            sql = f"ALTER TABLE `{tname}` ADD COLUMN `{col_name}` {col_type} {nullable}{default}"
            print(f"   + {tname}.{col_name} ({col_type})")
            with db.engine.begin() as conn:
                conn.execute(db.text(sql))
            changes += 1

    if changes:
        print(f"   {changes} coluna(s) adicionada(s).")
    else:
        print("   Nenhuma coluna nova para adicionar.")


def seed_data():
    """Popula dados iniciais (dados reais extraídos do dump do banco solicitacoes)."""
    print("\n🌱 Populando dados iniciais...")

    # ── Status de viagem ─────────────────────────────────────────────────
    if DiariasStatusViagem.query.count() == 0:
        db.session.add_all([
            DiariasStatusViagem(id=1, nome='gerado'),
            DiariasStatusViagem(id=2, nome='aceito'),
            DiariasStatusViagem(id=3, nome='rejeitado'),
            DiariasStatusViagem(id=4, nome='cancelado'),
        ])
        db.session.commit()
        print("   ✓ Status de viagem (4 registros)")
    else:
        print("   • Status de viagem (já populado)")

    # ── Tipos de itinerário ──────────────────────────────────────────────
    if DiariasTipoItinerario.query.count() == 0:
        db.session.add_all([
            DiariasTipoItinerario(id=1, nome='Estadual'),
            DiariasTipoItinerario(id=2, nome='Nacional'),
            DiariasTipoItinerario(id=3, nome='Internacional'),
        ])
        db.session.commit()
        print("   ✓ Tipos de itinerário (3 registros)")
    else:
        print("   • Tipos de itinerário (já populado)")

    # ── Tipos de solicitação ──────────────────────────────────────────────
    if DiariasTipoSolicitacao.query.count() == 0:
        db.session.add_all([
            DiariasTipoSolicitacao(id=1, nome='Apenas Diárias'),
            DiariasTipoSolicitacao(id=2, nome='Diárias + Passagens Aéreas'),
            DiariasTipoSolicitacao(id=3, nome='Apenas Passagens Aéreas'),
        ])
        db.session.commit()
        print("   ✓ Tipos de solicitação (3 registros)")
    else:
        print("   • Tipos de solicitação (já populado)")

    # ── Cargos ───────────────────────────────────────────────────────────
    if DiariasCargo.query.count() == 0:
        db.session.add_all([
            DiariasCargo(id=1, nome='Secretário'),
            DiariasCargo(id=2, nome='Superintendente'),
            DiariasCargo(id=3, nome='Diretor'),
            DiariasCargo(id=4, nome='Assessor'),
            DiariasCargo(id=5, nome='Motorista'),
            DiariasCargo(id=6, nome='Coordenador'),
            DiariasCargo(id=7, nome='Gerente'),
            DiariasCargo(id=8, nome='Assessor Técnico'),
        ])
        db.session.commit()
        print("   ✓ Cargos (8 registros)")
    else:
        print("   • Cargos (já populado)")

    # ── Naturezas de despesa ─────────────────────────────────────────────
    if DiariasNatureza.query.count() == 0:
        db.session.add_all([
            DiariasNatureza(id=1, cod_natureza=339014, cod_subnatureza=2,
                            nome_natureza='DIÁRIAS NO PAIS FORA DO ESTADO',
                            nome_subnatureza=None),
            DiariasNatureza(id=2, cod_natureza=339014, cod_subnatureza=1,
                            nome_natureza='DIARIAS NO PAÍS DENTRO DO ESTADO',
                            nome_subnatureza=None),
            DiariasNatureza(id=3, cod_natureza=339014, cod_subnatureza=3,
                            nome_natureza='DIARIAS NO EXTERIOR',
                            nome_subnatureza=None),
        ])
        db.session.commit()
        print("   ✓ Naturezas de despesa (3 registros)")
    else:
        print("   • Naturezas de despesa (já populado)")

    # ── Agências de viagem ───────────────────────────────────────────────
    if DiariasAgencia.query.count() == 0:
        db.session.add_all([
            DiariasAgencia(id=1, nome='Aerovip viagens e turismo ltda', siafe='23005147'),
            DiariasAgencia(id=2, nome='Miraceu Turismo LTDA', siafe='24008992'),
        ])
        db.session.commit()
        print("   ✓ Agências de viagem (2 registros)")
    else:
        print("   • Agências de viagem (já populado)")

    # ── Valor cargo ──────────────────────────────────────────────────────
    if DiariasValorCargo.query.count() == 0:
        db.session.add_all([
            # Estadual (tipo 1)
            DiariasValorCargo(id=1,  cargo_id=1, valor=240.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=2,  cargo_id=2, valor=240.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=3,  cargo_id=3, valor=160.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=4,  cargo_id=4, valor=240.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=5,  cargo_id=5, valor=160.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=6,  cargo_id=6, valor=160.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=7,  cargo_id=7, valor=160.00, tipo_itinerario_id=1),
            # Nacional (tipo 2)
            DiariasValorCargo(id=8,  cargo_id=1, valor=480.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=9,  cargo_id=2, valor=480.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=10, cargo_id=3, valor=320.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=11, cargo_id=4, valor=480.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=12, cargo_id=5, valor=320.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=13, cargo_id=6, valor=320.00, tipo_itinerario_id=2),
            DiariasValorCargo(id=14, cargo_id=7, valor=320.00, tipo_itinerario_id=2),
            # Assessor Técnico
            DiariasValorCargo(id=15, cargo_id=8, valor=160.00, tipo_itinerario_id=1),
            DiariasValorCargo(id=16, cargo_id=8, valor=320.00, tipo_itinerario_id=2),
        ])
        db.session.commit()
        print("   ✓ Valores de cargo (16 registros)")
    else:
        print("   • Valores de cargo (já populado)")

    print("\n✅ Seed concluído com sucesso!")


def main():
    parser = argparse.ArgumentParser(description='Migração do módulo Diárias')
    parser.add_argument('--seed', action='store_true',
                        help='Apenas popular dados (tabelas já existem)')
    parser.add_argument('--drop', action='store_true',
                        help='Dropar e recriar tabelas')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        check_reference_tables()

        if args.drop:
            drop_tables()
            create_tables()
            seed_data()
        elif args.seed:
            seed_data()
        else:
            create_tables()
            migrate_columns()
            seed_data()


if __name__ == '__main__':
    main()
