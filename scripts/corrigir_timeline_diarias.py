"""
Script de correção: Restaura a ordem correta das etapas no módulo de Diárias.

Problema:
  As etapas 2 (Escolha do Voo) e 3 (Análise 1ª Parte) tiveram seus valores de
  `ordem` trocados, causando exibição incorreta da timeline.

Correções aplicadas:
  1. Etapa 3 (Análise 1ª Parte): ordem 3 → 2
  2. Etapa 2 (Escolha do Voo): ordem 2 → 3
  3. Etapa 6 (Análise 2ª Parte): CRIAR se não existir, ordem=4
  4. Etapa 4 (Concessão): ordem 4 → 5 (deslocamento)
  5. Etapa 5 (Prestação): ordem 5 → 6 (deslocamento)

Uso:
  python scripts/corrigir_timeline_diarias.py

Resultado esperado:
  Timeline ordena-se conforme CLAUDE.md §12:
  1. Solicitação Inicial
  2. Análise 1ª Parte
  3. Escolha do Voo
  4. Análise 2ª Parte
  5. Concessão de Diárias
  6. Prestação de Contas
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasEtapa


def main():
    app = create_app()
    with app.app_context():
        print("=" * 80)
        print("CORREÇÃO DA TIMELINE DO MÓDULO DE DIÁRIAS")
        print("=" * 80)

        print("\n[1/3] ESTADO ANTES DA CORREÇÃO:")
        print("-" * 80)
        etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()
        for e in etapas:
            print(f"  ID={e.id:2d} | Ordem={e.ordem:2d} | {e.nome:30s}")

        print("\n[2/3] APLICANDO CORREÇÕES...")
        print("-" * 80)

        # Correção 1: Etapa 3 (Análise 1ª Parte) para ordem=2
        etapa_3 = DiariasEtapa.query.get(3)
        if etapa_3:
            old_ordem = etapa_3.ordem
            etapa_3.ordem = 2
            db.session.commit()
            print(
                f"  [OK] Etapa 3 (Análise da Solicitação):"
                f" ordem {old_ordem} -> 2"
            )

        # Correção 2: Etapa 2 (Escolha do Voo) para ordem=3
        etapa_2 = DiariasEtapa.query.get(2)
        if etapa_2:
            old_ordem = etapa_2.ordem
            etapa_2.ordem = 3
            db.session.commit()
            print(f"  [OK] Etapa 2 (Escolha do Voo): ordem {old_ordem} -> 3")

        # Correção 3: Inserir ou atualizar Etapa 6 (Análise 2ª Parte)
        etapa_6 = DiariasEtapa.query.get(6)
        if not etapa_6:
            etapa_6 = DiariasEtapa(
                id=6,
                nome='Análise 2ª Parte',
                alias='analise_2_parte',
                ordem=4,
                cor_hex='#17a2b8',
                icone='fas fa-microscope'
            )
            db.session.add(etapa_6)
            db.session.commit()
            print(f"  [OK] Etapa 6 (Análise 2ª Parte): CRIADA com ordem=4")
        else:
            old_ordem = etapa_6.ordem
            etapa_6.ordem = 4
            db.session.commit()
            print(
                f"  [OK] Etapa 6 (Análise 2ª Parte): ordem {old_ordem} -> 4"
                f" (já existia)"
            )

        # Correção 4: Deslocar Etapa 4 (Concessão) para ordem=5
        etapa_4 = DiariasEtapa.query.get(4)
        if etapa_4:
            old_ordem = etapa_4.ordem
            etapa_4.ordem = 5
            db.session.commit()
            print(
                f"  [OK] Etapa 4 (Concessão das Diárias):"
                f" ordem {old_ordem} -> 5"
            )

        # Correção 5: Deslocar Etapa 5 (Prestação) para ordem=6
        etapa_5 = DiariasEtapa.query.get(5)
        if etapa_5:
            old_ordem = etapa_5.ordem
            etapa_5.ordem = 6
            db.session.commit()
            print(
                f"  [OK] Etapa 5 (Prestação de Contas):"
                f" ordem {old_ordem} -> 6"
            )

        print("\n[3/3] ESTADO APÓS A CORREÇÃO:")
        print("-" * 80)
        etapas = DiariasEtapa.query.order_by(DiariasEtapa.ordem).all()
        for e in etapas:
            print(
                f"  ID={e.id:2d} | Ordem={e.ordem:2d} |"
                f" {e.nome:30s} | alias={e.alias}"
            )

        print("\n" + "=" * 80)
        print("✅ CORREÇÃO APLICADA COM SUCESSO!")
        print("=" * 80)


if __name__ == '__main__':
    main()
