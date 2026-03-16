"""
Script para criar índices de performance nas tabelas mais consultadas.
Cada índice é criado com IF NOT EXISTS (via try/except) para ser idempotente.

Uso: python scripts/criar_indices_performance.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.extensions import db

INDICES = [
    # sis_solicitacoes — filtros do dashboard
    ("idx_sol_status_empenho", "sis_solicitacoes", "status_empenho_id"),
    ("idx_sol_etapa_atual", "sis_solicitacoes", "etapa_atual_id"),
    ("idx_sol_competencia", "sis_solicitacoes", "competencia(50)"),  # TEXT precisa de prefixo
    ("idx_sol_status_geral", "sis_solicitacoes", "status_geral(50)"),
    ("idx_sol_usuario", "sis_solicitacoes", "id_usuario_solicitante"),
    ("idx_sol_data", "sis_solicitacoes", "data_solicitacao"),
    ("idx_sol_tipo_pagamento", "sis_solicitacoes", "id_tipo_pagamento"),

    # solicitacaoempenho — pendências NE
    ("idx_solempenho_sol_ne", "solicitacaoempenho", "id_solicitacao, ne"),

    # empenho — filtros financeiros
    ("idx_empenho_contrato_ano", "empenho", "codContrato, anoProcesso"),
    ("idx_empenho_ug_status_data", "empenho", "codigoUG(20), statusDocumento(30), dataEmissao"),  # TEXT cols

    # liquidacao — filtros financeiros
    ("idx_liq_ug_contrato_data", "liquidacao", "codigoUG(20), codContrato, statusDocumento(30), dataEmissao"),

    # sis_historico_movimentacoes — timeline e tempo médio
    ("idx_hist_sol_data", "sis_historico_movimentacoes", "id_solicitacao, data_movimentacao"),

    # notificacoes — context processor
    ("idx_notif_usuario_lida", "notificacoes", "usuario_id, lida"),

    # contratos — situação
    ("idx_contrato_situacao", "contratos", "situacao"),
]


def main():
    app = create_app()
    with app.app_context():
        criados = 0
        existentes = 0
        for idx_name, table, columns in INDICES:
            sql = f"CREATE INDEX {idx_name} ON {table} ({columns})"
            try:
                db.session.execute(db.text(sql))
                db.session.commit()
                print(f"  [OK] {idx_name} em {table}({columns})")
                criados += 1
            except Exception as e:
                db.session.rollback()
                if 'Duplicate' in str(e) or 'already exists' in str(e).lower():
                    print(f"  [--] {idx_name} já existe")
                    existentes += 1
                else:
                    print(f"  [ERRO] {idx_name}: {e}")

        print(f"\nResumo: {criados} criados, {existentes} já existiam")


if __name__ == '__main__':
    main()
