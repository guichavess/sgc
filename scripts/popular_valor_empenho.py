"""
Script para popular o valor de empenho nas solicitações identificadas na auditoria.

Fonte: popular_valr_empenho.xlsx (45 solicitações com status de empenho mas sem valor)
- 36 já possuem registro em solicitacaoempenho com valor=0.00 -> UPDATE
- 9 não possuem registro (Empenho Nao Solicitado) -> INSERT

Uso:
    python scripts/popular_valor_empenho.py              # DRY-RUN (padrão)
    python scripts/popular_valor_empenho.py --executar    # Aplica as alterações
"""

import argparse
import sys
import os
import unicodedata

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.solicitacao import Solicitacao, SolicitacaoEmpenho


EXCEL_PATH = r"C:\Users\guilh\OneDrive\Documentos\SEAD\popular_valr_empenho.xlsx"
ADMIN_USER_ID = 1


def main():
    parser = argparse.ArgumentParser(description="Popular valor de empenho a partir da planilha de auditoria.")
    parser.add_argument('--executar', action='store_true', help='Aplica as alterações no banco (sem isso, apenas DRY-RUN)')
    args = parser.parse_args()

    dry_run = not args.executar

    print("=" * 70)
    print("POPULAR VALOR DE EMPENHO - AUDITORIA PAGAMENTOS")
    modo = 'DRY-RUN (nenhuma alteracao sera feita)' if dry_run else 'EXECUCAO REAL'
    print(f"Modo: {modo}")
    print("=" * 70)

    # Ler Excel
    df = pd.read_excel(EXCEL_PATH, sheet_name='Detalhes1')
    # Normalizar nomes de colunas (remover acentos)
    df.columns = [unicodedata.normalize('NFKD', c).encode('ASCII', 'ignore').decode('ASCII') for c in df.columns]
    print(f"\nRegistros no Excel: {len(df)}")
    print(f"Colunas: {list(df.columns)}")

    app = create_app()
    with app.app_context():
        updates = 0
        inserts = 0
        erros = 0

        for _, row in df.iterrows():
            id_solicitacao = int(row['ID'])
            valor = float(row['Valor Empenho'])
            competencia_dt = pd.Timestamp(row['Competencia'])

            # Tratar competencia NaT (ex: IDs 4490, 4491) - buscar da solicitacao no banco
            if pd.isna(competencia_dt):
                sol = db.session.get(Solicitacao, id_solicitacao)
                if sol and sol.data_solicitacao:
                    print(f"  [WARN] Sol {id_solicitacao}: competencia NaT no Excel, usando data_solicitacao: {sol.data_solicitacao}")
                    competencia_str = None  # Manter a que ja existe no registro
                    data_registro = sol.data_solicitacao
                else:
                    print(f"  [ERRO] Sol {id_solicitacao}: competencia NaT e sem data_solicitacao. Pulando.")
                    erros += 1
                    continue
            else:
                # Formato DD/MM/YYYY para a coluna competencia da tabela solicitacaoempenho
                competencia_str = competencia_dt.strftime('%d/%m/%Y')
                # Data do registro = 1o dia da competencia
                data_registro = competencia_dt.to_pydatetime()

            # Buscar registro existente
            existing = SolicitacaoEmpenho.query.filter_by(id_solicitacao=id_solicitacao).first()

            if existing:
                data_str = data_registro.strftime('%Y-%m-%d') if data_registro else 'N/A'
                print(f"  [UPDATE] Sol {id_solicitacao}: valor {existing.valor} -> {valor:.2f} | data -> {data_str}")
                if not dry_run:
                    existing.valor = valor
                    if data_registro:
                        existing.data = data_registro
                updates += 1
            else:
                data_str = data_registro.strftime('%Y-%m-%d') if data_registro else 'N/A'
                print(f"  [INSERT] Sol {id_solicitacao}: valor={valor:.2f} | competencia={competencia_str} | data={data_str}")
                if not dry_run:
                    novo = SolicitacaoEmpenho(
                        id_solicitacao=id_solicitacao,
                        valor=valor,
                        competencia=competencia_str or '01/01/1900',
                        data=data_registro,
                        id_user=ADMIN_USER_ID,
                        ne='0',
                        saldo_momento=0
                    )
                    db.session.add(novo)
                inserts += 1

        print(f"\n{'=' * 70}")
        print(f"RESUMO:")
        print(f"  Updates: {updates}")
        print(f"  Inserts: {inserts}")
        print(f"  Erros:   {erros}")
        print(f"  Total:   {updates + inserts}")

        if not dry_run:
            db.session.commit()
            print("\nAlteracoes COMMITADAS com sucesso!")
        else:
            print(f"\n[DRY-RUN] Nenhuma alteracao foi feita. Use --executar para aplicar.")


if __name__ == '__main__':
    main()
