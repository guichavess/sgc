"""
Importa planejamento orçamentário a partir de planilha Excel.

Uso:
    python scripts/importar_planejamento.py <caminho_excel> [--executar]

Sem --executar, roda em DRY-RUN (mostra o que seria feito sem alterar o banco).

Formato esperado da planilha (aba '4_ContratosVigentes'):
    - Coluna 'Nº AUTOMÁTICO SIAFE' = código do contrato
    - Coluna 'NATUREZA' = código da natureza de despesa
    - Coluna 'SUB ITEM2' = código do subitem
    - Colunas '01/01/2026' a '01/12/2026' = valores mensais planejados
    - Contratos duplicados (mesmo SIAFE) são somados por mês
    - Meses com valor 0 ou vazio são ignorados
"""
import sys
import os
import argparse
from datetime import datetime

import pandas as pd

# Setup path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def main():
    parser = argparse.ArgumentParser(description='Importar planejamento orçamentário de Excel')
    parser.add_argument('arquivo', help='Caminho do arquivo Excel (.xlsx)')
    parser.add_argument('--executar', action='store_true', help='Executar de fato (sem isso, roda em DRY-RUN)')
    parser.add_argument('--sheet', default='4_ContratosVigentes', help='Nome da aba (default: 4_ContratosVigentes)')
    args = parser.parse_args()

    if not os.path.exists(args.arquivo):
        print(f'ERRO: Arquivo não encontrado: {args.arquivo}')
        sys.exit(1)

    dry_run = not args.executar
    if dry_run:
        print('=' * 60)
        print('  MODO DRY-RUN — nenhuma alteração será feita no banco')
        print('  Use --executar para aplicar as mudanças')
        print('=' * 60)
    else:
        print('=' * 60)
        print('  MODO EXECUÇÃO — dados serão gravados no banco')
        print('=' * 60)

    # ── Ler planilha ─────────────────────────────────────────────
    print(f'\nLendo {args.arquivo} (aba: {args.sheet})...')
    df = pd.read_excel(args.arquivo, sheet_name=args.sheet)
    print(f'  Linhas: {len(df)}')

    # Detectar coluna SIAFE
    siafe_col = None
    for c in df.columns:
        if 'SIAFE' in str(c).upper():
            siafe_col = c
            break
    if not siafe_col:
        print('ERRO: Coluna SIAFE não encontrada.')
        sys.exit(1)

    # Detectar colunas de meses (formato dd/mm/yyyy com 2026)
    month_cols = [c for c in df.columns
                  if '2026' in str(c) and 'TOTAL' not in str(c).upper() and 'Valor' not in str(c)]
    if not month_cols:
        print('ERRO: Colunas de meses (dd/mm/2026) não encontradas.')
        sys.exit(1)

    print(f'  Coluna SIAFE: "{siafe_col}"')
    print(f'  Colunas de meses: {len(month_cols)}')

    # Mapear coluna → competência (MM/YYYY)
    col_to_comp = {}
    for col in month_cols:
        try:
            dt = datetime.strptime(str(col).strip(), '%d/%m/%Y')
            col_to_comp[col] = dt.strftime('%m/%Y')
        except ValueError:
            # Tentar como datetime do pandas
            dt = pd.to_datetime(col)
            col_to_comp[col] = dt.strftime('%m/%Y')

    # ── Filtrar e agregar ────────────────────────────────────────
    df_valid = df[df[siafe_col].notna() & (df[siafe_col] != 0)].copy()
    df_valid['_cod'] = df_valid[siafe_col].astype(int).astype(str)
    print(f'  Contratos válidos (SIAFE != 0): {len(df_valid)}')

    # Natureza e subitem
    if 'NATUREZA' in df_valid.columns:
        df_valid['_nat'] = df_valid['NATUREZA'].apply(
            lambda x: str(int(x)) if pd.notna(x) else None
        )
    else:
        df_valid['_nat'] = None

    sub_col = 'SUB ITEM2' if 'SUB ITEM2' in df_valid.columns else None
    if sub_col:
        df_valid['_sub'] = df_valid[sub_col].apply(
            lambda x: str(int(x)) if pd.notna(x) else None
        )
    else:
        df_valid['_sub'] = None

    # Agregar por contrato (somar linhas duplicadas)
    agg_vals = df_valid.groupby('_cod')[month_cols].sum()
    meta = df_valid.groupby('_cod').first()[['_nat', '_sub']]

    # Checar duplicados
    from collections import Counter
    dups = {k: v for k, v in Counter(df_valid['_cod']).items() if v > 1}
    if dups:
        print(f'  Contratos com linhas duplicadas (somadas): {len(dups)}')
        for k, v in dups.items():
            print(f'    {k}: {v} linhas')

    # ── Construir registros ──────────────────────────────────────
    records = []
    for cod in agg_vals.index:
        nat = meta.loc[cod, '_nat'] if cod in meta.index else None
        sub = meta.loc[cod, '_sub'] if cod in meta.index else None
        for col in month_cols:
            val = agg_vals.loc[cod, col]
            if pd.notna(val) and float(val) > 0:
                records.append({
                    'cod_contrato': cod,
                    'competencia': col_to_comp[col],
                    'valor': round(float(val), 2),
                    'cod_natureza': nat,
                    'cod_subitem': sub,
                })

    n_contratos = len(agg_vals)
    print(f'\n  Registros a inserir: {len(records)}')
    print(f'  Contratos únicos: {n_contratos}')
    print(f'  Com natureza: {sum(1 for r in records if r["cod_natureza"])}')
    print(f'  Com subitem: {sum(1 for r in records if r["cod_subitem"])}')

    if not records:
        print('\nNenhum registro para inserir.')
        return

    # ── Gravar no banco ──────────────────────────────────────────
    if dry_run:
        print('\n[DRY-RUN] Nenhuma alteração feita. Use --executar para aplicar.')
        # Mostrar amostra
        print('\nAmostra (5 primeiros):')
        for r in records[:5]:
            print(f'  {r["cod_contrato"]} | {r["competencia"]} | '
                  f'R$ {r["valor"]:,.2f} | nat={r["cod_natureza"]} | sub={r["cod_subitem"]}')
        return

    from app import create_app
    from app.extensions import db
    from app.models.planejamento_orcamentario import PlanejamentoOrcamentario

    app = create_app()
    with app.app_context():
        # Limpar tabela
        deleted = db.session.execute(db.text('DELETE FROM planejamento_orcamentario'))
        db.session.commit()
        print(f'\n  Registros anteriores removidos: {deleted.rowcount}')

        agora = datetime.now()
        for r in records:
            p = PlanejamentoOrcamentario(
                cod_contrato=r['cod_contrato'],
                competencia=r['competencia'],
                valor=r['valor'],
                cod_natureza=r['cod_natureza'],
                cod_subitem=r['cod_subitem'],
                dt_lancamento=agora,
                planejamento_inicial=True,
                repactuacao_prorrogacao=False,
            )
            db.session.add(p)

        db.session.commit()
        total = PlanejamentoOrcamentario.query.count()
        print(f'  Inseridos: {len(records)}')
        print(f'  Total no banco: {total}')
        print('\nImportação concluída com sucesso!')


if __name__ == '__main__':
    main()
