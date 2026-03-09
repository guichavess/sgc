"""
Script padrao para importar solicitacoes de pagamento a partir de planilhas Excel.

Fluxo:
  1. Le o Excel e normaliza colunas
  2. Verifica quais protocolos ja existem no banco (por protocolo_gerado_sei)
  3. Gera CSV de analise com coluna "existe" (0=novo, 1=ja existe)
  4. Importa APENAS os registros novos

Uso:
  python3 scripts/importar_solicitacoes_excel.py planilha.xlsx                  (DRY-RUN + CSV)
  python3 scripts/importar_solicitacoes_excel.py planilha.xlsx --executar       (IMPORTA novos)
  python3 scripts/importar_solicitacoes_excel.py planilha.xlsx --usuario 5      (define usuario)
  python3 scripts/importar_solicitacoes_excel.py planilha.xlsx --sem-ajuste-ano (nao subtrai 1 ano da competencia)
"""
import os
import sys
import argparse
import csv
from datetime import datetime

import pandas as pd
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

# =============================================================================
# CONFIG
# =============================================================================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, '.env'))

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

if not all([DB_USER, DB_HOST, DB_NAME]):
    print("ERRO: Variaveis de banco ausentes no .env")
    sys.exit(1)

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
ENGINE = create_engine(DATABASE_URI, echo=False)

# =============================================================================
# MAPEAMENTOS (texto Excel -> ID no banco)
# =============================================================================
MAPA_ETAPA = {
    'Solicitação Criada': 1,
    'Documentação Solicitada': 2,
    'Liquidado': 5,
    'Pago': 6,
    'NF Atestada': 11,
    'Fiscais Notificados': 12,
    'Contrato Fiscalizado': 13,
    'Atestado pelo Controle Interno': 14,
    'Solicitação da NF': 15,
}

MAPA_TIPO_PAGAMENTO = {
    'Pagamento Regular': 1,
    'DEA: Indenizatório': 2,
    'DEA: Pagamento Regular': 3,
}

MAPA_STATUS_EMPENHO = {
    'Empenho Atendido': 2,
    'Empenho Não Solicitado': 3,
}

ID_CAIXA_SEI = '110006445'
ID_USUARIO_SOLICITANTE = 3


# =============================================================================
# FUNCOES AUXILIARES
# =============================================================================
def parse_args():
    parser = argparse.ArgumentParser(description='Importar solicitacoes de pagamento do Excel')
    parser.add_argument('arquivo', help='Caminho do arquivo Excel (.xlsx)')
    parser.add_argument('--executar', action='store_true', help='Executa a importacao (sem flag = DRY-RUN + CSV)')
    parser.add_argument('--usuario', type=int, default=ID_USUARIO_SOLICITANTE, help='ID do usuario solicitante (default: 3)')
    parser.add_argument('--sem-ajuste-ano', action='store_true', help='Nao subtrai 1 ano da competencia')
    return parser.parse_args()


def normalizar_colunas(df):
    """Normaliza nomes de colunas para lidar com encoding variavel."""
    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if 'compet' in cl:
            col_map[c] = 'Competencia'
        elif 'digo contrato' in cl or 'codigo contrato' in cl:
            col_map[c] = 'Codigo Contrato'
        elif 'data solicita' in cl:
            col_map[c] = 'Data Solicitacao'
        elif 'protocolo' in cl:
            col_map[c] = 'Protocolo SEI'
        elif 'id procedimento' in cl:
            col_map[c] = 'ID Procedimento SEI'
        elif 'tipo pagamento' in cl:
            col_map[c] = 'Tipo Pagamento'
        elif 'status empenho' in cl:
            col_map[c] = 'Status Empenho'
        elif 'valor pagamento' in cl:
            col_map[c] = 'Valor Pagamento'
        elif 'link sei' in cl:
            col_map[c] = 'Link SEI'
        elif 'etapa atual' in cl:
            col_map[c] = 'Etapa Atual'
        elif 'status geral' in cl:
            col_map[c] = 'Status Geral'
        elif 'tempo total' in cl:
            col_map[c] = 'Tempo Total'
    df.rename(columns=col_map, inplace=True)
    return df


def formatar_competencia(val, ajustar_ano=True):
    """Converte datetime para MM/YYYY. Se ajustar_ano=True, subtrai 1 ano."""
    if pd.isna(val):
        return None
    if isinstance(val, (datetime, pd.Timestamp)):
        ano = val.year - 1 if ajustar_ano else val.year
        return f"{val.month:02d}/{ano}"
    # Se ja for string no formato MM/YYYY, retorna como esta
    s = str(val).strip()
    if '/' in s and len(s) <= 7:
        return s
    return s


def formatar_codigo_contrato(val):
    """Garante que codigo_contrato seja string (ex: 23000548)."""
    if pd.isna(val):
        return None
    return str(int(val))


def buscar_protocolos_existentes():
    """Retorna set com todos os protocolos SEI que ja existem no banco."""
    with ENGINE.connect() as conn:
        result = conn.execute(text(
            "SELECT protocolo_gerado_sei FROM sis_solicitacoes WHERE protocolo_gerado_sei IS NOT NULL"
        ))
        return {str(r[0]).strip() for r in result if r[0]}


def buscar_contratos():
    """Retorna dict codigo -> numeroOriginal de todos os contratos."""
    with ENGINE.connect() as conn:
        result = conn.execute(text("SELECT codigo, numeroOriginal FROM contratos"))
        return {str(r[0]): r[1] for r in result}


def gerar_csv_analise(df, protocolos_db, caminho_csv):
    """Gera CSV com coluna 'existe' indicando se o protocolo ja esta no banco."""
    col_prot = 'Protocolo SEI'
    registros = []
    for _, row in df.iterrows():
        prot = str(row.get(col_prot, '')).strip()
        existe = 1 if prot in protocolos_db else 0
        registros.append({
            'Protocolo SEI': prot,
            'Codigo Contrato': row.get('Codigo Contrato', ''),
            'Competencia': row.get('Competencia', ''),
            'Etapa Atual': row.get('Etapa Atual', ''),
            'Status Geral': row.get('Status Geral', ''),
            'existe': existe,
        })

    with open(caminho_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=registros[0].keys(), delimiter=';')
        writer.writeheader()
        writer.writerows(registros)

    total = len(registros)
    existentes = sum(1 for r in registros if r['existe'] == 1)
    novos = total - existentes
    return total, existentes, novos


def validar_mapeamentos(df):
    """Valida se todos os valores texto tem mapeamento para ID."""
    erros = []
    for idx, row in df.iterrows():
        etapa = row.get('Etapa Atual', '')
        if pd.notna(etapa) and etapa and etapa not in MAPA_ETAPA:
            erros.append(f"  Linha {idx+2}: Etapa desconhecida '{etapa}'")
        tipo = row.get('Tipo Pagamento', '')
        if pd.notna(tipo) and tipo and tipo not in MAPA_TIPO_PAGAMENTO:
            erros.append(f"  Linha {idx+2}: Tipo Pagamento desconhecido '{tipo}'")
        status_emp = row.get('Status Empenho', '')
        if pd.notna(status_emp) and status_emp and status_emp not in MAPA_STATUS_EMPENHO:
            erros.append(f"  Linha {idx+2}: Status Empenho desconhecido '{status_emp}'")
    return erros


# =============================================================================
# MAIN
# =============================================================================
def main():
    args = parse_args()
    executar = args.executar
    usuario_id = args.usuario
    ajustar_ano = not args.sem_ajuste_ano
    arquivo = args.arquivo

    if not os.path.exists(arquivo):
        print(f"ERRO: Arquivo nao encontrado: {arquivo}")
        sys.exit(1)

    modo = "EXECUCAO" if executar else "DRY-RUN"
    print("=" * 60)
    print(f"  Importacao de Solicitacoes - {modo}")
    print(f"  Arquivo: {os.path.basename(arquivo)}")
    print(f"  Ajuste ano competencia: {'SIM (-1 ano)' if ajustar_ano else 'NAO'}")
    print("=" * 60)

    # 1. Ler e normalizar Excel
    df = pd.read_excel(arquivo)
    df = normalizar_colunas(df)
    print(f"\n[1/5] {len(df)} registros lidos do Excel.")

    # 2. Validar mapeamentos
    erros = validar_mapeamentos(df)
    if erros:
        print("\n[ERRO] Valores nao mapeados encontrados:")
        for e in erros:
            print(e)
        sys.exit(1)
    print("[OK] Todos os valores mapeados corretamente.")

    # 3. Verificar protocolos existentes no banco
    print(f"\n[2/5] Verificando protocolos existentes no banco...")
    protocolos_db = buscar_protocolos_existentes()
    contratos_db = buscar_contratos()

    # 4. Gerar CSV de analise
    nome_base = os.path.splitext(os.path.basename(arquivo))[0]
    caminho_csv = os.path.join(os.path.dirname(arquivo) or '.', f'{nome_base}_analise.csv')
    total, existentes, novos = gerar_csv_analise(df, protocolos_db, caminho_csv)

    print(f"\n[3/5] Analise de duplicatas:")
    print(f"  - Total no Excel:    {total}")
    print(f"  - Ja existem no DB:  {existentes}")
    print(f"  - Novos (a importar): {novos}")
    print(f"  - CSV gerado: {caminho_csv}")

    if novos == 0:
        print(f"\n{'='*60}")
        print("  Todos os registros ja existem no banco. Nada a importar.")
        print(f"{'='*60}")
        return

    # 5. Preparar registros para importacao (apenas novos)
    print(f"\n[4/5] Preparando {novos} registros novos para importacao...")

    codigos_excel = set(df['Codigo Contrato'].apply(formatar_codigo_contrato).dropna())
    faltantes = codigos_excel - set(contratos_db.keys())
    if faltantes:
        print(f"  [AVISO] {len(faltantes)} contratos NAO existem no banco:")
        for c in sorted(faltantes):
            print(f"    - {c}")
        print("    Esses registros serao IGNORADOS.")

    solicitacoes = []
    historicos = []
    ignorados_contrato = 0
    ignorados_existente = 0

    for _, row in df.iterrows():
        # Pular protocolos que ja existem
        prot = str(row.get('Protocolo SEI', '')).strip()
        if prot in protocolos_db:
            ignorados_existente += 1
            continue

        cod = formatar_codigo_contrato(row.get('Codigo Contrato'))
        if not cod or cod not in contratos_db:
            ignorados_contrato += 1
            continue

        etapa_id = MAPA_ETAPA.get(row.get('Etapa Atual', ''), 1)
        tipo_pag_id = MAPA_TIPO_PAGAMENTO.get(row.get('Tipo Pagamento', ''))
        status_emp_id = MAPA_STATUS_EMPENHO.get(row.get('Status Empenho', ''), 3)
        competencia = formatar_competencia(row.get('Competencia'), ajustar_ano=ajustar_ano)

        data_sol = row.get('Data Solicitacao')
        if pd.isna(data_sol):
            data_sol = datetime.now()

        id_proc_sei = str(int(row['ID Procedimento SEI'])) if pd.notna(row.get('ID Procedimento SEI')) else None

        link_sei = row.get('Link SEI', '')
        if pd.isna(link_sei):
            link_sei = None

        status_geral = row.get('Status Geral', 'ABERTO')
        if pd.isna(status_geral):
            status_geral = 'ABERTO'

        tempo_total = row.get('Tempo Total')
        if pd.isna(tempo_total):
            tempo_total = None

        # Gerar especificacao e descricao no padrao do sistema
        num_original = contratos_db.get(cod, '')
        if num_original and competencia:
            especificacao = f"PAGAMENTO DE CONTRATO {num_original}--{cod}-{competencia}"
        elif num_original:
            especificacao = f"PAGAMENTO DE CONTRATO {num_original}--{cod}"
        else:
            especificacao = f"PAGAMENTO DE CONTRATO {cod}"

        descricao = f"Solicitação de Pagamento - {competencia}" if competencia else "Solicitação de Pagamento"

        solicitacoes.append({
            'codigo_contrato': cod,
            'id_usuario_solicitante': usuario_id,
            'etapa_atual_id': etapa_id,
            'status_empenho_id': status_emp_id,
            'id_tipo_pagamento': tipo_pag_id,
            'data_solicitacao': data_sol,
            'protocolo_gerado_sei': prot,
            'id_procedimento_sei': id_proc_sei,
            'link_processo_sei': link_sei,
            'competencia': competencia,
            'especificacao': especificacao,
            'descricao': descricao,
            'id_caixa_sei': ID_CAIXA_SEI,
            'status_geral': status_geral,
            'tempo_total': tempo_total,
            'criado_em_lote': True,
        })

        historicos.append({
            'id_etapa_anterior': None,
            'id_etapa_nova': etapa_id,
            'id_usuario_responsavel': usuario_id,
            'data_movimentacao': data_sol,
            'comentario': f'Importado via Excel ({os.path.basename(arquivo)})',
        })

    print(f"\n[5/5] Resumo final:")
    print(f"  - Novos a inserir:           {len(solicitacoes)}")
    print(f"  - Ignorados (ja existem):    {ignorados_existente}")
    print(f"  - Ignorados (sem contrato):  {ignorados_contrato}")
    print(f"  - Historicos a criar:        {len(historicos)}")

    # Amostra
    if solicitacoes:
        print(f"\n[AMOSTRA] Primeiros 3 registros novos:")
        for i, sol in enumerate(solicitacoes[:3]):
            print(f"  {i+1}. prot={sol['protocolo_gerado_sei']}")
            print(f"     contrato={sol['codigo_contrato']}, etapa={sol['etapa_atual_id']}, "
                  f"tipo_pag={sol['id_tipo_pagamento']}, status_emp={sol['status_empenho_id']}")
            print(f"     comp={sol['competencia']}, status={sol['status_geral']}")
            print(f"     espec={sol['especificacao']}")

    if not executar:
        print(f"\n{'='*60}")
        print("  DRY-RUN: Nenhuma alteracao feita no banco.")
        print("  Execute com --executar para inserir os dados.")
        print(f"{'='*60}")
        return

    # =====================================================================
    # EXECUTAR INSERTS
    # =====================================================================
    print(f"\n[EXEC] Inserindo {len(solicitacoes)} solicitacoes...")

    insert_sol_sql = text("""
        INSERT INTO sis_solicitacoes (
            codigo_contrato, id_usuario_solicitante, etapa_atual_id,
            status_empenho_id, id_tipo_pagamento, data_solicitacao,
            protocolo_gerado_sei, id_procedimento_sei, link_processo_sei,
            competencia, especificacao, descricao, id_caixa_sei,
            status_geral, tempo_total, criado_em_lote
        ) VALUES (
            :codigo_contrato, :id_usuario_solicitante, :etapa_atual_id,
            :status_empenho_id, :id_tipo_pagamento, :data_solicitacao,
            :protocolo_gerado_sei, :id_procedimento_sei, :link_processo_sei,
            :competencia, :especificacao, :descricao, :id_caixa_sei,
            :status_geral, :tempo_total, :criado_em_lote
        )
    """)

    insert_hist_sql = text("""
        INSERT INTO sis_historico_movimentacoes (
            id_solicitacao, id_etapa_anterior, id_etapa_nova,
            id_usuario_responsavel, data_movimentacao, comentario
        ) VALUES (
            :id_solicitacao, :id_etapa_anterior, :id_etapa_nova,
            :id_usuario_responsavel, :data_movimentacao, :comentario
        )
    """)

    inseridos = 0
    with ENGINE.begin() as conn:
        for sol, hist in zip(solicitacoes, historicos):
            result = conn.execute(insert_sol_sql, sol)
            new_id = result.lastrowid
            hist['id_solicitacao'] = new_id
            conn.execute(insert_hist_sql, hist)
            inseridos += 1
            if inseridos % 50 == 0:
                print(f"  ... {inseridos}/{len(solicitacoes)} inseridos")

    print(f"\n[OK] {inseridos} solicitacoes inseridas com sucesso!")
    print(f"[OK] {inseridos} historicos de movimentacao criados.")
    print(f"\n{'='*60}")
    print("  Importacao concluida!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
