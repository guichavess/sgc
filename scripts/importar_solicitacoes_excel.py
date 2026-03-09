"""
Script para importar solicitações de pagamento a partir do Excel (ValoresContratos.xlsx).

Insere 219 registros novos em sis_solicitacoes + sis_historico_movimentacoes.

Uso:
    python scripts/importar_solicitacoes_excel.py                    (DRY-RUN)
    python scripts/importar_solicitacoes_excel.py --executar         (EXECUTA)
    python scripts/importar_solicitacoes_excel.py --executar --usuario 5   (define usuario ID)
"""
import os
import sys
import argparse
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

EXCEL_FILE = os.path.join(base_dir, 'ValoresContratos.xlsx')

# =============================================================================
# MAPEAMENTOS (texto Excel → ID no banco)
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
    'Pagamento Regular': 1,        # id=1
    'DEA: Indenizatório': 2,       # id=2
    'DEA: Pagamento Regular': 3,   # id=3
}

MAPA_STATUS_EMPENHO = {
    'Empenho Atendido': 2,         # ATENDIDO
    'Empenho Não Solicitado': 3,   # NAO_SOLICITADO
}

# Status geral: valores já batem com o banco (ABERTO, PAGO, AGUARDANDO_ASSINATURA, EM LIQUIDAÇÃO)


ID_CAIXA_SEI = '110006445'
ID_USUARIO_SOLICITANTE = 3


def parse_args():
    parser = argparse.ArgumentParser(description='Importar solicitacoes do Excel')
    parser.add_argument('--executar', action='store_true', help='Executa de fato (sem flag = DRY-RUN)')
    parser.add_argument('--usuario', type=int, default=ID_USUARIO_SOLICITANTE, help='ID do usuario solicitante (default: 3)')
    parser.add_argument('--arquivo', type=str, default=EXCEL_FILE, help='Caminho do Excel')
    return parser.parse_args()


def ler_excel(caminho):
    df = pd.read_excel(caminho)
    # Normalizar nomes de colunas (encoding issues)
    col_map = {}
    for c in df.columns:
        if 'Compet' in c:
            col_map[c] = 'Competencia'
        elif 'digo Contrato' in c:
            col_map[c] = 'Codigo Contrato'
        elif 'Data Solicita' in c:
            col_map[c] = 'Data Solicitacao'
        elif 'Protocolo' in c:
            col_map[c] = 'Protocolo SEI'
        elif 'ID Procedimento' in c:
            col_map[c] = 'ID Procedimento SEI'
        elif 'Tipo Pagamento' in c:
            col_map[c] = 'Tipo Pagamento'
        elif 'Status Empenho' in c:
            col_map[c] = 'Status Empenho'
        elif 'Valor Pagamento' in c:
            col_map[c] = 'Valor Pagamento'
        elif 'Link SEI' in c:
            col_map[c] = 'Link SEI'
        elif 'Etapa Atual' in c:
            col_map[c] = 'Etapa Atual'
        elif 'Status Geral' in c:
            col_map[c] = 'Status Geral'
        elif 'Tempo Total' in c:
            col_map[c] = 'Tempo Total'
    df.rename(columns=col_map, inplace=True)
    return df


def formatar_competencia(val):
    """Converte datetime (2026-09-25) → '09/2026' ou retorna None."""
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.strftime('%m/%Y')
    if isinstance(val, pd.Timestamp):
        return val.strftime('%m/%Y')
    return str(val)


def formatar_codigo_contrato(val):
    """Garante que codigo_contrato seja string (ex: 23000548)."""
    if pd.isna(val):
        return None
    return str(int(val))


def main():
    args = parse_args()
    executar = args.executar
    usuario_id = args.usuario

    modo = "EXECUCAO" if executar else "DRY-RUN"
    print("=" * 60)
    print(f"  Importacao de Solicitacoes - {modo}")
    print("=" * 60)

    # Ler Excel
    df = ler_excel(args.arquivo)
    print(f"\n[INFO] {len(df)} registros lidos do Excel.")

    # Validar mapeamentos
    erros = []
    for idx, row in df.iterrows():
        etapa = row.get('Etapa Atual', '')
        if etapa and etapa not in MAPA_ETAPA:
            erros.append(f"  Linha {idx+2}: Etapa desconhecida '{etapa}'")

        tipo = row.get('Tipo Pagamento', '')
        if tipo and tipo not in MAPA_TIPO_PAGAMENTO:
            erros.append(f"  Linha {idx+2}: Tipo Pagamento desconhecido '{tipo}'")

        status_emp = row.get('Status Empenho', '')
        if status_emp and status_emp not in MAPA_STATUS_EMPENHO:
            erros.append(f"  Linha {idx+2}: Status Empenho desconhecido '{status_emp}'")

    if erros:
        print("\n[ERRO] Valores nao mapeados encontrados:")
        for e in erros:
            print(e)
        sys.exit(1)

    print("[OK] Todos os valores mapeados corretamente.")

    # Verificar contratos existentes no banco e buscar numeroOriginal
    with ENGINE.connect() as conn:
        result = conn.execute(text("SELECT codigo, numeroOriginal FROM contratos"))
        contratos_db = {}
        for r in result:
            contratos_db[str(r[0])] = r[1]  # codigo → numeroOriginal

    codigos_excel = set(df['Codigo Contrato'].apply(formatar_codigo_contrato).dropna())
    faltantes = codigos_excel - set(contratos_db.keys())
    if faltantes:
        print(f"\n[AVISO] {len(faltantes)} contratos do Excel NAO existem no banco:")
        for c in sorted(faltantes):
            print(f"  - {c}")
        print("  Esses registros serao IGNORADOS na importacao.")

    # Preparar inserts
    solicitacoes = []
    historicos = []
    ignorados = 0

    for idx, row in df.iterrows():
        cod = formatar_codigo_contrato(row.get('Codigo Contrato'))
        if not cod or cod not in contratos_db:
            ignorados += 1
            continue

        etapa_id = MAPA_ETAPA.get(row.get('Etapa Atual', ''), 1)
        tipo_pag_id = MAPA_TIPO_PAGAMENTO.get(row.get('Tipo Pagamento', ''))
        status_emp_id = MAPA_STATUS_EMPENHO.get(row.get('Status Empenho', ''), 3)
        competencia = formatar_competencia(row.get('Competencia'))
        data_sol = row.get('Data Solicitacao')
        if pd.isna(data_sol):
            data_sol = datetime.now()
        protocolo = row.get('Protocolo SEI', '')
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
            'protocolo_gerado_sei': protocolo,
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

        # Historico: movimentacao inicial (etapa 1 → etapa atual)
        historicos.append({
            'id_etapa_anterior': None,
            'id_etapa_nova': etapa_id,
            'id_usuario_responsavel': usuario_id,
            'data_movimentacao': data_sol,
            'comentario': 'Importado via Excel (ValoresContratos.xlsx)',
        })

    print(f"\n[INFO] Resumo:")
    print(f"  - Registros a inserir: {len(solicitacoes)}")
    print(f"  - Registros ignorados (contrato inexistente): {ignorados}")
    print(f"  - Historicos a criar: {len(historicos)}")

    # Amostra
    print(f"\n[AMOSTRA] Primeiros 3 registros:")
    for i, sol in enumerate(solicitacoes[:3]):
        print(f"  {i+1}. contrato={sol['codigo_contrato']}, etapa={sol['etapa_atual_id']}, "
              f"tipo_pag={sol['id_tipo_pagamento']}, status_emp={sol['status_empenho_id']}, "
              f"comp={sol['competencia']}, status={sol['status_geral']}")
        print(f"     espec={sol['especificacao']}")
        print(f"     descr={sol['descricao']}, caixa_sei={sol['id_caixa_sei']}")

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
        for i, (sol, hist) in enumerate(zip(solicitacoes, historicos)):
            # Insert solicitacao
            result = conn.execute(insert_sol_sql, sol)
            new_id = result.lastrowid

            # Insert historico com o ID da solicitacao recem-criada
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
