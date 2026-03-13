"""
Importa dados do dump do sistema original (sei.processo_enviado) para cgfr_processo_enviado.

Etapas:
1. ALTER TABLE: adiciona colunas faltantes no cgfr_processo_enviado
2. Carrega dump SQL em tabela temporaria (dump_processo_enviado)
3. Mapeia FKs: natdespesas.codigo → .id, fonte.codigo → class_fonte.id, acao via codigo
4. INSERT INTO cgfr_processo_enviado com mapeamento

Uso:
    python scripts/importar_dump_cgfr.py                          # DRY-RUN (mostra o que faria)
    python scripts/importar_dump_cgfr.py --executar               # Executa de verdade
    python scripts/importar_dump_cgfr.py --executar --truncar     # Limpa tabela antes de importar
"""
import argparse
import os
import re
import sys

# Ajusta path para importar app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import create_app
from app.extensions import db


DUMP_PATH = r'C:\Users\guilh\OneDrive\Documentos\dumps\Dump20260313.sql'

# Colunas que precisam ser adicionadas ao cgfr_processo_enviado
COLUNAS_NOVAS = [
    ('link_acesso', 'VARCHAR(255) DEFAULT NULL'),
    ('id_unidade_geradora', 'VARCHAR(255) DEFAULT NULL'),
    ('geracao_sigla', 'VARCHAR(255) DEFAULT NULL'),
    ('geracao_data', 'DATETIME DEFAULT NULL'),
    ('geracao_descricao', 'TEXT DEFAULT NULL'),
    ('usuario_gerador', 'VARCHAR(255) DEFAULT NULL'),
    ('ultimo_andamento_sigla', 'VARCHAR(255) DEFAULT NULL'),
    ('ultimo_andamento_descricao', 'TEXT DEFAULT NULL'),
    ('ultimo_andamento_data', 'DATETIME DEFAULT NULL'),
    ('ultimo_andamento_usuario', 'VARCHAR(255) DEFAULT NULL'),
]


def get_existing_columns(conn):
    """Retorna set de nomes de colunas existentes na tabela."""
    result = conn.execute(db.text('DESCRIBE cgfr_processo_enviado'))
    return {row[0] for row in result}


def add_missing_columns(conn, dry_run=True):
    """Adiciona colunas faltantes ao cgfr_processo_enviado."""
    existing = get_existing_columns(conn)
    added = 0

    for col_name, col_def in COLUNAS_NOVAS:
        if col_name not in existing:
            sql = f'ALTER TABLE cgfr_processo_enviado ADD COLUMN `{col_name}` {col_def}'
            if dry_run:
                print(f'  [DRY-RUN] {sql}')
            else:
                conn.execute(db.text(sql))
                print(f'  [OK] Adicionada coluna: {col_name}')
            added += 1
        else:
            print(f'  [SKIP] Coluna {col_name} ja existe')

    return added


def build_acao_map(conn):
    """Constroi mapa: dump acao.ID → SGC acao.id, via codigo extraido do titulo."""
    # Dump acao data: (1, '2000 - ADMINISTRACAO...'), (2, '2500 - GESTAO...')
    # Extrair codigo do inicio do texto
    dump_acoes = {
        1: 2000, 2: 2500, 3: 5047, 4: 6131, 5: 6132,
        6: 6133, 7: 6135, 8: 6187, 9: 7100, 10: 2919, 11: 6149,
    }

    # Buscar SGC acao: codigo → id
    result = conn.execute(db.text('SELECT id, codigo FROM acao'))
    sgc_map = {row[1]: row[0] for row in result}  # codigo → id

    # dump_acao_id → sgc_acao_id
    mapping = {}
    for dump_id, codigo in dump_acoes.items():
        if codigo in sgc_map:
            mapping[dump_id] = sgc_map[codigo]
        else:
            print(f'  [WARN] Acao codigo={codigo} (dump ID={dump_id}) nao encontrada no SGC')

    return mapping


def build_natdespesa_map(conn):
    """Constroi mapa: natdespesas.codigo → natdespesas.id no SGC."""
    result = conn.execute(db.text('SELECT id, codigo FROM natdespesas'))
    return {row[1]: row[0] for row in result}  # codigo → id


def build_fonte_map(conn):
    """Constroi mapa: fonte.codigo → class_fonte.id no SGC."""
    result = conn.execute(db.text('SELECT id, codigo FROM class_fonte'))
    return {int(row[1]): row[0] for row in result}  # codigo → id


def load_dump_data(dump_path, table_name='processo_enviado'):
    """Extrai INSERT VALUES de uma tabela do dump SQL."""
    print(f'\nLendo dump: {dump_path} (tabela: {table_name})')

    with open(dump_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Encontrar o bloco INSERT INTO `<table_name>`
    pattern = rf"INSERT INTO `{table_name}` VALUES\s*(.+?);\s*$"
    match = re.search(pattern, content, re.DOTALL | re.MULTILINE)

    if not match:
        print(f'  [ERRO] Nao encontrou INSERT INTO {table_name} no dump!')
        return ''

    values_str = match.group(1)
    print(f'  Bloco INSERT encontrado ({len(values_str)} chars)')

    return values_str


def import_via_temp_table(conn, values_str, acao_map, nat_map, fonte_map, dry_run=True, truncar=False):
    """Importa dados via tabela temporaria com mapeamento de FKs."""

    # 1. Criar tabela temporaria com estrutura do dump
    print('\n--- Criando tabela temporaria ---')
    drop_sql = 'DROP TABLE IF EXISTS _dump_processo_enviado'
    create_sql = """
    CREATE TEMPORARY TABLE _dump_processo_enviado (
        processo_formatado VARCHAR(255) NOT NULL,
        link_acesso VARCHAR(255),
        especificacao VARCHAR(255),
        id_unidade_geradora VARCHAR(255),
        geracao_sigla VARCHAR(255),
        geracao_data DATETIME,
        geracao_descricao TEXT,
        usuario_gerador VARCHAR(255),
        ultimo_andamento_sigla VARCHAR(255),
        ultimo_andamento_descricao TEXT,
        ultimo_andamento_data DATETIME,
        ultimo_andamento_usuario VARCHAR(255),
        natureza_despesa INT,
        fonte INT,
        acao INT,
        objeto_do_pedido TEXT,
        necessidade TEXT,
        deliberacao TEXT,
        tipo_despesa VARCHAR(50),
        data_da_reuniao DATE,
        valor_solicitado DECIMAL(12,2),
        valor_aprovado DECIMAL(12,2),
        tramitado_sead_cgfr VARCHAR(50),
        recebido_cgfr INT,
        data_recebido_cgfr VARCHAR(50),
        devolvido_cgfr_sead INT,
        data_devolvido_cgfr_sead VARCHAR(50),
        tipo_processo VARCHAR(100),
        fornecedor VARCHAR(255),
        observacao TEXT,
        possui_reserva VARCHAR(100),
        valor_reserva DECIMAL(12,2),
        data_inclusao DATETIME,
        nivel_prioridade VARCHAR(10),
        PRIMARY KEY (processo_formatado)
    )
    """

    if dry_run:
        print('  [DRY-RUN] DROP + CREATE TEMPORARY TABLE _dump_processo_enviado')
    else:
        conn.execute(db.text(drop_sql))
        conn.execute(db.text(create_sql))
        print('  [OK] Tabela temporaria criada')

    # 2. Inserir dados do dump na temp
    insert_sql = f'INSERT INTO _dump_processo_enviado VALUES {values_str}'
    if dry_run:
        print(f'  [DRY-RUN] INSERT INTO _dump_processo_enviado VALUES ... ({len(values_str)} chars)')
    else:
        conn.execute(db.text(insert_sql))
        count = conn.execute(db.text('SELECT COUNT(*) FROM _dump_processo_enviado')).scalar()
        print(f'  [OK] {count} registros inseridos na tabela temporaria')

    # 3. Truncar tabela destino se solicitado
    if truncar:
        if dry_run:
            print('  [DRY-RUN] TRUNCATE TABLE cgfr_processo_enviado')
        else:
            conn.execute(db.text('DELETE FROM cgfr_processo_enviado'))
            print('  [OK] Tabela cgfr_processo_enviado limpa')

    # 4. Construir e executar INSERT com mapeamento
    # Acao: precisa mapear dump_acao_id → sgc_acao_id via CASE
    acao_cases = ' '.join(f'WHEN {k} THEN {v}' for k, v in acao_map.items())
    acao_expr = f'CASE d.acao {acao_cases} ELSE NULL END' if acao_cases else 'NULL'

    insert_mapped = f"""
    INSERT INTO cgfr_processo_enviado (
        processo_formatado, link_acesso, especificacao, tipo_processo,
        data_hora_processo,
        id_unidade_geradora, geracao_sigla, geracao_data, geracao_descricao, usuario_gerador,
        ultimo_andamento_sigla, ultimo_andamento_descricao, ultimo_andamento_data, ultimo_andamento_usuario,
        tramitado_sead_cgfr, recebido_cgfr, data_recebido_cgfr,
        devolvido_cgfr_sead, data_devolvido_cgfr_sead,
        natureza_despesa_id, fonte_id, acao_id,
        fornecedor, objeto_do_pedido, necessidade, deliberacao,
        tipo_despesa, valor_solicitado, valor_aprovado,
        data_da_reuniao, observacao,
        possui_reserva, valor_reserva, nivel_prioridade,
        data_inclusao
    )
    SELECT
        d.processo_formatado,
        d.link_acesso,
        d.especificacao,
        d.tipo_processo,
        d.geracao_data,
        d.id_unidade_geradora,
        d.geracao_sigla,
        d.geracao_data,
        d.geracao_descricao,
        d.usuario_gerador,
        d.ultimo_andamento_sigla,
        d.ultimo_andamento_descricao,
        d.ultimo_andamento_data,
        d.ultimo_andamento_usuario,
        d.tramitado_sead_cgfr,
        d.recebido_cgfr,
        d.data_recebido_cgfr,
        d.devolvido_cgfr_sead,
        d.data_devolvido_cgfr_sead,
        n.id AS natureza_despesa_id,
        f.id AS fonte_id,
        {acao_expr} AS acao_id,
        d.fornecedor,
        d.objeto_do_pedido,
        d.necessidade,
        d.deliberacao,
        d.tipo_despesa,
        d.valor_solicitado,
        d.valor_aprovado,
        d.data_da_reuniao,
        d.observacao,
        CASE WHEN d.possui_reserva IN ('1', 'Sim', 'sim') THEN 1 ELSE 0 END,
        CAST(d.valor_reserva AS CHAR(30)),
        d.nivel_prioridade,
        d.data_inclusao
    FROM _dump_processo_enviado d
    LEFT JOIN natdespesas n ON n.codigo = d.natureza_despesa
    LEFT JOIN class_fonte f ON CAST(f.codigo AS UNSIGNED) = d.fonte
    ON DUPLICATE KEY UPDATE
        link_acesso = VALUES(link_acesso),
        especificacao = VALUES(especificacao),
        tipo_processo = VALUES(tipo_processo),
        data_hora_processo = VALUES(data_hora_processo),
        id_unidade_geradora = VALUES(id_unidade_geradora),
        geracao_sigla = VALUES(geracao_sigla),
        geracao_data = VALUES(geracao_data),
        geracao_descricao = VALUES(geracao_descricao),
        usuario_gerador = VALUES(usuario_gerador),
        ultimo_andamento_sigla = VALUES(ultimo_andamento_sigla),
        ultimo_andamento_descricao = VALUES(ultimo_andamento_descricao),
        ultimo_andamento_data = VALUES(ultimo_andamento_data),
        ultimo_andamento_usuario = VALUES(ultimo_andamento_usuario),
        tramitado_sead_cgfr = VALUES(tramitado_sead_cgfr),
        recebido_cgfr = VALUES(recebido_cgfr),
        data_recebido_cgfr = VALUES(data_recebido_cgfr),
        devolvido_cgfr_sead = VALUES(devolvido_cgfr_sead),
        data_devolvido_cgfr_sead = VALUES(data_devolvido_cgfr_sead),
        natureza_despesa_id = VALUES(natureza_despesa_id),
        fonte_id = VALUES(fonte_id),
        acao_id = VALUES(acao_id),
        fornecedor = VALUES(fornecedor),
        objeto_do_pedido = VALUES(objeto_do_pedido),
        necessidade = VALUES(necessidade),
        deliberacao = VALUES(deliberacao),
        tipo_despesa = VALUES(tipo_despesa),
        valor_solicitado = VALUES(valor_solicitado),
        valor_aprovado = VALUES(valor_aprovado),
        data_da_reuniao = VALUES(data_da_reuniao),
        observacao = VALUES(observacao),
        possui_reserva = VALUES(possui_reserva),
        valor_reserva = VALUES(valor_reserva),
        nivel_prioridade = VALUES(nivel_prioridade),
        data_inclusao = VALUES(data_inclusao)
    """

    if dry_run:
        print(f'  [DRY-RUN] INSERT INTO cgfr_processo_enviado SELECT ... FROM _dump_processo_enviado (com JOINs)')
        # Show mapping stats
        print(f'\n--- Mapeamento de FKs ---')
        print(f'  Acao: {len(acao_map)} mapeamentos (dump ID -> SGC ID)')
        for k, v in sorted(acao_map.items()):
            print(f'    dump.acao={k} -> sgc.acao.id={v}')
        print(f'  Natdespesas: {len(nat_map)} codigos mapeados')
        print(f'  Fontes: {len(fonte_map)} codigos mapeados')
    else:
        result = conn.execute(db.text(insert_mapped))
        print(f'  [OK] {result.rowcount} registros importados/atualizados')

    # 5. Cleanup
    if not dry_run:
        conn.execute(db.text('DROP TABLE IF EXISTS _dump_processo_enviado'))


def enrich_from_tabela_nova(conn, dump_path, acao_map, dry_run=True):
    """Enriquece cgfr_processo_enviado com dados SEI da tabela_nova do dump.
    A tabela_nova tem dados completos (link_acesso, objeto_do_pedido, fornecedor, etc.)
    que processo_enviado nao possui para muitos registros.
    """
    values_str = load_dump_data(dump_path, 'tabela_nova')
    if not values_str:
        print('  [SKIP] tabela_nova nao encontrada no dump')
        return

    # Criar temp table com estrutura de tabela_nova
    drop_sql = 'DROP TABLE IF EXISTS _dump_tabela_nova'
    create_sql = """
    CREATE TEMPORARY TABLE _dump_tabela_nova (
        idprotocol INT,
        numprocesso VARCHAR(255),
        sector_id INT,
        tramitado_sead_cgfr VARCHAR(50),
        recebido_cgfr INT,
        data_recebido_cgfr VARCHAR(50),
        devolvido_cgfr_sead INT,
        data_devolvido_cgfr_sead VARCHAR(50),
        processo_formatado VARCHAR(255),
        link_acesso VARCHAR(255),
        especificacao VARCHAR(255),
        id_unidade_geradora VARCHAR(255),
        geracao_sigla VARCHAR(255),
        geracao_data DATETIME,
        geracao_descricao TEXT,
        usuario_gerador VARCHAR(255),
        ultimo_andamento_sigla VARCHAR(255),
        ultimo_andamento_descricao TEXT,
        ultimo_andamento_data DATETIME,
        ultimo_andamento_usuario VARCHAR(255),
        natureza_despesa INT,
        fonte INT,
        acao INT,
        objeto_do_pedido TEXT,
        necessidade TEXT,
        deliberacao TEXT,
        tipo_despesa VARCHAR(50),
        data_da_reuniao DATE,
        valor_solicitado DECIMAL(12,2),
        valor_aprovado DECIMAL(12,2)
    )
    """

    if dry_run:
        print('  [DRY-RUN] CREATE TEMPORARY TABLE _dump_tabela_nova')
        print('  [DRY-RUN] INSERT INTO _dump_tabela_nova VALUES ...')
        print('  [DRY-RUN] UPDATE cgfr_processo_enviado com dados SEI da tabela_nova')
        return

    conn.execute(db.text(drop_sql))
    conn.execute(db.text(create_sql))

    insert_sql = f'INSERT INTO _dump_tabela_nova VALUES {values_str}'
    conn.execute(db.text(insert_sql))
    count = conn.execute(db.text('SELECT COUNT(*) FROM _dump_tabela_nova')).scalar()
    print(f'  [OK] {count} registros na tabela_nova temporaria')

    # Construir CASE para acao mapping
    acao_cases = ' '.join(f'WHEN {k} THEN {v}' for k, v in acao_map.items())
    acao_expr = f'CASE t.acao {acao_cases} ELSE NULL END' if acao_cases else 'NULL'

    # UPDATE: enriquecer registros existentes com dados SEI da tabela_nova
    update_sql = f"""
    UPDATE cgfr_processo_enviado c
    INNER JOIN _dump_tabela_nova t ON t.processo_formatado = c.processo_formatado
    LEFT JOIN natdespesas n ON n.codigo = t.natureza_despesa
    LEFT JOIN class_fonte f ON CAST(f.codigo AS UNSIGNED) = t.fonte
    SET
        c.link_acesso = COALESCE(t.link_acesso, c.link_acesso),
        c.especificacao = COALESCE(t.especificacao, c.especificacao),
        c.id_unidade_geradora = COALESCE(t.id_unidade_geradora, c.id_unidade_geradora),
        c.geracao_sigla = COALESCE(t.geracao_sigla, c.geracao_sigla),
        c.geracao_data = COALESCE(t.geracao_data, c.geracao_data),
        c.geracao_descricao = COALESCE(t.geracao_descricao, c.geracao_descricao),
        c.usuario_gerador = COALESCE(t.usuario_gerador, c.usuario_gerador),
        c.ultimo_andamento_sigla = COALESCE(t.ultimo_andamento_sigla, c.ultimo_andamento_sigla),
        c.ultimo_andamento_descricao = COALESCE(t.ultimo_andamento_descricao, c.ultimo_andamento_descricao),
        c.ultimo_andamento_data = COALESCE(t.ultimo_andamento_data, c.ultimo_andamento_data),
        c.ultimo_andamento_usuario = COALESCE(t.ultimo_andamento_usuario, c.ultimo_andamento_usuario),
        c.objeto_do_pedido = COALESCE(t.objeto_do_pedido, c.objeto_do_pedido),
        c.necessidade = COALESCE(t.necessidade, c.necessidade),
        c.deliberacao = COALESCE(t.deliberacao, c.deliberacao),
        c.tipo_despesa = COALESCE(t.tipo_despesa, c.tipo_despesa),
        c.data_da_reuniao = COALESCE(t.data_da_reuniao, c.data_da_reuniao),
        c.valor_solicitado = COALESCE(t.valor_solicitado, c.valor_solicitado),
        c.valor_aprovado = COALESCE(t.valor_aprovado, c.valor_aprovado),
        c.fornecedor = c.fornecedor,
        c.tipo_processo = COALESCE(c.tipo_processo, 'Processo'),
        c.data_hora_processo = COALESCE(c.data_hora_processo, t.geracao_data),
        c.tramitado_sead_cgfr = COALESCE(t.tramitado_sead_cgfr, c.tramitado_sead_cgfr),
        c.recebido_cgfr = COALESCE(t.recebido_cgfr, c.recebido_cgfr),
        c.data_recebido_cgfr = COALESCE(t.data_recebido_cgfr, c.data_recebido_cgfr),
        c.devolvido_cgfr_sead = COALESCE(t.devolvido_cgfr_sead, c.devolvido_cgfr_sead),
        c.data_devolvido_cgfr_sead = COALESCE(t.data_devolvido_cgfr_sead, c.data_devolvido_cgfr_sead),
        c.natureza_despesa_id = COALESCE(n.id, c.natureza_despesa_id),
        c.fonte_id = COALESCE(f.id, c.fonte_id),
        c.acao_id = COALESCE({acao_expr}, c.acao_id)
    """
    result = conn.execute(db.text(update_sql))
    print(f'  [OK] {result.rowcount} registros enriquecidos com dados SEI')

    # INSERT: registros da tabela_nova que nao existem em cgfr_processo_enviado
    insert_new_sql = f"""
    INSERT INTO cgfr_processo_enviado (
        processo_formatado, link_acesso, especificacao, tipo_processo, data_hora_processo,
        id_unidade_geradora, geracao_sigla, geracao_data, geracao_descricao, usuario_gerador,
        ultimo_andamento_sigla, ultimo_andamento_descricao, ultimo_andamento_data, ultimo_andamento_usuario,
        tramitado_sead_cgfr, recebido_cgfr, data_recebido_cgfr, devolvido_cgfr_sead, data_devolvido_cgfr_sead,
        natureza_despesa_id, fonte_id, acao_id,
        objeto_do_pedido, necessidade, deliberacao, tipo_despesa,
        data_da_reuniao, valor_solicitado, valor_aprovado,
        data_inclusao
    )
    SELECT
        t.processo_formatado, t.link_acesso, t.especificacao, 'Processo', t.geracao_data,
        t.id_unidade_geradora, t.geracao_sigla, t.geracao_data, t.geracao_descricao, t.usuario_gerador,
        t.ultimo_andamento_sigla, t.ultimo_andamento_descricao, t.ultimo_andamento_data, t.ultimo_andamento_usuario,
        t.tramitado_sead_cgfr, t.recebido_cgfr, t.data_recebido_cgfr, t.devolvido_cgfr_sead, t.data_devolvido_cgfr_sead,
        n.id, f.id, {acao_expr},
        t.objeto_do_pedido, t.necessidade, t.deliberacao, t.tipo_despesa,
        t.data_da_reuniao, t.valor_solicitado, t.valor_aprovado,
        NOW()
    FROM _dump_tabela_nova t
    LEFT JOIN natdespesas n ON n.codigo = t.natureza_despesa
    LEFT JOIN class_fonte f ON CAST(f.codigo AS UNSIGNED) = t.fonte
    WHERE t.processo_formatado IS NOT NULL
      AND t.processo_formatado NOT IN (SELECT processo_formatado FROM cgfr_processo_enviado)
    """
    result2 = conn.execute(db.text(insert_new_sql))
    print(f'  [OK] {result2.rowcount} novos registros inseridos da tabela_nova')

    conn.execute(db.text('DROP TABLE IF EXISTS _dump_tabela_nova'))


def verify_import(conn):
    """Mostra estatisticas apos importacao."""
    print('\n--- Verificacao ---')

    total = conn.execute(db.text('SELECT COUNT(*) FROM cgfr_processo_enviado')).scalar()
    print(f'  Total registros: {total}')

    classificados = conn.execute(db.text(
        'SELECT COUNT(*) FROM cgfr_processo_enviado '
        'WHERE natureza_despesa_id IS NOT NULL AND fonte_id IS NOT NULL AND acao_id IS NOT NULL'
    )).scalar()
    print(f'  Classificados: {classificados}')
    print(f'  Pendentes: {total - classificados}')

    com_link = conn.execute(db.text(
        'SELECT COUNT(*) FROM cgfr_processo_enviado WHERE link_acesso IS NOT NULL AND link_acesso != ""'
    )).scalar()
    print(f'  Com link SEI: {com_link}')

    com_valor = conn.execute(db.text(
        'SELECT COUNT(*) FROM cgfr_processo_enviado WHERE valor_aprovado IS NOT NULL AND valor_aprovado > 0'
    )).scalar()
    print(f'  Com valor aprovado: {com_valor}')

    soma_aprov = conn.execute(db.text(
        'SELECT COALESCE(SUM(valor_aprovado), 0) FROM cgfr_processo_enviado'
    )).scalar()
    soma_solic = conn.execute(db.text(
        'SELECT COALESCE(SUM(valor_solicitado), 0) FROM cgfr_processo_enviado'
    )).scalar()
    print(f'  Valor total aprovado: R$ {float(soma_aprov):,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))
    print(f'  Valor total solicitado: R$ {float(soma_solic):,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))

    # Amostra
    print('\n--- Amostra (5 registros com classificacao) ---')
    rows = conn.execute(db.text(
        'SELECT processo_formatado, natureza_despesa_id, fonte_id, acao_id, '
        'valor_aprovado, nivel_prioridade '
        'FROM cgfr_processo_enviado '
        'WHERE natureza_despesa_id IS NOT NULL '
        'LIMIT 5'
    ))
    for row in rows:
        print(f'  {row[0]} | nat={row[1]} | fonte={row[2]} | acao={row[3]} | '
              f'aprov={row[4]} | prio={row[5]}')


def main():
    parser = argparse.ArgumentParser(description='Importa dump do sistema CGFR original')
    parser.add_argument('--executar', action='store_true', help='Executa de verdade (sem flag = dry-run)')
    parser.add_argument('--truncar', action='store_true', help='Limpa tabela antes de importar')
    args = parser.parse_args()

    dry_run = not args.executar

    if dry_run:
        print('='*60)
        print('  MODO DRY-RUN (use --executar para aplicar)')
        print('='*60)
    else:
        print('='*60)
        print('  MODO EXECUCAO - Alteracoes serao aplicadas!')
        print('='*60)

    app = create_app()

    with app.app_context():
        # Etapa 1: Adicionar colunas faltantes
        print('\n--- Etapa 1: Verificar/adicionar colunas ---')
        conn = db.session.connection()
        added = add_missing_columns(conn, dry_run)
        print(f'  Colunas adicionadas: {added}')
        if not dry_run:
            db.session.commit()

        # Re-obter conexao apos commit
        conn = db.session.connection()

        # Etapa 2: Construir mapas de FK
        print('\n--- Etapa 2: Construir mapeamentos de FK ---')
        acao_map = build_acao_map(conn)
        nat_map = build_natdespesa_map(conn)
        fonte_map = build_fonte_map(conn)
        print(f'  Acao: {len(acao_map)} | Natdespesas: {len(nat_map)} | Fontes: {len(fonte_map)}')

        # Etapa 3: Carregar dados do dump
        values_str = load_dump_data(DUMP_PATH)
        if not values_str:
            print('\nAbortando: nenhum dado encontrado no dump.')
            return

        # Etapa 4: Importar com mapeamento
        print('\n--- Etapa 4: Importar dados de processo_enviado ---')
        import_via_temp_table(conn, values_str, acao_map, nat_map, fonte_map,
                              dry_run=dry_run, truncar=args.truncar)

        # Etapa 5: Enriquecer com dados SEI da tabela_nova
        print('\n--- Etapa 5: Enriquecer com dados SEI (tabela_nova) ---')
        enrich_from_tabela_nova(conn, DUMP_PATH, acao_map, dry_run=dry_run)

        if not dry_run:
            db.session.commit()
            # Re-obter conexao para verificacao
            conn = db.session.connection()
            verify_import(conn)

    print('\n[OK] Concluido!')


if __name__ == '__main__':
    main()
