'''
Relatorio Completo: Tipificacao, Vinculacoes e Execucoes
=========================================================
Gera um relatorio detalhado do estado atual dos contratos apos
a aplicacao do CSV de tipo_contrato e limpeza de inconsistencias.

Secoes:
  1. Visao Geral dos Contratos
  2. Tipo de Contrato (CSV) - Detalhamento
  3. Status de Tipificacao (Catalogo) por Tipo
  4. Vinculacoes vs Tipo de Contrato
  5. Execucoes - Cobertura e Valores
  6. Inconsistencias Remanescentes
  7. Contratos sem tipo_contrato (fora do CSV)
  8. Contratos do CSV nao encontrados no DB

Uso:
  python scripts/relatorio_completo_tipificacao.py
'''
import os
import sys
import csv
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PASS', ''),
    database=os.getenv('DB_NAME', 'sgc'),
    charset='utf8mb4'
)

CSV_PATH = os.path.join(BASE_DIR, 'tipificacao contratos.csv')

LINE = '=' * 90
SUBLINE = '-' * 90


def fmt_valor(v):
    """Formata Decimal como moeda BRL."""
    if v is None:
        return 'R$ 0,00'
    return 'R$ {:,.2f}'.format(float(v)).replace(',', 'X').replace('.', ',').replace('X', '.')


def truncar(texto, max_len=70):
    if not texto:
        return '-'
    return (texto[:max_len] + '...') if len(texto) > max_len else texto


def main():
    print(LINE)
    print('  RELATORIO COMPLETO: TIPIFICACAO, VINCULACOES E EXECUCOES')
    print('  Gerado em: %s' % datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
    print(LINE)

    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1. VISAO GERAL DOS CONTRATOS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n[1/8] VISAO GERAL DOS CONTRATOS')
    print(SUBLINE)

    cur.execute('SELECT COUNT(*) FROM contratos')
    total_contratos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contratos WHERE situacao = 'EM_VIGOR'")
    em_vigor = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contratos WHERE situacao = 'ENCERRADO'")
    encerrados = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contratos WHERE situacao NOT IN ('EM_VIGOR', 'ENCERRADO') OR situacao IS NULL")
    outros_sit = cur.fetchone()[0]

    cur.execute('SELECT COALESCE(SUM(valor), 0), COALESCE(SUM(valorTotal), 0) FROM contratos')
    soma_valor, soma_valor_total = cur.fetchone()

    cur.execute("SELECT COALESCE(SUM(valor), 0) FROM contratos WHERE situacao = 'EM_VIGOR'")
    soma_em_vigor = cur.fetchone()[0]

    print('  Total de Contratos no DB:           %d' % total_contratos)
    print('    Em Vigor:                         %d' % em_vigor)
    print('    Encerrados:                       %d' % encerrados)
    print('    Outros/NULL:                      %d' % outros_sit)
    print('  Valor Total (soma valores):         %s' % fmt_valor(soma_valor))
    print('  Valor Total (soma valorTotal):      %s' % fmt_valor(soma_valor_total))
    print('  Valor Contratos Em Vigor:           %s' % fmt_valor(soma_em_vigor))

    # Modalidades
    cur.execute('''
        SELECT modalidade, COUNT(*), COALESCE(SUM(valor), 0)
        FROM contratos
        GROUP BY modalidade
        ORDER BY COUNT(*) DESC
    ''')
    print('\n  Distribuicao por Modalidade:')
    for mod, qtd, val in cur.fetchall():
        print('    %-30s  %3d contratos   %s' % (mod or 'NULL', qtd, fmt_valor(val)))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2. TIPO DE CONTRATO (CSV)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[2/8] TIPO DE CONTRATO (fonte: CSV)')
    print(SUBLINE)

    cur.execute('''
        SELECT tipo_contrato, COUNT(*), COALESCE(SUM(valor), 0)
        FROM contratos
        GROUP BY tipo_contrato
        ORDER BY tipo_contrato
    ''')
    tipos_db = cur.fetchall()

    mapa_label = {'S': 'Servico', 'M': 'Material', 'SM': 'Misto', None: 'Nao definido'}
    total_com_tipo = 0
    total_sem_tipo = 0
    for tipo, qtd, val in tipos_db:
        label = mapa_label.get(tipo, tipo or 'NULL')
        flag = '' if tipo else '  <<<'
        print('    %-5s (%-15s)  %3d contratos   %s%s' % (tipo or 'NULL', label, qtd, fmt_valor(val), flag))
        if tipo:
            total_com_tipo += qtd
        else:
            total_sem_tipo += qtd

    print('\n  COM tipo_contrato:                  %d' % total_com_tipo)
    print('  SEM tipo_contrato:                  %d' % total_sem_tipo)
    print('  Cobertura:                          %.1f%%' % (100.0 * total_com_tipo / total_contratos if total_contratos else 0))

    # Detalhe por tipo + situacao
    print('\n  Cruzamento Tipo x Situacao:')
    cur.execute('''
        SELECT tipo_contrato, situacao, COUNT(*)
        FROM contratos
        WHERE tipo_contrato IS NOT NULL
        GROUP BY tipo_contrato, situacao
        ORDER BY tipo_contrato, situacao
    ''')
    for tipo, sit, qtd in cur.fetchall():
        print('    %-5s | %-15s  %3d' % (tipo, sit or 'NULL', qtd))

    # Cruzamento tipo_contrato x modalidade
    print('\n  Cruzamento Tipo x Modalidade:')
    cur.execute('''
        SELECT tipo_contrato, modalidade, COUNT(*)
        FROM contratos
        WHERE tipo_contrato IS NOT NULL
        GROUP BY tipo_contrato, modalidade
        ORDER BY tipo_contrato, modalidade
    ''')
    for tipo, mod, qtd in cur.fetchall():
        print('    %-5s | %-30s  %3d' % (tipo, mod or 'NULL', qtd))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3. STATUS DE TIPIFICACAO (CATALOGO)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[3/8] STATUS DE TIPIFICACAO (Catalogo CATSERV/CATMAT)')
    print(SUBLINE)

    # Buscar todos os contratos com tipo_contrato definido
    cur.execute('''
        SELECT codigo, tipo_contrato, modalidade, situacao, objeto,
               catserv_classe_id, catserv_grupo_id, catmat_pdm_id, catmat_classe_id
        FROM contratos
        WHERE tipo_contrato IS NOT NULL
        ORDER BY tipo_contrato, codigo
    ''')
    contratos_tipados = cur.fetchall()

    stats = {
        'S': {'total': 0, 'tipificado': 0, 'nivel_classe': 0, 'nivel_grupo': 0, 'nao_tipificado': 0, 'lista_nao_tip': []},
        'M': {'total': 0, 'tipificado': 0, 'com_pdm': 0, 'com_classe': 0, 'nao_tipificado': 0, 'lista_nao_tip': []},
        'SM': {'total': 0, 'tipificado': 0, 'nao_tipificado': 0, 'lista_nao_tip': []},
    }

    for cod, tipo, mod, sit, obj, cs_classe, cs_grupo, cm_pdm, cm_classe in contratos_tipados:
        s = stats.get(tipo)
        if not s:
            continue
        s['total'] += 1

        if tipo == 'S':
            catserv_ok = cs_classe is not None or cs_grupo is not None
            if catserv_ok:
                s['tipificado'] += 1
                if cs_classe:
                    s['nivel_classe'] += 1
                else:
                    s['nivel_grupo'] += 1
            else:
                s['nao_tipificado'] += 1
                s['lista_nao_tip'].append((cod, mod, sit, truncar(obj, 60)))

        elif tipo == 'M':
            catmat_ok = cm_classe is not None and cm_pdm is not None
            if catmat_ok:
                s['tipificado'] += 1
            else:
                s['nao_tipificado'] += 1
                s['lista_nao_tip'].append((cod, mod, sit, truncar(obj, 60)))
            if cm_pdm:
                s['com_pdm'] += 1
            if cm_classe:
                s['com_classe'] += 1

        elif tipo == 'SM':
            catserv_ok = cs_classe is not None or cs_grupo is not None
            catmat_ok = cm_classe is not None and cm_pdm is not None
            if catserv_ok and catmat_ok:
                s['tipificado'] += 1
            else:
                s['nao_tipificado'] += 1
                s['lista_nao_tip'].append((cod, mod, sit, truncar(obj, 60)))

    # --- SERVICO ---
    s = stats['S']
    print('\n  SERVICO (S): %d contratos' % s['total'])
    print('    Tipificados (catalogo):  %d (%.0f%%)' % (s['tipificado'], 100.0 * s['tipificado'] / s['total'] if s['total'] else 0))
    print('      Nivel classe:          %d' % s['nivel_classe'])
    print('      Nivel grupo:           %d' % s['nivel_grupo'])
    print('    NAO tipificados:         %d' % s['nao_tipificado'])
    if s['lista_nao_tip']:
        for cod, mod, sit, obj in s['lista_nao_tip']:
            print('      [%s] %s | %s | %s' % (cod, sit or '-', mod or '-', obj))

    # --- MATERIAL ---
    s = stats['M']
    print('\n  MATERIAL (M): %d contratos' % s['total'])
    print('    Tipificados (catalogo):  %d (%.0f%%)' % (s['tipificado'], 100.0 * s['tipificado'] / s['total'] if s['total'] else 0))
    print('      Com PDM:               %d' % s['com_pdm'])
    print('      Com classe:            %d' % s['com_classe'])
    print('    NAO tipificados:         %d' % s['nao_tipificado'])
    if s['lista_nao_tip']:
        for cod, mod, sit, obj in s['lista_nao_tip']:
            print('      [%s] %s | %s | %s' % (cod, sit or '-', mod or '-', obj))

    # --- MISTO ---
    s = stats['SM']
    print('\n  MISTO (SM): %d contratos' % s['total'])
    print('    Tipificados (catalogo):  %d (%.0f%%)' % (s['tipificado'], 100.0 * s['tipificado'] / s['total'] if s['total'] else 0))
    print('    NAO tipificados:         %d' % s['nao_tipificado'])
    if s['lista_nao_tip']:
        for cod, mod, sit, obj in s['lista_nao_tip']:
            print('      [%s] %s | %s | %s' % (cod, sit or '-', mod or '-', obj))

    total_tip = sum(stats[t]['tipificado'] for t in stats)
    total_nao_tip = sum(stats[t]['nao_tipificado'] for t in stats)
    print('\n  RESUMO TIPIFICACAO CATALOGO:')
    print('    Tipificados:     %d / %d (%.1f%%)' % (total_tip, total_com_tipo, 100.0 * total_tip / total_com_tipo if total_com_tipo else 0))
    print('    Nao tipificados: %d' % total_nao_tip)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 4. VINCULACOES vs TIPO DE CONTRATO
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[4/8] VINCULACOES vs TIPO DE CONTRATO')
    print(SUBLINE)

    # Contar vinculacoes por contrato e tipo
    cur.execute('''
        SELECT iv.codigo_contrato, iv.tipo, COUNT(*)
        FROM itens_vinculados iv
        JOIN contratos c ON c.codigo = iv.codigo_contrato
        GROUP BY iv.codigo_contrato, iv.tipo
    ''')
    vinc_map = defaultdict(lambda: {'S': 0, 'M': 0})
    for cod, vtipo, qtd in cur.fetchall():
        vinc_map[cod][vtipo] = qtd

    # Buscar tipo_contrato de cada
    cur.execute('SELECT codigo, tipo_contrato FROM contratos WHERE tipo_contrato IS NOT NULL')
    tipo_map = {}
    for cod, tipo in cur.fetchall():
        tipo_map[cod] = tipo

    # Contratos COM tipo e COM vinculacoes
    com_vinc_e_tipo = 0
    com_tipo_sem_vinc = 0
    vinc_sem_tipo = 0

    # Analise de concordancia
    concordantes = []
    divergentes = []

    for cod in sorted(tipo_map.keys()):
        tipo = tipo_map[cod]
        if cod in vinc_map:
            com_vinc_e_tipo += 1
            v = vinc_map[cod]
            tem_s = v['S'] > 0
            tem_m = v['M'] > 0

            if tipo == 'S' and tem_s and not tem_m:
                concordantes.append(cod)
            elif tipo == 'M' and tem_m and not tem_s:
                concordantes.append(cod)
            elif tipo == 'SM' and tem_s and tem_m:
                concordantes.append(cod)
            else:
                divergentes.append((cod, tipo, v['S'], v['M']))
        else:
            com_tipo_sem_vinc += 1

    for cod in vinc_map:
        if cod not in tipo_map:
            vinc_sem_tipo += 1

    total_com_vinc = len(vinc_map)
    print('  Contratos com vinculacoes:              %d' % total_com_vinc)
    print('  Contratos com tipo E vinculacoes:       %d' % com_vinc_e_tipo)
    print('  Contratos com tipo SEM vinculacoes:     %d' % com_tipo_sem_vinc)
    print('  Contratos com vinculacoes SEM tipo:     %d' % vinc_sem_tipo)
    print()
    print('  Concordancia tipo vs vinculacoes:')
    print('    Concordantes:                         %d' % len(concordantes))
    print('    Divergentes:                          %d' % len(divergentes))

    if divergentes:
        print('\n  DIVERGENCIAS (tipo CSV != vinculacoes):')
        for cod, tipo, vs, vm in divergentes:
            cur.execute('SELECT objeto, modalidade FROM contratos WHERE codigo = %s', (cod,))
            r = cur.fetchone()
            obj = truncar(r[0], 50) if r else '-'
            mod = r[1] if r else '-'
            vinc_desc = []
            if vs:
                vinc_desc.append('%d serv' % vs)
            if vm:
                vinc_desc.append('%d mat' % vm)
            print('    [%s] tipo=%s | vinculacoes: %s | %s | %s' % (
                cod, tipo, '+'.join(vinc_desc), mod, obj))

    # Detalhe: contratos com vinculacoes mas sem tipo_contrato
    if vinc_sem_tipo > 0:
        print('\n  Contratos com vinculacoes SEM tipo_contrato (amostra):')
        count = 0
        for cod in sorted(vinc_map.keys()):
            if cod not in tipo_map:
                v = vinc_map[cod]
                cur.execute('SELECT objeto, modalidade, situacao FROM contratos WHERE codigo = %s', (cod,))
                r = cur.fetchone()
                obj = truncar(r[0], 40) if r else '-'
                mod = r[1] if r else '-'
                sit = r[2] if r else '-'
                print('    [%s] %s | %s | vinc: %dS+%dM | %s' % (
                    cod, sit, mod, v['S'], v['M'], obj))
                count += 1
                if count >= 15:
                    print('    ... e mais %d' % (vinc_sem_tipo - count))
                    break

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 5. EXECUCOES - COBERTURA E VALORES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[5/8] EXECUCOES - COBERTURA E VALORES')
    print(SUBLINE)

    cur.execute('SELECT COUNT(*) FROM execucoes')
    total_exec = cur.fetchone()[0]

    cur.execute('SELECT COUNT(DISTINCT codigo_contrato) FROM execucoes')
    contratos_com_exec = cur.fetchone()[0]

    cur.execute('SELECT COALESCE(SUM(valor), 0) FROM execucoes')
    soma_exec = cur.fetchone()[0]

    cur.execute('''
        SELECT tipo, COUNT(*), COALESCE(SUM(valor), 0)
        FROM execucoes
        GROUP BY tipo
    ''')
    print('  Total de execucoes:                   %d' % total_exec)
    print('  Contratos com execucoes:              %d / %d' % (contratos_com_exec, total_contratos))
    print('  Valor total executado:                %s' % fmt_valor(soma_exec))
    print()
    print('  Por tipo de execucao:')
    for etipo, eqtd, eval_ in cur.fetchall():
        label = 'Servico' if etipo == 'S' else 'Material'
        print('    %-10s  %5d execucoes   %s' % (label, eqtd, fmt_valor(eval_)))

    # Por ano
    cur.execute('''
        SELECT YEAR(data) as ano, tipo, COUNT(*), COALESCE(SUM(valor), 0)
        FROM execucoes
        WHERE data IS NOT NULL
        GROUP BY YEAR(data), tipo
        ORDER BY ano DESC, tipo
    ''')
    print('\n  Execucoes por Ano/Tipo:')
    print('    %-6s %-10s %7s %20s' % ('Ano', 'Tipo', 'Qtd', 'Valor'))
    print('    ' + '-' * 50)
    rows_ano = cur.fetchall()
    ano_totais = defaultdict(lambda: Decimal('0'))
    ano_qtds = defaultdict(int)
    for ano, etipo, eqtd, eval_ in rows_ano:
        label = 'Servico' if etipo == 'S' else 'Material'
        print('    %-6s %-10s %7d %20s' % (ano, label, eqtd, fmt_valor(eval_)))
        ano_totais[ano] += eval_
        ano_qtds[ano] += eqtd
    print('    ' + '-' * 50)
    for ano in sorted(ano_totais.keys(), reverse=True):
        print('    %-6s %-10s %7d %20s' % (ano, 'SUBTOTAL', ano_qtds[ano], fmt_valor(ano_totais[ano])))

    # Top 10 contratos por valor executado
    cur.execute('''
        SELECT e.codigo_contrato, c.tipo_contrato, c.situacao,
               COUNT(*) as qtd, SUM(e.valor) as total_val
        FROM execucoes e
        JOIN contratos c ON c.codigo = e.codigo_contrato
        GROUP BY e.codigo_contrato, c.tipo_contrato, c.situacao
        ORDER BY total_val DESC
        LIMIT 10
    ''')
    print('\n  Top 10 Contratos por Valor Executado:')
    print('    %-12s %-5s %-12s %5s %20s' % ('Contrato', 'Tipo', 'Situacao', 'Exec', 'Valor Total'))
    print('    ' + '-' * 60)
    for cod, tipo, sit, qtd, val in cur.fetchall():
        print('    %-12s %-5s %-12s %5d %20s' % (cod, tipo or '-', sit or '-', qtd, fmt_valor(val)))

    # Vinculacao das execucoes
    cur.execute('SELECT COUNT(*) FROM execucoes WHERE item_vinculado_id IS NOT NULL')
    exec_vinculadas = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM execucoes WHERE item_vinculado_id IS NULL')
    exec_sem_vinc = cur.fetchone()[0]

    print('\n  Vinculacao das execucoes:')
    print('    Com item_vinculado_id:  %d (%.1f%%)' % (
        exec_vinculadas, 100.0 * exec_vinculadas / total_exec if total_exec else 0))
    print('    Sem item_vinculado_id:  %d' % exec_sem_vinc)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 6. INCONSISTENCIAS REMANESCENTES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[6/8] INCONSISTENCIAS REMANESCENTES')
    print(SUBLINE)

    problemas = 0

    # 6a. Tipo S com catmat preenchido
    cur.execute('''
        SELECT codigo, catmat_pdm_id, catmat_classe_id
        FROM contratos
        WHERE tipo_contrato = 'S' AND (catmat_pdm_id IS NOT NULL OR catmat_classe_id IS NOT NULL)
    ''')
    s_com_catmat = cur.fetchall()
    if s_com_catmat:
        print('\n  [!] Tipo S com CATMAT preenchido: %d' % len(s_com_catmat))
        for cod, pdm, cls in s_com_catmat:
            print('      [%s] catmat_pdm=%s, catmat_classe=%s' % (cod, pdm, cls))
        problemas += len(s_com_catmat)
    else:
        print('\n  [OK] Nenhum tipo S com CATMAT preenchido')

    # 6b. Tipo M com catserv preenchido
    cur.execute('''
        SELECT codigo, catserv_classe_id, catserv_grupo_id
        FROM contratos
        WHERE tipo_contrato = 'M' AND (catserv_classe_id IS NOT NULL OR catserv_grupo_id IS NOT NULL)
    ''')
    m_com_catserv = cur.fetchall()
    if m_com_catserv:
        print('  [!] Tipo M com CATSERV preenchido: %d' % len(m_com_catserv))
        for cod, cls, grp in m_com_catserv:
            print('      [%s] catserv_classe=%s, catserv_grupo=%s' % (cod, cls, grp))
        problemas += len(m_com_catserv)
    else:
        print('  [OK] Nenhum tipo M com CATSERV preenchido')

    # 6c. Execucoes tipo S em contratos tipo M (e vice-versa)
    cur.execute('''
        SELECT e.codigo_contrato, c.tipo_contrato, e.tipo, COUNT(*)
        FROM execucoes e
        JOIN contratos c ON c.codigo = e.codigo_contrato
        WHERE c.tipo_contrato IS NOT NULL
          AND c.tipo_contrato != 'SM'
          AND e.tipo != c.tipo_contrato
        GROUP BY e.codigo_contrato, c.tipo_contrato, e.tipo
    ''')
    exec_tipo_errado = cur.fetchall()
    if exec_tipo_errado:
        print('  [!] Execucoes com tipo diferente do contrato: %d casos' % len(exec_tipo_errado))
        for cod, ctipo, etipo, qtd in exec_tipo_errado:
            print('      [%s] contrato=%s, execucao=%s (%d ocorrencias)' % (cod, ctipo, etipo, qtd))
        problemas += len(exec_tipo_errado)
    else:
        print('  [OK] Todas execucoes concordam com tipo do contrato')

    # 6d. Vinculacoes tipo divergente do tipo_contrato
    cur.execute('''
        SELECT iv.codigo_contrato, c.tipo_contrato, iv.tipo, COUNT(*)
        FROM itens_vinculados iv
        JOIN contratos c ON c.codigo = iv.codigo_contrato
        WHERE c.tipo_contrato IS NOT NULL
          AND c.tipo_contrato != 'SM'
          AND iv.tipo != c.tipo_contrato
        GROUP BY iv.codigo_contrato, c.tipo_contrato, iv.tipo
    ''')
    vinc_tipo_errado = cur.fetchall()
    if vinc_tipo_errado:
        print('  [!] Vinculacoes com tipo diferente do contrato: %d casos' % len(vinc_tipo_errado))
        for cod, ctipo, vtipo, qtd in vinc_tipo_errado:
            print('      [%s] contrato=%s, vinculacao=%s (%d itens)' % (cod, ctipo, vtipo, qtd))
        problemas += len(vinc_tipo_errado)
    else:
        print('  [OK] Todas vinculacoes concordam com tipo do contrato')

    # 6e. Execucoes apontando para item_vinculado inexistente
    cur.execute('''
        SELECT e.id, e.codigo_contrato, e.item_vinculado_id
        FROM execucoes e
        LEFT JOIN itens_vinculados iv ON iv.id = e.item_vinculado_id
        WHERE e.item_vinculado_id IS NOT NULL AND iv.id IS NULL
    ''')
    orfas = cur.fetchall()
    if orfas:
        print('  [!] Execucoes com item_vinculado_id orfao: %d' % len(orfas))
        for eid, ecod, evid in orfas[:10]:
            print('      exec_id=%d, contrato=%s, vinculado_id=%s' % (eid, ecod, evid))
        problemas += len(orfas)
    else:
        print('  [OK] Nenhuma execucao com item_vinculado_id orfao')

    print('\n  Total de inconsistencias encontradas: %d' % problemas)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 7. CONTRATOS SEM TIPO_CONTRATO
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[7/8] CONTRATOS SEM TIPO_CONTRATO (fora do CSV)')
    print(SUBLINE)

    cur.execute('''
        SELECT codigo, situacao, modalidade, objeto
        FROM contratos
        WHERE tipo_contrato IS NULL
        ORDER BY situacao DESC, codigo
    ''')
    sem_tipo = cur.fetchall()

    # Agrupar por situacao
    por_situacao = defaultdict(list)
    for cod, sit, mod, obj in sem_tipo:
        por_situacao[sit or 'NULL'].append((cod, mod, obj))

    print('  Total sem tipo_contrato: %d' % len(sem_tipo))
    print()
    for sit in sorted(por_situacao.keys()):
        lista = por_situacao[sit]
        print('  [%s] %d contratos:' % (sit, len(lista)))
        for cod, mod, obj in lista[:10]:
            print('    [%s] %-25s %s' % (cod, mod or '-', truncar(obj, 45)))
        if len(lista) > 10:
            print('    ... e mais %d' % (len(lista) - 10))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 8. CONTRATOS DO CSV NAO ENCONTRADOS NO DB
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n[8/8] CONTRATOS DO CSV NAO ENCONTRADOS NO DB')
    print(SUBLINE)

    csv_data = {}
    try:
        with open(CSV_PATH, encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                cod = row['Contrato'].strip()
                tipo = row['Tipo'].strip().lower()
                if cod and tipo and cod != 'Total Geral':
                    csv_data[cod] = tipo.upper()
    except FileNotFoundError:
        print('  CSV nao encontrado: %s' % CSV_PATH)
        csv_data = {}

    if csv_data:
        nao_encontrados = []
        for cod in sorted(csv_data.keys()):
            cur.execute('SELECT COUNT(*) FROM contratos WHERE codigo = %s', (cod,))
            if cur.fetchone()[0] == 0:
                nao_encontrados.append((cod, csv_data[cod]))

        print('  Total no CSV: %d' % len(csv_data))
        print('  Encontrados no DB: %d' % (len(csv_data) - len(nao_encontrados)))
        print('  NAO encontrados: %d' % len(nao_encontrados))

        if nao_encontrados:
            print()
            for cod, tipo in nao_encontrados:
                print('    [%s] tipo=%s' % (cod, tipo))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RESUMO FINAL
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print('\n\n' + LINE)
    print('  RESUMO EXECUTIVO')
    print(LINE)
    print('  Contratos no DB:                     %d' % total_contratos)
    print('  Com tipo_contrato (CSV):             %d (%.1f%%)' % (total_com_tipo, 100.0 * total_com_tipo / total_contratos if total_contratos else 0))
    print('    Servico (S):                       %d' % stats['S']['total'])
    print('    Material (M):                      %d' % stats['M']['total'])
    print('    Misto (SM):                        %d' % stats['SM']['total'])
    print('  Sem tipo_contrato:                   %d' % total_sem_tipo)
    print('  Tipificados (catalogo):              %d / %d' % (total_tip, total_com_tipo))
    print('  Com execucoes:                       %d (%d execucoes, %s)' % (
        contratos_com_exec, total_exec, fmt_valor(soma_exec)))
    print('  Com vinculacoes:                     %d' % total_com_vinc)
    print('  Inconsistencias:                     %d' % problemas)
    print('  CSV: %d contratos, %d nao no DB' % (len(csv_data), len(nao_encontrados) if csv_data else 0))
    print(LINE)

    conn.close()


if __name__ == '__main__':
    main()
