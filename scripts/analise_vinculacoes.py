"""Analise de inconsistencias nas vinculacoes de itens e tipificacoes."""
import pymysql
import os
from dotenv import load_dotenv
from collections import defaultdict

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(base_dir, '.env'))

DB = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PASS', ''),
    database=os.getenv('DB_NAME', 'sgc'),
    charset='utf8mb4'
)

conn = pymysql.connect(**DB)
cur = conn.cursor()

# =====================================================================
# 1. ANALISE DO CONTRATO 25017284 (caso especifico)
# =====================================================================
cod = '25017284'
print('=' * 80)
print('CONTRATO %s - ANALISE DETALHADA' % cod)
print('=' * 80)

cur.execute("""
    SELECT codigo, objeto, modalidade, natureza, situacao,
           catserv_classe_id, catserv_grupo_id, catmat_pdm_id, catmat_classe_id
    FROM contratos WHERE codigo = %s
""", (cod,))
r = cur.fetchone()
if r:
    obj = (r[1] or '-')[:120]
    print('Objeto: %s' % obj)
    print('Modalidade: %s | Natureza: %s | Situacao: %s' % (r[2], r[3], r[4]))
    print('Tipificacao: catserv_classe=%s, catserv_grupo=%s, catmat_pdm=%s, catmat_classe=%s' % (r[5], r[6], r[7], r[8]))

print('\nVINCULACOES (itens_vinculados):')
cur.execute("""
    SELECT iv.id, iv.tipo, iv.catserv_servico_id, iv.catmat_item_id, iv.item_contrato_id
    FROM itens_vinculados iv
    WHERE iv.codigo_contrato = %s
    ORDER BY iv.tipo, iv.id
""", (cod,))
vincs = cur.fetchall()
for row in vincs:
    vid, vtipo, catserv, catmat, ic_id = row
    tipo_label = 'SERVICO' if vtipo == 'S' else 'MATERIAL'
    nome = '-'
    if vtipo == 'S' and catserv:
        cur.execute('SELECT nome FROM catserv_servicos WHERE codigo_servico = %s', (catserv,))
        n = cur.fetchone()
        nome = (n[0] or '?')[:80] if n else '?'
    elif vtipo == 'M' and catmat:
        cur.execute('SELECT descricao FROM catmat_itens WHERE id = %s', (catmat,))
        n = cur.fetchone()
        nome = (n[0] or '?')[:80] if n else '?'
    print('  [%-8s] ID=%d | catserv=%s | catmat=%s | item_contrato=%s' % (tipo_label, vid, catserv, catmat, ic_id))
    print('             -> %s' % nome)

print('\nEXECUCOES:')
cur.execute("""
    SELECT id, tipo, valor, quantidade, data, catserv_servico_id, catmat_item_id, item_vinculado_id
    FROM execucoes WHERE codigo_contrato = %s ORDER BY data
""", (cod,))
execs = cur.fetchall()
for row in execs:
    eid, etipo, val, qtd, dt, cs, cm, vinc_id = row
    t = 'SERV' if etipo == 'S' else 'MAT '
    print('  ID=%d | %s | R$ %12s | qtd=%s | %s | vinc_id=%s' % (eid, t, '{:,.2f}'.format(float(val)), qtd, dt, vinc_id))
print('  TOTAL: %d execucoes' % len(execs))

# =====================================================================
# 2. ANALISE GERAL: contratos com vinculacoes de tipos mistos
# =====================================================================
print('\n' + '=' * 80)
print('ANALISE GERAL: INCONSISTENCIAS NAS VINCULACOES')
print('=' * 80)

# Buscar todos contratos com vinculacoes e seus tipos
cur.execute("""
    SELECT iv.codigo_contrato, iv.tipo, COUNT(*) as qtd
    FROM itens_vinculados iv
    JOIN contratos c ON c.codigo = iv.codigo_contrato
    GROUP BY iv.codigo_contrato, iv.tipo
    ORDER BY iv.codigo_contrato
""")
vinc_por_contrato = defaultdict(dict)
for row in cur.fetchall():
    vinc_por_contrato[row[0]][row[1]] = row[2]

# Buscar modalidade de cada contrato
cur.execute("""
    SELECT codigo, modalidade, natureza, objeto,
           catserv_classe_id, catserv_grupo_id, catmat_pdm_id, catmat_classe_id
    FROM contratos
    WHERE codigo IN (%s)
""" % ','.join("'%s'" % c for c in vinc_por_contrato.keys()))

contratos_info = {}
for row in cur.fetchall():
    contratos_info[row[0]] = {
        'modalidade': row[1],
        'natureza': row[2],
        'objeto': (row[3] or '-')[:80],
        'catserv_classe': row[4],
        'catserv_grupo': row[5],
        'catmat_pdm': row[6],
        'catmat_classe': row[7],
    }

# Classificar
mistos = []       # contratos com vinculacoes S + M
so_servico = []   # so vinculacoes S
so_material = []  # so vinculacoes M

for cod_c, tipos in sorted(vinc_por_contrato.items()):
    tem_s = 'S' in tipos
    tem_m = 'M' in tipos
    info = contratos_info.get(cod_c, {})
    entry = {
        'codigo': cod_c,
        'vinc_S': tipos.get('S', 0),
        'vinc_M': tipos.get('M', 0),
        'modalidade': info.get('modalidade', '?'),
        'objeto': info.get('objeto', '?'),
        'catserv_classe': info.get('catserv_classe'),
        'catserv_grupo': info.get('catserv_grupo'),
        'catmat_pdm': info.get('catmat_pdm'),
        'catmat_classe': info.get('catmat_classe'),
    }
    if tem_s and tem_m:
        mistos.append(entry)
    elif tem_s:
        so_servico.append(entry)
    elif tem_m:
        so_material.append(entry)

print('\n--- RESUMO ---')
print('  Contratos com vinculacoes:      %d' % len(vinc_por_contrato))
print('  So servico (tipo=S):            %d' % len(so_servico))
print('  So material (tipo=M):           %d' % len(so_material))
print('  MISTOS (S + M):                 %d' % len(mistos))

# 3. Contratos MISTOS - potenciais problemas
if mistos:
    print('\n--- CONTRATOS MISTOS (vinculacoes S + M) ---')
    for e in mistos:
        print('\n  [%s] %s' % (e['codigo'], e['modalidade']))
        print('    Objeto: %s' % e['objeto'])
        print('    Vinculacoes: %d servico + %d material' % (e['vinc_S'], e['vinc_M']))
        print('    Tipificacao: catserv_classe=%s grupo=%s | catmat_pdm=%s classe=%s' % (
            e['catserv_classe'], e['catserv_grupo'], e['catmat_pdm'], e['catmat_classe']))

# 4. Contratos SERVICOS com modalidade FORNECIMENTO_BENS (suspeito)
print('\n--- CONTRATOS SO SERVICO COM MODALIDADE FORNECIMENTO_BENS ---')
suspeitos_serv = [e for e in so_servico if e['modalidade'] == 'FORNECIMENTO_BENS']
for e in suspeitos_serv:
    print('  [%s] %s' % (e['codigo'], e['objeto']))
    print('    Vinculacoes: %d servico' % e['vinc_S'])

if not suspeitos_serv:
    print('  Nenhum encontrado.')

# 5. Contratos MATERIAL com modalidade SERVICOS (suspeito)
print('\n--- CONTRATOS SO MATERIAL COM MODALIDADE SERVICOS ---')
suspeitos_mat = [e for e in so_material if e['modalidade'] == 'SERVICOS']
for e in suspeitos_mat:
    print('  [%s] %s' % (e['codigo'], e['objeto']))
    print('    Vinculacoes: %d material' % e['vinc_M'])

if not suspeitos_mat:
    print('  Nenhum encontrado.')

# 6. Tipificacao inconsistente - contrato so servico mas tem catmat, ou vice-versa
print('\n--- TIPIFICACAO INCONSISTENTE ---')
inconsistentes = []
for e in so_servico:
    if e['catmat_pdm'] or e['catmat_classe']:
        inconsistentes.append(('SO_SERVICO com catmat', e))
for e in so_material:
    if e['catserv_classe'] or e['catserv_grupo']:
        inconsistentes.append(('SO_MATERIAL com catserv', e))

for label, e in inconsistentes:
    print('  [%s] %s' % (e['codigo'], label))
    print('    Modalidade: %s | Objeto: %s' % (e['modalidade'], e['objeto']))
    print('    catserv_classe=%s grupo=%s | catmat_pdm=%s classe=%s' % (
        e['catserv_classe'], e['catserv_grupo'], e['catmat_pdm'], e['catmat_classe']))

if not inconsistentes:
    print('  Nenhuma inconsistencia encontrada.')

# 7. Execucoes com tipo diferente das vinculacoes
print('\n--- EXECUCOES COM TIPO DIFERENTE DAS VINCULACOES ---')
cur.execute("""
    SELECT e.codigo_contrato, e.tipo AS exec_tipo, iv.tipo AS vinc_tipo, COUNT(*) as qtd
    FROM execucoes e
    JOIN itens_vinculados iv ON iv.id = e.item_vinculado_id
    WHERE e.tipo <> iv.tipo
    GROUP BY e.codigo_contrato, e.tipo, iv.tipo
""")
divergencias = cur.fetchall()
for row in divergencias:
    print('  Contrato %s: execucao tipo=%s mas vinculacao tipo=%s (%d ocorrencias)' % row)
if not divergencias:
    print('  Nenhuma divergencia encontrada.')

print('\n' + '=' * 80)
conn.close()
