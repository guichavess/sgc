"""Analise do CSV de tipificacao vs modalidade atual dos contratos."""
import pymysql
import os
import csv
from dotenv import load_dotenv

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

# 1. Ler CSV
csv_path = os.path.join(base_dir, 'tipificacao contratos.csv')
csv_contratos = {}
with open(csv_path, encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        cod = row['Contrato'].strip()
        tipo = row['Tipo'].strip().lower()
        if cod and tipo and cod != 'Total Geral':
            csv_contratos[cod] = tipo

print('CSV: %d contratos' % len(csv_contratos))
print('  s:  %d' % sum(1 for v in csv_contratos.values() if v == 's'))
print('  m:  %d' % sum(1 for v in csv_contratos.values() if v == 'm'))
print('  sm: %d' % sum(1 for v in csv_contratos.values() if v == 'sm'))

# 2. Buscar modalidade de cada contrato no DB
db_contratos = {}
for cod in csv_contratos:
    cur.execute('SELECT codigo, modalidade FROM contratos WHERE codigo = %s', (cod,))
    r = cur.fetchone()
    if r:
        db_contratos[r[0]] = r[1]

print('\nNo DB: %d de %d' % (len(db_contratos), len(csv_contratos)))
nao_encontrados = sorted(set(csv_contratos.keys()) - set(db_contratos.keys()))
if nao_encontrados:
    print('NAO encontrados no DB: %s' % nao_encontrados)

# 3. Mapear modalidade -> tipo derivado
def tipo_from_modalidade(mod):
    if mod in ('SERVICOS', 'ALUGUEIS_IMOVEIS', 'ALUGUEIS'):
        return 's'
    elif mod == 'FORNECIMENTO_MATERIAIS':
        return 'm'
    elif mod == 'FORNECIMENTO_BENS':
        return 'sm'
    return '?'

# 4. Encontrar conflitos
print('\n' + '='*80)
print('CONFLITOS: CSV diz um tipo, mas modalidade implica outro')
print('='*80)
conflitos = []
for cod in sorted(csv_contratos.keys()):
    tipo_csv = csv_contratos[cod]
    mod = db_contratos.get(cod)
    if not mod:
        continue
    tipo_modal = tipo_from_modalidade(mod)
    if tipo_csv != tipo_modal:
        conflitos.append((cod, tipo_csv, tipo_modal, mod))
        print('  [%s] CSV=%s  Modalidade=%s (%s)' % (cod, tipo_csv.upper(), tipo_modal.upper(), mod))

print('\nTotal conflitos: %d de %d contratos' % (len(conflitos), len(db_contratos)))

# 5. Detalhar o que o CSV quer mudar
print('\n' + '='*80)
print('MAPEAMENTO CSV -> MODALIDADE NECESSARIA')
print('='*80)
print('  CSV "s"  -> modalidade deveria ser SERVICOS (ou ALUGUEIS*)')
print('  CSV "m"  -> modalidade deveria ser FORNECIMENTO_MATERIAIS')
print('  CSV "sm" -> modalidade deveria ser FORNECIMENTO_BENS')

# Agrupar conflitos por tipo de mudanca
mudancas = {}
for cod, tipo_csv, tipo_modal, mod in conflitos:
    chave = '%s -> %s' % (mod, tipo_csv.upper())
    if chave not in mudancas:
        mudancas[chave] = []
    mudancas[chave].append(cod)

print('\nMudancas necessarias:')
for chave, cods in sorted(mudancas.items()):
    print('  %s: %d contratos' % (chave, len(cods)))
    for c in cods:
        cur.execute('SELECT objeto FROM contratos WHERE codigo = %s', (c,))
        obj = cur.fetchone()
        obj_txt = (obj[0] or '-')[:80] if obj else '-'
        print('    [%s] %s' % (c, obj_txt))

conn.close()
