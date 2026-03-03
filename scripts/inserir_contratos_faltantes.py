'''
Inserir contratos faltantes em producao
========================================
Contratos que nao existem na tabela contratos e precisam ser inseridos
para que as vinculacoes e execucoes possam ser importadas.

DRY-RUN por padrao. Use --executar para aplicar.

Uso:
  python scripts/inserir_contratos_faltantes.py             # dry-run
  python scripts/inserir_contratos_faltantes.py --executar   # aplica
'''
import os
import sys

import pymysql
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB = dict(host=os.getenv('DB_HOST', 'localhost'), user=os.getenv('DB_USER', 'root'),
          password=os.getenv('DB_PASS', ''), database=os.getenv('DB_NAME', 'sgc'), charset='utf8mb4')

EXECUTAR = '--executar' in sys.argv

CONTRATOS = [
    {
        'codigo': '25014859',
        'situacao': 'EM_VIGOR',
        'numeroOriginal': '09/2025',
        'numProcesso': '00002.001206/2025-01',
        'objeto': 'Aquisição de equipamentos de informática',
        'natureza': 'DESPESA',
        'tipoContratante': 'TIPO_CREDOR_UG',
        'codigoContratante': '210102',
        'nomeContratante': '210102 - FUNDO ROTATIVO DE MATERIAL E CONS. PATRIM. PI',
        'tipoContratado': 'TIPO_CREDOR_PJ',
        'codigoContratado': '10742806000109',
        'nomeContratado': '10742806000109 - NATAL COMPUTER LTDA',
        'valor': 218686.00,
        'valorTotal': 218686.00,
        'dataProposta': None,
        'dataCelebracao': '2025-06-14',
        'dataPublicacao': '2025-06-18',
        'dataInicioVigencia': '2025-06-14',
        'dataFimVigencia': '2026-06-14',
        'codigoModalidadeLicitacao': '12',
        'nomeModalidadeLicitacao': 'Pregão',
        'regimeExecucao': 'ENTREGA_IMEDIATA',
        'modalidade': 'FORNECIMENTO_BENS',
        'objetivo': 'Contratação de empresa especializada para aquisição de computadores (Desktop), notebooks, impressoras e estabilizadores para atender a demanda da Secretaria de Administração do Estado do Piauí - SEAD/PI.',
        'status': 'STATUS_ASSINADO',
        'dataFimVigenciaTotal': '2026-06-14',
        'hash_contrato': 'db5a4f15ef48f7a8c541be5a82ba8d5b5decd42f',
        'hash_fiscais': '97d170e1550eee4afc0af065b78cda302a97674c',
        'hash_aditivos': '97d170e1550eee4afc0af065b78cda302a97674c',
    },
    {
        'codigo': '25014928',
        'situacao': 'EM_VIGOR',
        'numeroOriginal': '11/2025',
        'numProcesso': '00002.001206/2025-01',
        'objeto': 'Aquisição de equipamentos de informática',
        'natureza': 'DESPESA',
        'tipoContratante': 'TIPO_CREDOR_UG',
        'codigoContratante': '210102',
        'nomeContratante': '210102 - FUNDO ROTATIVO DE MATERIAL E CONS. PATRIM. PI',
        'tipoContratado': 'TIPO_CREDOR_PJ',
        'codigoContratado': '04191666000125',
        'nomeContratado': '04191666000125 - NTECH TI LTDA',
        'valor': 98158.46,
        'valorTotal': 98158.46,
        'dataProposta': None,
        'dataCelebracao': '2025-06-13',
        'dataPublicacao': '2025-06-18',
        'dataInicioVigencia': '2025-06-13',
        'dataFimVigencia': '2026-06-13',
        'codigoModalidadeLicitacao': '12',
        'nomeModalidadeLicitacao': 'Pregão',
        'regimeExecucao': 'ENTREGA_IMEDIATA',
        'modalidade': 'FORNECIMENTO_BENS',
        'objetivo': 'Contratação de empresa especializada para aquisição de computadores (Desktop), notebooks, impressoras e estabilizadores para atender a demanda da Secretaria de Administração do Estado do Piauí - SEAD/PI.',
        'status': 'STATUS_ASSINADO',
        'dataFimVigenciaTotal': '2026-06-13',
        'hash_contrato': '4d05119bc47257be281bb31f27d8d7edf2ecece7',
        'hash_fiscais': '97d170e1550eee4afc0af065b78cda302a97674c',
        'hash_aditivos': '97d170e1550eee4afc0af065b78cda302a97674c',
    },
    {
        'codigo': '25014931',
        'situacao': 'EM_VIGOR',
        'numeroOriginal': '10/2025',
        'numProcesso': '00002.001206/2025-01',
        'objeto': 'Aquisição de equipamentos de informática',
        'natureza': 'DESPESA',
        'tipoContratante': 'TIPO_CREDOR_UG',
        'codigoContratante': '210102',
        'nomeContratante': '210102 - FUNDO ROTATIVO DE MATERIAL E CONS. PATRIM. PI',
        'tipoContratado': 'TIPO_CREDOR_PJ',
        'codigoContratado': '13015273000151',
        'nomeContratado': '13015273000151 - INFORMOVEIS DIST. DE INFOR. E ESC. LTDA-ME',
        'valor': 88572.22,
        'valorTotal': 88572.22,
        'dataProposta': None,
        'dataCelebracao': '2025-06-13',
        'dataPublicacao': '2025-06-18',
        'dataInicioVigencia': '2025-06-13',
        'dataFimVigencia': '2026-06-13',
        'codigoModalidadeLicitacao': '12',
        'nomeModalidadeLicitacao': 'Pregão',
        'regimeExecucao': 'ENTREGA_IMEDIATA',
        'modalidade': 'FORNECIMENTO_BENS',
        'objetivo': 'Contratação de empresa especializada para aquisição de equipamento de informatica para atender a demanda da Secretaria de Administração do Estado do Piauí - SEAD/PI.',
        'status': 'STATUS_ASSINADO',
        'dataFimVigenciaTotal': '2026-06-13',
        'hash_contrato': 'dfb36df7eada5fdcacf7f48a97b7f9b3abbc6390',
        'hash_fiscais': '97d170e1550eee4afc0af065b78cda302a97674c',
        'hash_aditivos': '97d170e1550eee4afc0af065b78cda302a97674c',
    },
    {
        'codigo': '25017768',
        'situacao': 'EM_VIGOR',
        'numeroOriginal': '45/2025',
        'numProcesso': '00002.009453/2025-48',
        'objeto': 'Fornecimento de material de permanente - mobiliário (Cadeira Giratória Presidente com Espaldar Alto)',
        'natureza': 'DESPESA',
        'tipoContratante': 'TIPO_CREDOR_UG',
        'codigoContratante': '210102',
        'nomeContratante': '210102 - FUNDO ROTATIVO DE MATERIAL E CONS. PATRIM. PI',
        'tipoContratado': 'TIPO_CREDOR_PJ',
        'codigoContratado': '49058654000165',
        'nomeContratado': '49058654000165 - FLEXFORM INDUSTRIA E COMERCIO DE MOVEIS LTDA',
        'valor': 5556.50,
        'valorTotal': 5556.50,
        'dataProposta': None,
        'dataCelebracao': '2025-11-28',
        'dataPublicacao': '2025-12-02',
        'dataInicioVigencia': '2025-11-28',
        'dataFimVigencia': '2026-11-28',
        'codigoModalidadeLicitacao': '12',
        'nomeModalidadeLicitacao': 'Pregão',
        'regimeExecucao': 'ENTREGA_IMEDIATA',
        'modalidade': 'FORNECIMENTO_BENS',
        'objetivo': 'Aquisição de 01 (uma) Cadeira Giratória Presidente com Espaldar Alto, apoio de cabeça e base em Nylon Preta - Linha Flextopic, por meio de adesão (carona) à Ata de Registro de Preços nº 062/2024, decorrente do Pregão Eletrônico nº 018/2024, gerenciada pelo Ministério Público do Estado do Ceará, através da Procuradoria-Geral de Justiça.',
        'status': 'STATUS_ASSINADO',
        'dataFimVigenciaTotal': '2026-11-28',
        'hash_contrato': '4c57bae03023eceba5010cf5efa7e0d5b7e863ef',
        'hash_fiscais': '97d170e1550eee4afc0af065b78cda302a97674c',
        'hash_aditivos': '97d170e1550eee4afc0af065b78cda302a97674c',
    },
]

COLUNAS = [
    'codigo', 'situacao', 'numeroOriginal', 'numProcesso', 'objeto', 'natureza',
    'tipoContratante', 'codigoContratante', 'nomeContratante',
    'tipoContratado', 'codigoContratado', 'nomeContratado',
    'valor', 'valorTotal', 'dataProposta', 'dataCelebracao', 'dataPublicacao',
    'dataInicioVigencia', 'dataFimVigencia',
    'codigoModalidadeLicitacao', 'nomeModalidadeLicitacao',
    'regimeExecucao', 'modalidade', 'objetivo', 'status', 'dataFimVigenciaTotal',
    'hash_contrato', 'hash_fiscais', 'hash_aditivos',
]


def main():
    modo = 'EXECUTAR' if EXECUTAR else 'DRY-RUN'
    print(f'Inserir Contratos Faltantes [{modo}]')
    print('=' * 60)

    conn = pymysql.connect(**DB)
    cur = conn.cursor()

    inseridos = 0
    ja_existem = 0

    for c in CONTRATOS:
        cur.execute('SELECT COUNT(*) FROM contratos WHERE codigo = %s', (c['codigo'],))
        existe = cur.fetchone()[0] > 0

        if existe:
            print(f'  [{c["codigo"]}] Ja existe. Pulando.')
            ja_existem += 1
            continue

        print(f'  [{c["codigo"]}] {c["objeto"][:60]}')
        print(f'    Contratado: {c["nomeContratado"][:50]}')
        print(f'    Valor: R$ {c["valor"]:,.2f}')
        print(f'    Vigencia: {c["dataInicioVigencia"]} a {c["dataFimVigencia"]}')

        if EXECUTAR:
            placeholders = ', '.join(['%s'] * len(COLUNAS))
            cols = ', '.join(COLUNAS)
            valores = [c[col] for col in COLUNAS]
            cur.execute(f'INSERT INTO contratos ({cols}) VALUES ({placeholders})', valores)
            inseridos += 1
            print(f'    -> INSERIDO')
        else:
            print(f'    -> [DRY-RUN] Seria inserido')
            inseridos += 1

    if EXECUTAR and inseridos > 0:
        conn.commit()

    conn.close()

    print('\n' + '=' * 60)
    print(f'  Ja existiam: {ja_existem}')
    print(f'  Inseridos:   {inseridos}')
    print('=' * 60)
    if not EXECUTAR:
        print('Use --executar para aplicar')


if __name__ == '__main__':
    main()
