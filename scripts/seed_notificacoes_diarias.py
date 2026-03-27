"""
Seed de tipos de notificacao para o fluxo completo de Diarias.

Adiciona tipos que ainda nao existem (verifica por codigo).

Uso:
  python scripts/seed_notificacoes_diarias.py             (DRY-RUN)
  python scripts/seed_notificacoes_diarias.py --executar   (aplica)
"""
import os
import sys
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'sgc')

DRY_RUN = '--executar' not in sys.argv

TIPOS = [
    # ── Já existentes (apenas referência) ──
    # 'diarias.nova_solicitacao' - lembrete
    # 'diarias.aprovada' - lembrete

    # ── Etapa 1: Assinatura do Superintendente ──
    {
        'codigo': 'diarias.assinatura_superintendente',
        'modulo': 'diarias',
        'nome': 'Superintendente Assinou Requisicoes',
        'descricao': 'Notifica que o Superintendente assinou as requisicoes. Aguardando autorizacao do Secretario.',
        'nivel': 'lembrete',
    },

    # ── Fluxo Financeiro (etapas 2-9) ──
    {
        'codigo': 'diarias.nota_reserva',
        'modulo': 'diarias',
        'nome': 'Nota de Reserva Inserida',
        'descricao': 'Notifica que a Nota de Reserva e Quadro Orcamentario foram inseridos',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.nota_empenho',
        'modulo': 'diarias',
        'nome': 'Nota de Empenho Inserida',
        'descricao': 'Notifica que a NE foi inserida no processo',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.despacho_ccdp',
        'modulo': 'diarias',
        'nome': 'Despacho CCDP Gerado',
        'descricao': 'Notifica que o Despacho CCDP foi gerado apos a NE',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.ciencia_sga',
        'modulo': 'diarias',
        'nome': 'Ciencia SGA Registrada',
        'descricao': 'Notifica que o Superintendente registrou ciencia e gerou despacho SGA',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.analise_nci',
        'modulo': 'diarias',
        'nome': 'Analise NCI Concluida',
        'descricao': 'Notifica que o NCI concluiu a analise de pagamento',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.despacho_apoio',
        'modulo': 'diarias',
        'nome': 'Despacho APOIO/DFIN Gerado',
        'descricao': 'Notifica que o Superintendente gerou o Despacho APOIO/DFIN',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.despacho_diretor',
        'modulo': 'diarias',
        'nome': 'Despacho Diretor DFIN Gerado',
        'descricao': 'Notifica que o Diretor DFIN gerou o despacho e encaminhou a GEO',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.despacho_geo',
        'modulo': 'diarias',
        'nome': 'Despacho GEO Gerado',
        'descricao': 'Notifica que o GEO gerou o despacho e encaminhou a CCDP',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.nl_inserida',
        'modulo': 'diarias',
        'nome': 'NL Inserida',
        'descricao': 'Nota de Liquidacao inserida no processo',
        'nivel': 'silenciosa',
    },
    {
        'codigo': 'diarias.pd_inserida',
        'modulo': 'diarias',
        'nome': 'PD Inserida',
        'descricao': 'Programacao de Desembolso inserida no processo',
        'nivel': 'silenciosa',
    },
    {
        'codigo': 'diarias.ob_inserida',
        'modulo': 'diarias',
        'nome': 'OB Inserida - Pagamento Realizado',
        'descricao': 'Ordem Bancaria inserida. Solicitante deve gerar Relatorio de Viagem.',
        'nivel': 'alerta',
    },

    # ── Fluxo Pós-OB (etapas 10-11) ──
    {
        'codigo': 'diarias.relatorio_viagem',
        'modulo': 'diarias',
        'nome': 'Relatorio de Viagem Gerado',
        'descricao': 'Solicitante gerou o Relatorio de Viagem no SEI',
        'nivel': 'lembrete',
    },
    {
        'codigo': 'diarias.comprovante_viagem',
        'modulo': 'diarias',
        'nome': 'Comprovante de Viagem Enviado',
        'descricao': 'Solicitante enviou o comprovante de viagem. CCDP deve finalizar prestacao de contas.',
        'nivel': 'alerta',
    },
    {
        'codigo': 'diarias.np_inserida',
        'modulo': 'diarias',
        'nome': 'Nota Patrimonial Inserida',
        'descricao': 'NP inserida pela CCDP na prestacao de contas',
        'nivel': 'silenciosa',
    },
    {
        'codigo': 'diarias.prestacao_scdp',
        'modulo': 'diarias',
        'nome': 'Prestacao SCDP Enviada',
        'descricao': 'Documento de Prestacao SCDP enviado pela CCDP',
        'nivel': 'silenciosa',
    },
    {
        'codigo': 'diarias.processo_concluido',
        'modulo': 'diarias',
        'nome': 'Processo de Diaria Concluido',
        'descricao': 'Despacho final gerado. Processo pago e concluido.',
        'nivel': 'alerta',
    },
]


def run():
    conn = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        database=DB_NAME, charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )
    cursor = conn.cursor()

    modo = "DRY-RUN" if DRY_RUN else "EXECUTANDO"
    print("=" * 60)
    print(f"Seed: Tipos de Notificacao Diarias - {modo}")
    print("=" * 60)

    inseridos = 0
    pulados = 0

    for t in TIPOS:
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM notificacao_tipos WHERE codigo = %s",
            (t['codigo'],)
        )
        if cursor.fetchone()['cnt'] > 0:
            print(f"  SKIP  {t['codigo']} (ja existe)")
            pulados += 1
            continue

        sql = """INSERT INTO notificacao_tipos
                 (codigo, modulo, nome, descricao, nivel, canal_in_app, canal_email, canal_telegram, ativo)
                 VALUES (%s, %s, %s, %s, %s, 1, 0, 0, 1)"""
        params = (t['codigo'], t['modulo'], t['nome'], t['descricao'], t['nivel'])

        if DRY_RUN:
            print(f"  [DRY] {t['codigo']} ({t['nivel']})")
        else:
            cursor.execute(sql, params)
            print(f"  OK    {t['codigo']}")
        inseridos += 1

    if DRY_RUN:
        print(f"\n[!] DRY-RUN: {inseridos} tipos seriam inseridos, {pulados} pulados.")
        conn.rollback()
    else:
        conn.commit()
        print(f"\n[OK] {inseridos} tipos inseridos, {pulados} pulados.")

    cursor.close()
    conn.close()
    print("=" * 60)


if __name__ == '__main__':
    run()
