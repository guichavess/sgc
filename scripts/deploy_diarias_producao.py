"""
Script de deploy para produção: Importação de Diárias + Normalização SEI.

Executa em sequência:
  1. Migração fase 1: cria diarias_itinerario_sei e copia dados (se não existir)
  2. Normalização: cria diarias_itinerario_documentos + diarias_itinerario_quadro_orcamentario
  3. Importação: importa processos históricos do SEI
  4. Relatório: lista processos importados e não importados com motivo
  5. Limpeza: remove diarias_itinerario_sei e colunas antigas (opcional)

Uso:
  python scripts/deploy_diarias_producao.py --dry-run            # simula tudo
  python scripts/deploy_diarias_producao.py --executar           # executa tudo
  python scripts/deploy_diarias_producao.py --relatorio          # apenas gera relatório
  python scripts/deploy_diarias_producao.py --executar --limpeza # + remove tabela/colunas antigas

IMPORTANTE: Antes de rodar, certifique-se de que o código já foi atualizado
(models, routes, templates, services) para usar get_doc/set_doc/has_doc.
"""
import argparse
import sys
import os
import time
import warnings
import urllib3

warnings.filterwarnings('ignore')
urllib3.disable_warnings()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db


def tabela_existe(cursor, db_name, tabela):
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """, (db_name, tabela))
    return cursor.fetchone()[0] > 0


def etapa_1_migracao_sei(dry_run=False):
    """Cria tabela diarias_itinerario_sei e copia dados (migração fase 1)."""
    print("\n" + "=" * 70)
    print("ETAPA 1: MIGRAÇÃO FASE 1 — Criar diarias_itinerario_sei")
    print("=" * 70)

    conn = db.engine.raw_connection()
    cursor = conn.cursor()
    db_name = db.engine.url.database

    try:
        if tabela_existe(cursor, db_name, 'diarias_itinerario_sei'):
            cursor.execute("SELECT COUNT(*) FROM diarias_itinerario_sei")
            count = cursor.fetchone()[0]
            print(f"  Tabela já existe ({count} registros). Pulando criação.")
            return True

        if dry_run:
            print("  [DRY-RUN] Criaria tabela diarias_itinerario_sei e copiaria dados.")
            return True

        # Importa e executa o script de migração fase 1
        from scripts.migrar_diarias_sei_fase1 import criar_e_copiar
        criar_e_copiar(dry_run=False)
        return True

    except ImportError:
        # Se o script fase 1 não existe, pular (já foi feito antes)
        print("  Script fase 1 não encontrado. Verificando se tabela existe...")
        if tabela_existe(cursor, db_name, 'diarias_itinerario_sei'):
            print("  Tabela diarias_itinerario_sei já existe. OK.")
            return True
        else:
            print("  AVISO: Tabela diarias_itinerario_sei não existe.")
            print("  A normalização buscará dados diretamente de diarias_itinerario.")
            return True
    except Exception as e:
        print(f"  ERRO na migração: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def etapa_2_normalizacao(dry_run=False):
    """Cria tabelas normalizadas (documentos + quadro) e migra dados."""
    print("\n" + "=" * 70)
    print("ETAPA 2: NORMALIZAÇÃO — documentos + quadro orçamentário")
    print("=" * 70)

    try:
        from scripts.migrar_diarias_sei import normalizar
        normalizar(dry_run=dry_run)
        return True
    except Exception as e:
        print(f"  ERRO na normalização: {e}")
        return False


def etapa_3_importacao(executar=False):
    """Importa processos históricos do SEI."""
    print("\n" + "=" * 70)
    print("ETAPA 3: IMPORTAÇÃO — Processos históricos do SEI")
    print("=" * 70)

    import requests
    from app.services.sei_auth import gerar_token_sei_admin
    from app.models.diaria import DiariasItinerario

    # Carrega lista de processos
    arquivo_processos = os.path.join(os.path.dirname(__file__), '..', 'processos de diárias.txt')
    if not os.path.exists(arquivo_processos):
        print(f"  ERRO: Arquivo não encontrado: {arquivo_processos}")
        return {'ok': 0, 'erros': 0, 'sem_requisicao': 0, 'duplicados': 0, 'total': 0}

    with open(arquivo_processos, 'r', encoding='utf-8') as f:
        processos = [l.strip() for l in f if l.strip()]

    print(f"  Total de processos na lista: {len(processos)}")

    # Verifica duplicados
    existentes = {r[0] for r in db.session.execute(
        db.text("SELECT sei_protocolo FROM diarias_itinerario WHERE sei_protocolo IS NOT NULL")
    ).fetchall()}

    duplicados = 0
    a_processar = []
    for p in processos:
        protocolo_limpo = ''.join(filter(str.isdigit, p))
        found = any(protocolo_limpo in ''.join(filter(str.isdigit, e)) for e in existentes)
        if found:
            duplicados += 1
        else:
            a_processar.append(p)

    print(f"  Já importados: {duplicados}")
    print(f"  A processar: {len(a_processar)}")

    if not a_processar:
        print("  Nenhum processo novo para importar.")
        return {'ok': 0, 'erros': 0, 'sem_requisicao': 0, 'duplicados': duplicados, 'total': len(processos)}

    if not executar:
        print("  [DRY-RUN] Não executando importação. Use --executar para importar.")
        return {'ok': 0, 'erros': 0, 'sem_requisicao': len(a_processar), 'duplicados': duplicados, 'total': len(processos)}

    from scripts.importar_diarias_sei import processar_processo

    print("\n  Autenticando no SEI...")
    token = gerar_token_sei_admin()
    if not token:
        print("  ERRO FATAL: Falha na autenticação SEI.")
        return {'ok': 0, 'erros': len(a_processar), 'sem_requisicao': 0, 'duplicados': duplicados, 'total': len(processos)}

    ok = 0
    erros = 0
    sem_req = 0
    for i, processo in enumerate(a_processar, 1):
        print(f"\n  [{i}/{len(a_processar)}]", end='')
        try:
            resultado = processar_processo(token, processo, executar=True)
            if resultado:
                ok += 1
            else:
                erros += 1
        except Exception as e:
            erro_str = str(e)
            if 'Sem documento' in erro_str or 'IdSerie 532' in erro_str:
                sem_req += 1
            else:
                erros += 1
            print(f"  EXCEÇÃO: {e}")

        if i < len(a_processar):
            time.sleep(1.5)

    print(f"\n  Resultado: {ok} OK, {erros} erros, {sem_req} sem requisição")
    return {'ok': ok, 'erros': erros, 'sem_requisicao': sem_req, 'duplicados': duplicados, 'total': len(processos)}


def etapa_4_relatorio():
    """Gera relatório completo de processos importados vs não importados."""
    print("\n" + "=" * 70)
    print("ETAPA 4: RELATÓRIO FINAL")
    print("=" * 70)

    import requests
    from app.services.sei_auth import gerar_token_sei_admin

    # Carrega lista
    arquivo_processos = os.path.join(os.path.dirname(__file__), '..', 'processos de diárias.txt')
    with open(arquivo_processos, 'r', encoding='utf-8') as f:
        processos = [l.strip() for l in f if l.strip()]

    # Busca importados
    importados = db.session.execute(
        db.text("SELECT sei_protocolo, id, etapa_atual_id FROM diarias_itinerario WHERE sei_protocolo IS NOT NULL")
    ).fetchall()

    protocolos_importados = {}
    for prot, itin_id, etapa in importados:
        protocolos_importados[''.join(filter(str.isdigit, prot))] = {
            'protocolo': prot, 'id': itin_id, 'etapa': etapa
        }

    importados_list = []
    nao_importados_list = []

    for p in processos:
        protocolo_limpo = ''.join(filter(str.isdigit, p))
        if protocolo_limpo in protocolos_importados:
            info = protocolos_importados[protocolo_limpo]
            importados_list.append({'processo': p, **info})
        else:
            nao_importados_list.append(p)

    # Verifica motivo dos não importados via API SEI
    motivos = {}
    if nao_importados_list:
        print(f"\n  Verificando {len(nao_importados_list)} processos não importados via API SEI...")
        try:
            token = gerar_token_sei_admin()
            if token:
                BASE_URL = 'https://api.sei.pi.gov.br'
                UNIDADE_SEAD = '110006213'
                headers = {'token': token, 'Accept': 'application/json'}

                for proc in nao_importados_list:
                    protocolo_limpo = ''.join(filter(str.isdigit, proc))
                    try:
                        url = f'{BASE_URL}/v1/unidades/{UNIDADE_SEAD}/procedimentos/documentos'
                        resp = requests.get(url, params={'protocolo_procedimento': protocolo_limpo, 'quantidade': 1000},
                                            headers=headers, timeout=60, verify=False)
                        data = resp.json()
                        docs = data.get('Documentos', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                        series = set(d.get('IdSerie', '') for d in docs)
                        tem_532 = '532' in series

                        if not tem_532:
                            motivos[proc] = f'Sem Requisição de Diárias (IdSerie 532) — {len(docs)} docs presentes'
                        else:
                            motivos[proc] = f'Tem Requisição (532) mas falhou na importação — verificar manualmente'
                    except Exception as e:
                        motivos[proc] = f'Erro ao consultar API: {e}'
                    time.sleep(0.5)
        except Exception:
            pass

    # Imprime relatório
    print("\n" + "=" * 70)
    print("PROCESSOS IMPORTADOS COM SUCESSO")
    print("=" * 70)
    print(f"  Total: {len(importados_list)}")
    for item in importados_list:
        print(f"  {item['processo']} — itinerario.id={item['id']}, etapa={item['etapa']}")

    print("\n" + "=" * 70)
    print("PROCESSOS NÃO IMPORTADOS")
    print("=" * 70)
    print(f"  Total: {len(nao_importados_list)}")
    for proc in nao_importados_list:
        motivo = motivos.get(proc, 'Motivo desconhecido')
        print(f"  {proc} — {motivo}")

    # Estatísticas das tabelas
    print("\n" + "=" * 70)
    print("ESTATÍSTICAS DO BANCO DE DADOS")
    print("=" * 70)
    tabelas_stats = [
        ('diarias_itinerario', 'Itinerários'),
        ('diarias_itinerario_documentos', 'Documentos SEI (normalizado)'),
        ('diarias_itinerario_quadro_orcamentario', 'Quadro Orçamentário'),
        ('diarias_itinerario_sei', 'SEI legado (a remover)'),
        ('diarias_servidores', 'Servidores'),
        ('diarias_itens_itinerario', 'Itens (pessoas/viagem)'),
        ('diarias_historico_movimentacoes', 'Histórico movimentações'),
        ('diarias_controle_viagens', 'Controle viagens'),
        ('diarias_controle_servidores', 'Controle servidores'),
        ('diarias_controle_prestacao', 'Controle prestação'),
        ('diarias_cargos', 'Cargos'),
        ('diarias_paradas', 'Paradas'),
        ('diarias_cotacoes', 'Cotações'),
    ]
    for tabela, desc in tabelas_stats:
        try:
            result = db.session.execute(db.text(f"SELECT COUNT(*) FROM `{tabela}`"))
            count = result.fetchone()[0]
            print(f"  {desc:<40} ({tabela}): {count}")
        except Exception:
            print(f"  {desc:<40} ({tabela}): NÃO EXISTE")

    return {
        'importados': len(importados_list),
        'nao_importados': len(nao_importados_list),
        'total': len(processos),
    }


def etapa_5_limpeza(dry_run=False):
    """Remove tabela legada diarias_itinerario_sei e colunas antigas."""
    print("\n" + "=" * 70)
    print("ETAPA 5: LIMPEZA — Remover tabela/colunas antigas")
    print("=" * 70)

    from scripts.migrar_diarias_sei import drop_sei_table, drop_columns

    print("\n  5a. Removendo tabela diarias_itinerario_sei...")
    drop_sei_table(dry_run=dry_run)

    print("\n  5b. Removendo colunas sei_* de diarias_itinerario...")
    drop_columns(dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description='Deploy diárias em produção')
    parser.add_argument('--executar', action='store_true',
                        help='Executa importação (default: DRY-RUN)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simula tudo sem alterar banco')
    parser.add_argument('--limpeza', action='store_true',
                        help='Também remove tabela/colunas antigas após migração')
    parser.add_argument('--relatorio', action='store_true',
                        help='Apenas gera relatório (sem importar/migrar)')
    args = parser.parse_args()

    print("=" * 70)
    print("DEPLOY DIÁRIAS — PRODUÇÃO")
    print(f"Modo: {'EXECUTAR' if args.executar else 'DRY-RUN'}")
    print("=" * 70)

    app = create_app()
    with app.app_context():
        if args.relatorio:
            etapa_4_relatorio()
            return

        # Etapa 1: Migração fase 1 (diarias_itinerario → diarias_itinerario_sei)
        ok = etapa_1_migracao_sei(dry_run=args.dry_run)
        if not ok:
            print("\nERRO: Migração fase 1 falhou. Abortando.")
            return

        # Etapa 2: Normalização (sei → documentos + quadro)
        ok = etapa_2_normalizacao(dry_run=args.dry_run)
        if not ok:
            print("\nERRO: Normalização falhou. Abortando.")
            return

        # Etapa 3: Importação
        stats = etapa_3_importacao(executar=args.executar)

        # Etapa 4: Relatório
        etapa_4_relatorio()

        # Etapa 5: Limpeza (opcional)
        if args.limpeza:
            etapa_5_limpeza(dry_run=args.dry_run)

        print("\n" + "=" * 70)
        print("DEPLOY CONCLUÍDO")
        print("=" * 70)


if __name__ == '__main__':
    main()
