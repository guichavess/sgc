"""
Script de importação: carrega servidores do CSV para a tabela diarias_servidores.

Colunas esperadas no CSV:
    matricula, cpf, cargo, numBanco, numAgenciaBanco, numOpBanco,
    numContaBanco, nome_orgao, nome_entidade, nome_superintendencia

Uso:
    python scripts/importar_servidores.py "C:\\caminho\\servidores.csv"
    python scripts/importar_servidores.py "C:\\caminho\\servidores.csv" --limpar   # Limpa tabela antes
"""
import argparse
import csv
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.diaria import DiariasServidor


def importar_csv(caminho_csv, limpar=False):
    """Importa servidores do CSV para a tabela diarias_servidores."""
    if not os.path.isfile(caminho_csv):
        print(f"   ERRO: Arquivo não encontrado: {caminho_csv}")
        return

    if limpar:
        print("   Limpando tabela diarias_servidores...")
        DiariasServidor.query.delete()
        db.session.commit()

    # Ler CSV
    print(f"   Lendo CSV: {caminho_csv}")
    with open(caminho_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        registros = []
        matriculas_vistas = set()
        duplicados = 0
        linhas = 0

        for row in reader:
            linhas += 1
            matricula = (row.get('matricula') or '').strip()
            cpf = (row.get('cpf') or '').strip()
            if not matricula or not cpf:
                continue

            # Deduplica por matrícula (mantém o primeiro)
            if matricula in matriculas_vistas:
                duplicados += 1
                continue
            matriculas_vistas.add(matricula)

            cargo = (row.get('cargo') or '').strip() or None
            num_banco = (row.get('numBanco') or '').strip() or None
            num_agencia_banco = (row.get('numAgenciaBanco') or '').strip() or None
            num_op_banco = (row.get('numOpBanco') or '').strip() or None
            num_conta_banco = (row.get('numContaBanco') or '').strip() or None
            nome_orgao = (row.get('nome_orgao') or '').strip() or None
            nome_entidade = (row.get('nome_entidade') or '').strip() or None
            nome_superintendencia = (row.get('nome_superintendencia') or '').strip() or None

            registros.append(DiariasServidor(
                matricula=matricula,
                cpf=cpf,
                cargo=cargo,
                num_banco=num_banco,
                num_agencia_banco=num_agencia_banco,
                num_op_banco=num_op_banco,
                num_conta_banco=num_conta_banco,
                nome_orgao=nome_orgao,
                nome_entidade=nome_entidade,
                nome_superintendencia=nome_superintendencia,
            ))

    print(f"   Lidas {linhas} linhas, {len(registros)} servidores únicos ({duplicados} duplicados por matrícula)")

    # Inserir em batch
    print("   Inserindo no banco...")
    batch_size = 500
    inseridos = 0
    atualizados = 0

    for i in range(0, len(registros), batch_size):
        batch = registros[i:i + batch_size]
        for srv in batch:
            existente = DiariasServidor.query.filter_by(matricula=srv.matricula).first()
            if existente:
                existente.cpf = srv.cpf
                existente.cargo = srv.cargo
                existente.num_banco = srv.num_banco
                existente.num_agencia_banco = srv.num_agencia_banco
                existente.num_op_banco = srv.num_op_banco
                existente.num_conta_banco = srv.num_conta_banco
                existente.nome_orgao = srv.nome_orgao
                existente.nome_entidade = srv.nome_entidade
                existente.nome_superintendencia = srv.nome_superintendencia
                atualizados += 1
            else:
                db.session.add(srv)
                inseridos += 1
        db.session.commit()
        print(f"   ... processados {min(i + batch_size, len(registros))}/{len(registros)}")

    print(f"\n   Inseridos: {inseridos} | Atualizados: {atualizados}")
    print("   Importação concluída com sucesso!")


def main():
    parser = argparse.ArgumentParser(description='Importar servidores do CSV')
    parser.add_argument('csv_path', help='Caminho do arquivo CSV de servidores')
    parser.add_argument('--limpar', action='store_true',
                        help='Limpar tabela antes de importar')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        importar_csv(args.csv_path, limpar=args.limpar)


if __name__ == '__main__':
    main()
