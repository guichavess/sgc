"""
Importa municípios do Piauí a partir do CSV do IBGE.
Cria tabela municipios_pi e associa registros existentes de identidade_visual_locais.

Uso:
    python scripts/importar_municipios_pi.py                  # DRY-RUN
    python scripts/importar_municipios_pi.py --executar       # aplica
"""
import csv
import html
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CSV_PATH = '/tmp/1ea51a9afb80a30312ac5186a4804b80.csv'


def main():
    executar = '--executar' in sys.argv

    from app import create_app
    from app.extensions import db
    from app.models.identidade_visual import MunicipioPiaui, IdentidadeVisualLocal

    app = create_app()

    with app.app_context():
        # --- Fase 0: Criar tabela + coluna se não existem ---
        from sqlalchemy import text, inspect as sa_inspect
        insp = sa_inspect(db.engine)
        if 'municipios_pi' not in insp.get_table_names():
            if executar:
                db.session.execute(text("""
                    CREATE TABLE municipios_pi (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        nome VARCHAR(100) NOT NULL UNIQUE,
                        codigo_ibge VARCHAR(10) NOT NULL UNIQUE,
                        gentilico VARCHAR(100)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                db.session.commit()
                print('Tabela municipios_pi criada.')
            else:
                print('[DRY-RUN] Tabela municipios_pi será criada.')

        cols = [c['name'] for c in insp.get_columns('identidade_visual_locais')]
        if 'municipio_id' not in cols:
            if executar:
                db.session.execute(text("""
                    ALTER TABLE identidade_visual_locais
                    ADD COLUMN municipio_id INT NULL,
                    ADD INDEX idx_iv_municipio_id (municipio_id)
                """))
                db.session.commit()
                print('Coluna municipio_id adicionada.')
            else:
                print('[DRY-RUN] Coluna municipio_id será adicionada.')

        # --- Fase 1: Parse CSV ---
        municipios = []
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Line 1 = title, Line 2 = header, Lines 3+ = data
        for line in lines[2:]:
            line = line.strip()
            if not line or line.startswith('<'):
                continue
            parts = line.split(',')
            if len(parts) < 3:
                continue
            nome = html.unescape(parts[0]).strip()
            codigo = html.unescape(parts[1]).strip()
            gentilico = html.unescape(parts[2]).strip()
            if not nome or not codigo.isdigit():
                continue
            municipios.append((nome, codigo, gentilico))

        print(f'Municípios encontrados no CSV: {len(municipios)}')

        if not executar:
            print('\n--- DRY-RUN (use --executar para aplicar) ---')
            for nome, codigo, gentilico in municipios[:10]:
                print(f'  {codigo} | {nome} | {gentilico}')
            if len(municipios) > 10:
                print(f'  ... e mais {len(municipios) - 10}')

        # --- Fase 2: Inserir na tabela ---
        if executar:
            inseridos = 0
            for nome, codigo, gentilico in municipios:
                existente = MunicipioPiaui.query.filter_by(codigo_ibge=codigo).first()
                if existente:
                    continue
                m = MunicipioPiaui(nome=nome, codigo_ibge=codigo, gentilico=gentilico or None)
                db.session.add(m)
                inseridos += 1
            db.session.commit()
            print(f'Inseridos: {inseridos} municípios')

        # --- Fase 3: Associar registros existentes ---
        mapa = {}
        for nome, codigo, _ in municipios:
            mapa[nome.upper()] = (nome, codigo)

        rows = db.session.execute(text(
            'SELECT id, cidade FROM identidade_visual_locais'
        )).fetchall()

        associados = 0
        nao_encontrados = []
        for row_id, cidade in rows:
            cidade_upper = (cidade or '').strip().upper()
            if cidade_upper in mapa:
                nome_correto, codigo = mapa[cidade_upper]
                if executar:
                    mun = MunicipioPiaui.query.filter_by(codigo_ibge=codigo).first()
                    if mun:
                        db.session.execute(text(
                            'UPDATE identidade_visual_locais SET municipio_id = :mid, cidade = :nome WHERE id = :lid'
                        ), {'mid': mun.id, 'nome': nome_correto, 'lid': row_id})
                        associados += 1
                else:
                    print(f'  MATCH: "{cidade}" -> {nome_correto} ({codigo})')
                    associados += 1
            else:
                nao_encontrados.append(cidade)

        if executar:
            db.session.commit()
            print(f'Associados: {associados} registros')
        else:
            print(f'\nAssociações possíveis: {associados}')

        if nao_encontrados:
            print(f'NÃO encontrados ({len(nao_encontrados)}):')
            for c in set(nao_encontrados):
                print(f'  - "{c}"')


if __name__ == '__main__':
    main()
