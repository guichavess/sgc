"""
Importa dados da planilha 'Acompanhamento Fachada.xlsx' para a tabela identidade_visual_locais.
Fragmenta a coluna ENDEREÇO em: endereço, bairro e CEP.

Uso:
    python scripts/importar_identidade_visual.py                          # dry-run
    python scripts/importar_identidade_visual.py --executar               # aplica no banco
    python scripts/importar_identidade_visual.py --arquivo outra.xlsx     # planilha customizada
"""
import re
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from app import create_app
from app.extensions import db
from app.models.identidade_visual import IdentidadeVisualLocal


ROMAN_NUMERALS = {'Ii': 'II', 'Iii': 'III', 'Iv': 'IV', 'Vi': 'VI', 'Vii': 'VII', 'Viii': 'VIII', 'Ix': 'IX'}
PREPOSITIONS = {'Da', 'Das', 'De', 'Do', 'Dos', 'E'}


def _smart_title(text):
    """Title case preservando numerais romanos e preposições."""
    if not text or text == 'nan':
        return text
    result = text.title() if text.isupper() else text
    words = result.split()
    for i, w in enumerate(words):
        if w in ROMAN_NUMERALS:
            words[i] = ROMAN_NUMERALS[w]
        elif i > 0 and w in PREPOSITIONS:
            words[i] = w.lower()
    return ' '.join(words)


def extrair_cep(texto):
    """Extrai CEP (7-8 dígitos, com ou sem hífen) do texto."""
    m = re.search(r'(?:CEP[:\s]*)?(\d{5})-?(\d{2,3})\b', texto, re.IGNORECASE)
    if m:
        sufixo = m.group(2).ljust(3, '0')
        return f"{m.group(1)}-{sufixo}"
    m = re.search(r'(\d{5})-(\d{2,3})', texto)
    if m:
        sufixo = m.group(2).ljust(3, '0')
        return f"{m.group(1)}-{sufixo}"
    return None


def extrair_bairro(texto):
    """Extrai bairro do texto usando padrão 'Bairro' ou heurísticas."""
    m = re.search(r'Bairro[:\s;]*([A-Za-zÀ-ú\s]{2,30}?)(?:\s*[-,;\n]|\s*CEP|\s*\d{5}|\s*$)', texto, re.IGNORECASE)
    if m:
        bairro = m.group(1).strip()
        bairro = re.sub(r'\s+', ' ', bairro)
        palavras = bairro.split()
        if len(palavras) > 3:
            bairro = ' '.join(palavras[:3])
        if len(bairro) > 2:
            return bairro.title()

    m = re.search(r'[-–,]\s*Centro\b', texto, re.IGNORECASE)
    if m:
        return 'Centro'

    if re.search(r'\bcentro\b', texto, re.IGNORECASE) and len(texto) < 60:
        return 'Centro'

    return None


def limpar_endereco(texto, bairro, cep):
    """Remove bairro e CEP do endereço, limpando separadores residuais."""
    resultado = texto

    if cep:
        cep_limpo = cep.replace('-', '')
        resultado = re.sub(r'CEP[:\s]*' + cep_limpo[:5] + r'[-]?' + cep_limpo[5:], '', resultado, flags=re.IGNORECASE)
        resultado = re.sub(cep_limpo[:5] + r'[-]?' + cep_limpo[5:], '', resultado)

    if bairro and bairro.lower() != 'centro':
        resultado = re.sub(r'Bairro[:\s;]*' + re.escape(bairro), '', resultado, flags=re.IGNORECASE)
        padrao_bairro = re.escape(bairro)
        resultado = re.sub(r'Bairro[:\s;]*[A-Za-zÀ-ú\s]+?(?=\s*[-,;]|\s*CEP|\s*\d{5}|\s*$)', '', resultado, flags=re.IGNORECASE)

    resultado = re.sub(r'[,;.\s-]+$', '', resultado)
    resultado = re.sub(r'^\s*[,;-]\s*', '', resultado)
    resultado = re.sub(r'\s*[,;]\s*$', '', resultado)
    resultado = re.sub(r'\n+', ', ', resultado)
    resultado = re.sub(r'\s{2,}', ' ', resultado)
    resultado = resultado.strip(' ,;.-')

    if resultado.lower().startswith('endereço '):
        resultado = resultado[9:]

    return resultado.strip()


def processar_planilha(caminho):
    """Lê a planilha e retorna lista de dicts com campos tratados."""
    df = pd.read_excel(caminho)

    col_endereco = [c for c in df.columns if 'ENDERE' in c.upper()]
    if not col_endereco:
        print("ERRO: Coluna de endereço não encontrada na planilha.")
        sys.exit(1)
    col_endereco = col_endereco[0]

    registros = []
    for _, row in df.iterrows():
        cidade = str(row.get('CIDADE', '')).strip()
        local_nome = str(row.get('LOCAL', '')).strip()
        endereco_raw = str(row.get(col_endereco, '')).strip()

        if not cidade or cidade == 'nan':
            continue

        cep = extrair_cep(endereco_raw)
        bairro = extrair_bairro(endereco_raw)
        endereco = limpar_endereco(endereco_raw, bairro, cep)

        registros.append({
            'cidade': _smart_title(cidade),
            'local': _smart_title(local_nome),
            'endereco': endereco if endereco else None,
            'bairro': bairro,
            'cep': cep,
        })

    return registros


def main():
    parser = argparse.ArgumentParser(description='Importar planilha Identidade Visual')
    parser.add_argument('--executar', action='store_true', help='Aplica no banco (default: dry-run)')
    parser.add_argument('--arquivo', default=None, help='Caminho da planilha (default: Downloads/Acompanhamento Fachada.xlsx)')
    args = parser.parse_args()

    caminho = args.arquivo or os.path.expanduser(r'~\Downloads\Acompanhamento Fachada.xlsx')
    if not os.path.exists(caminho):
        print(f"ERRO: Arquivo não encontrado: {caminho}")
        sys.exit(1)

    registros = processar_planilha(caminho)

    print(f"\n{'='*80}")
    print(f"  Identidade Visual — Importação de Fachadas")
    print(f"  Arquivo: {caminho}")
    print(f"  Registros: {len(registros)}")
    print(f"  Modo: {'EXECUTAR' if args.executar else 'DRY-RUN'}")
    print(f"{'='*80}\n")

    for i, r in enumerate(registros, 1):
        print(f"  {i:2d}. {r['cidade']}")
        print(f"      Local:    {r['local']}")
        print(f"      Endereço: {r['endereco'] or '(vazio)'}")
        print(f"      Bairro:   {r['bairro'] or '(não identificado)'}")
        print(f"      CEP:      {r['cep'] or '(não identificado)'}")
        print()

    if not args.executar:
        print("  [DRY-RUN] Nenhuma alteração foi feita. Use --executar para importar.")
        return

    app = create_app()
    with app.app_context():
        db.session.execute(db.text("CREATE TABLE IF NOT EXISTS identidade_visual_locais ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "cidade VARCHAR(100) NOT NULL,"
            "`local` VARCHAR(200) NOT NULL,"
            "endereco VARCHAR(500),"
            "bairro VARCHAR(200),"
            "cep VARCHAR(10),"
            "custo DECIMAL(12,2),"
            "arquivo_nome VARCHAR(255),"
            "arquivo_caminho VARCHAR(500),"
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"))
        db.session.commit()

        existentes = IdentidadeVisualLocal.query.count()
        if existentes > 0:
            print(f"  AVISO: Tabela já contém {existentes} registros. Pulando inserção.")
            print(f"  Para reimportar, limpe a tabela primeiro (TRUNCATE identidade_visual_locais).")
            return

        for r in registros:
            local = IdentidadeVisualLocal(
                cidade=r['cidade'],
                tipo_local=r['local'],
                endereco=r['endereco'],
                bairro=r['bairro'],
                cep=r['cep'],
            )
            db.session.add(local)

        db.session.commit()
        print(f"  OK: {len(registros)} registros inseridos com sucesso.")


if __name__ == '__main__':
    main()
