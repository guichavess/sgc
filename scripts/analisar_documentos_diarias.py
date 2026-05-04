"""
Fase 2 automatizada: Analisa documentos HTML dos processos de diarias e gera _extracted.json.

Extrai dados estruturados dos documentos baixados usando BeautifulSoup,
aproveitando o formato padronizado dos documentos SEI.

Uso:
    # Analisar um processo
    python scripts/analisar_documentos_diarias.py --processo 00002.000065_2026-82

    # Analisar todos os pendentes
    python scripts/analisar_documentos_diarias.py

    # Limitar quantidade
    python scripts/analisar_documentos_diarias.py --limit 10

    # Forcar re-analise
    python scripts/analisar_documentos_diarias.py --force
"""

import sys
import os
import re
import json
import argparse
from datetime import datetime
from decimal import Decimal, InvalidOperation

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from bs4 import BeautifulSoup

DEST_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'diarias')
)


# ═══════════════════════════════════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════════════════════════════════

def limpar_texto(texto):
    """Remove espacos extras e normaliza texto."""
    if not texto:
        return ''
    return re.sub(r'\s+', ' ', texto).strip()


def extrair_valor(texto):
    """Extrai valor monetario de texto (ex: 'R$ 1.750,00' -> '1750.00')."""
    if not texto:
        return None
    # Remover R$, espacos
    s = re.sub(r'[R$\s]', '', texto)
    # trocar ponto de milhar e virgula decimal
    s = s.replace('.', '').replace(',', '.')
    try:
        return str(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def extrair_texto_html(caminho):
    """Extrai texto limpo de arquivo HTML."""
    with open(caminho, 'r', encoding='utf-8', errors='replace') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    for tag in soup(['style', 'script', 'head']):
        tag.decompose()
    return soup.get_text(separator='\n', strip=True)


def buscar_arquivo(pasta, prefixo_serie):
    """Busca arquivo que comeca com o prefixo da serie."""
    for f in os.listdir(pasta):
        if f.startswith(f'{prefixo_serie}_') and not f.startswith('_'):
            return os.path.join(pasta, f)
    return None


def buscar_arquivos(pasta, prefixo_serie):
    """Busca todos os arquivos que comecam com o prefixo da serie."""
    resultado = []
    for f in sorted(os.listdir(pasta)):
        if f.startswith(f'{prefixo_serie}_') and not f.startswith('_'):
            resultado.append(os.path.join(pasta, f))
    return resultado


# ═══════════════════════════════════════════════════════════════════════
# PARSERS DE DOCUMENTOS
# ═══════════════════════════════════════════════════════════════════════

def parsear_requisicao_diarias(caminho):
    """Extrai dados da Requisicao de Diarias (532)."""
    texto = extrair_texto_html(caminho)
    dados = {
        'tipo': 'requisicao_diarias',
        'integrantes': [],
    }

    # Numero da requisicao
    m = re.search(r'REQUISI[ÇC][AÃ]O DE DI[AÁ]RIAS\s*N[º°]?\s*(\d+/\d+)', texto, re.IGNORECASE)
    if m:
        dados['numero_requisicao'] = m.group(1)

    # Objetivo
    m = re.search(r'OBJETIVO DA VIAGEM[:\s]*(.+?)(?=TRECHO|$)', texto, re.DOTALL | re.IGNORECASE)
    if m:
        obj = limpar_texto(m.group(1))
        # Remover assinaturas do final
        obj = re.split(r'\((?:A|a)ssinado', obj)[0].strip()
        dados['objetivo'] = obj

    # Trecho
    m = re.search(r'TRECHO[:\s]*(.+?)(?=PER[IÍ]ODO|$)', texto, re.DOTALL | re.IGNORECASE)
    if m:
        dados['trecho'] = limpar_texto(m.group(1))

    # Periodo
    m = re.search(r'PER[IÍ]ODO[:\s]*(.+?)(?=\(|Documento|$)', texto, re.DOTALL | re.IGNORECASE)
    if m:
        periodo = limpar_texto(m.group(1))
        # Extrair datas
        datas = re.findall(r'(\d{2}[/\.]\d{2}[/\.]\d{4})', periodo)
        if len(datas) >= 2:
            dados['data_viagem'] = datas[0].replace('.', '/')
            dados['data_retorno'] = datas[1].replace('.', '/')
        elif len(datas) == 1:
            dados['data_viagem'] = datas[0].replace('.', '/')
        # Tentar formato "02 a 07/02/2026"
        if not datas:
            m2 = re.search(r'(\d{2})\s*a\s*(\d{2})/(\d{2})/(\d{4})', periodo)
            if m2:
                dados['data_viagem'] = f"{m2.group(1)}/{m2.group(3)}/{m2.group(4)}"
                dados['data_retorno'] = f"{m2.group(2)}/{m2.group(3)}/{m2.group(4)}"

    # Valor total
    m = re.search(r'VALOR\s+TOTAL\s*\(R\$\)[^\d]*([\d.,]+)', texto, re.IGNORECASE)
    if m:
        dados['valor_total'] = extrair_valor(m.group(1))
    # Fallback: ultimo valor na tabela
    if not dados.get('valor_total'):
        valores = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', texto)
        if valores:
            dados['valor_total'] = extrair_valor(valores[-1])

    # Integrantes (parsear tabela)
    # Padrao: Matricula, Nome, Cargo, Vinculo, CPF, Banco, Qtd, Valor Unit, Valor Total
    # Regex para CPF
    cpfs = re.findall(r'(\d{3}\.\d{3}\.\d{3}-\d{2})', texto)
    matriculas = re.findall(r'(\d{3,6}[\.\-]\d{1,2}(?:-\d)?)', texto)

    # Extrair blocos de integrantes usando CPF como ancora
    for cpf in cpfs:
        integrante = {'cpf': cpf}

        # Buscar nome antes do CPF (na mesma regiao)
        pos_cpf = texto.find(cpf)
        bloco = texto[max(0, pos_cpf - 500):pos_cpf + 200]

        # Matricula
        m_mat = re.search(r'(\d{3,6}[\.\-]?\d{1,2}(?:-\d)?)', bloco)
        if m_mat:
            integrante['matricula'] = m_mat.group(1)

        # Nome (geralmente em MAIUSCULAS antes do cargo)
        nomes = re.findall(r'([A-ZÁÉÍÓÚÂÊÎÔÛÃÕÇ][A-ZÁÉÍÓÚÂÊÎÔÛÃÕÇ\s]{5,})', bloco)
        for nome in nomes:
            nome = nome.strip()
            # Filtrar palavras-chave
            if any(kw in nome for kw in ['GOVERNO', 'SECRETARIA', 'REQUISIÇÃO', 'MATRÍCULA',
                                          'CARGO', 'EFETIVO', 'BANCO', 'VALOR', 'OBJETIVO',
                                          'TRECHO', 'DESPACHO', 'AUTORIZO']):
                continue
            if len(nome) > 5:
                integrante['nome'] = nome
                break

        # Cargo
        cargos = ['Superintendente', 'Gerente', 'Assessor', 'Diretor', 'Coordenador',
                   'Analista', 'Técnico', 'Secretário', 'Chefe', 'Presidente', 'Assistente']
        for cargo in cargos:
            if cargo.lower() in bloco.lower():
                integrante['cargo'] = cargo
                break

        # Vinculo
        for vinc in ['Efetivo e Comissionado', 'Comissionado', 'Efetivo', 'Terceirizado', 'Contratado']:
            if vinc.lower() in bloco.lower():
                integrante['vinculo'] = vinc
                break

        # Banco/Agencia/Conta
        m_ag = re.search(r'Ag[\.:\s]*(\S+)', bloco, re.IGNORECASE)
        if m_ag:
            integrante['banco_agencia'] = m_ag.group(1)
        m_cc = re.search(r'(?:C/C|Conta)[\.:\s]*(\S+)', bloco, re.IGNORECASE)
        if m_cc:
            integrante['banco_conta'] = m_cc.group(1)

        # Quantidade diarias
        m_qtd = re.search(r'(\d+(?:\s*e\s*1/2)?(?:,5)?)\s*\n?\s*\d{2,3}[,.]', bloco)
        if m_qtd:
            qtd_str = m_qtd.group(1)
            if '1/2' in qtd_str:
                qtd = float(re.search(r'(\d+)', qtd_str).group(1)) + 0.5
            elif ',5' in qtd_str:
                qtd = float(qtd_str.replace(',', '.'))
            else:
                qtd = float(qtd_str)
            integrante['qtd_diarias'] = qtd

        # Valor unitario e total
        valores_bloco = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', bloco)
        if len(valores_bloco) >= 2:
            integrante['valor_unitario'] = extrair_valor(valores_bloco[-2])
            integrante['valor_total'] = extrair_valor(valores_bloco[-1])
        elif len(valores_bloco) == 1:
            integrante['valor_unitario'] = extrair_valor(valores_bloco[0])

        dados['integrantes'].append(integrante)

    # Tipo de viagem (inferir do trecho)
    trecho = dados.get('trecho', '').upper()
    if 'PI/' in trecho and trecho.count('/') <= 2:
        # Verificar se destino e outro estado
        estados = ['SP', 'RJ', 'DF', 'MG', 'BA', 'CE', 'PE', 'PR', 'AM', 'PA', 'GO', 'MA',
                   'RN', 'AL', 'PB', 'RS', 'SC', 'ES', 'MS', 'MT', 'TO', 'AP', 'RR', 'RO', 'AC', 'SE']
        for uf in estados:
            if f'-{uf}' in trecho or f'/{uf}' in trecho or f' {uf}' in trecho:
                if uf != 'PI':
                    dados['tipo_itinerario'] = 2  # Nacional
                    break
        else:
            dados['tipo_itinerario'] = 1  # Estadual

    return dados


def parsear_quadro_orcamentario(caminho):
    """Extrai dados do Quadro Orcamentario (723)."""
    texto = extrair_texto_html(caminho)
    dados = {}

    campos = {
        'ug': r'UG[:\s]*(\d+)',
        'funcao': r'FUN[CÇ][AÃ]O[:\s]*(\d+)',
        'subfuncao': r'SUBFUN[CÇ][AÃ]O[:\s]*(\d+)',
        'programa': r'PROGRAMA[:\s]*(\d+)',
        'plano_interno': r'PLANO\s+INTERNO[:\s]*(\d+)',
        'fonte_recursos': r'FONTE\s+DE\s+RECURSOS[:\s]*(\d+)',
        'natureza_despesa': r'NATUREZA\s+D[AE]\s+DESPESA[:\s]*([\d.]+)',
    }

    for campo, regex in campos.items():
        m = re.search(regex, texto, re.IGNORECASE)
        if m:
            dados[campo] = m.group(1)

    # Valores
    m = re.search(r'VALOR\s+INICIAL\s+D[AO]\s+NOTA\s+DE\s+RESERVA[:\s]*(?:SALDO[^R]*)?R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m:
        dados['valor_inicial_nr'] = extrair_valor(m.group(1))
    else:
        # Tentar formato alternativo
        valores = re.findall(r'R\$\s*([\d.,]+)', texto)
        if len(valores) >= 4:
            dados['valor_inicial_nr'] = extrair_valor(valores[0])
            dados['saldo_nr'] = extrair_valor(valores[1])
            dados['valor_despesa'] = extrair_valor(valores[2])
            dados['saldo_atual_nr'] = extrair_valor(valores[3])

    m = re.search(r'SALDO\s+D[AO]\s+NOTA\s+DE\s+RESERVA[:\s]*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m and 'saldo_nr' not in dados:
        dados['saldo_nr'] = extrair_valor(m.group(1))

    m = re.search(r'VALOR\s+D[AO]\s+DESPESA[:\s]*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m and 'valor_despesa' not in dados:
        dados['valor_despesa'] = extrair_valor(m.group(1))

    m = re.search(r'SALDO\s+ATUAL\s+D[AO]\s+NOTA\s+DE\s+RESERVA[:\s]*R\$\s*([\d.,]+)', texto, re.IGNORECASE)
    if m and 'saldo_atual_nr' not in dados:
        dados['saldo_atual_nr'] = extrair_valor(m.group(1))

    return dados


def extrair_doc_sei_info(metadata, serie_id):
    """Extrai sei_id e sei_formatado de um documento no metadata."""
    for doc in metadata.get('documentos', []):
        if str(doc.get('id_serie')) == str(serie_id) and doc.get('baixado'):
            return {
                'sei_id': str(doc.get('id_documento', '')),
                'sei_formatado': str(doc.get('documento_formatado', '')),
            }
    return None


def extrair_todos_docs_sei(metadata):
    """Extrai referencia de todos os documentos SEI relevantes."""
    # Mapeamento serie -> tipo_documento
    serie_tipo = {
        '532': 'requisicao', '2975': 'requisicao_passagens',
        '723': 'quadro_orcamentario', '425': 'nota_reserva',
        '419': 'nota_empenho', '420': 'nl', '421': 'pd', '422': 'ob', '423': 'np',
        '574': 'autorizacao', '269': 'autorizacao',
        '461': 'analise_pagamento', '5': 'despacho_nci', '2987': 'despacho_sga',
        '272': 'memorando_cotacoes', '2977': 'escolha_passagens', '543': 'escolha_passagens',
        '1908': 'relatorio_viagem', '261': 'relatorio_viagem', '1014': 'relatorio_viagem',
        '7': 'analise_habilitacao',
    }
    # Series de memorando
    series_memo = {'534', '1907', '2186', '2187', '2188', '2189', '2195',
                   '2986', '3517', '3550', '3591', '3950', '12'}

    docs = {}
    for doc in metadata.get('documentos', []):
        serie = str(doc.get('id_serie', ''))
        if not doc.get('baixado'):
            continue

        if serie in series_memo:
            tipo = 'memorando'
        elif serie in serie_tipo:
            tipo = serie_tipo[serie]
        else:
            continue

        # Pegar o primeiro de cada tipo
        if tipo not in docs:
            docs[tipo] = {
                'sei_id': str(doc.get('id_documento', '')),
                'sei_formatado': str(doc.get('documento_formatado', '')),
            }

    return docs


def inferir_tipo_solicitacao(metadata, dados_req):
    """Infere tipo_solicitacao_id baseado nos documentos presentes."""
    series_presentes = {str(d.get('id_serie', '')) for d in metadata.get('documentos', [])}

    tem_req_diarias = '532' in series_presentes
    tem_req_passagens = '2975' in series_presentes
    tem_cotacoes = '272' in series_presentes

    if tem_req_diarias and (tem_req_passagens or tem_cotacoes):
        return 2  # Diarias + Passagens
    elif tem_req_passagens and not tem_req_diarias:
        return 3  # Apenas Passagens
    else:
        return 1  # Apenas Diarias


# ═══════════════════════════════════════════════════════════════════════
# PROCESSAMENTO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════

def analisar_processo(pasta_path, protocolo):
    """Analisa todos os documentos de um processo e gera dados extraidos."""
    metadata_path = os.path.join(pasta_path, '_metadata.json')
    if not os.path.exists(metadata_path):
        return None

    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    resultado = {
        'processo': protocolo,
        'extraction_timestamp': datetime.now().isoformat(),
        'confidence': 'medium',
        'notes': '',
        'itinerario': {},
        'integrantes': [],
        'quadro_orcamentario': {},
        'documentos_sei': {},
    }

    notas = []

    # ── 1. Parsear Requisicao de Diarias (532) ──
    arq_req = buscar_arquivo(pasta_path, '532')
    if arq_req and arq_req.endswith('.html'):
        try:
            dados_req = parsear_requisicao_diarias(arq_req)
            resultado['itinerario']['objetivo'] = dados_req.get('objetivo')
            resultado['itinerario']['tipo_itinerario'] = dados_req.get('tipo_itinerario')

            if dados_req.get('data_viagem'):
                # Converter DD/MM/YYYY para YYYY-MM-DD
                try:
                    dt = datetime.strptime(dados_req['data_viagem'], '%d/%m/%Y')
                    resultado['itinerario']['data_viagem'] = dt.strftime('%Y-%m-%d')
                except ValueError:
                    resultado['itinerario']['data_viagem'] = dados_req['data_viagem']

            if dados_req.get('data_retorno'):
                try:
                    dt = datetime.strptime(dados_req['data_retorno'], '%d/%m/%Y')
                    resultado['itinerario']['data_retorno'] = dt.strftime('%Y-%m-%d')
                except ValueError:
                    resultado['itinerario']['data_retorno'] = dados_req['data_retorno']

            resultado['itinerario']['valor_total'] = dados_req.get('valor_total')
            resultado['integrantes'] = dados_req.get('integrantes', [])

            # Origem/destino do trecho
            trecho = dados_req.get('trecho', '')
            if trecho:
                resultado['itinerario']['trecho'] = trecho

        except Exception as e:
            notas.append(f'Erro parsear requisicao: {e}')
    else:
        notas.append('Sem requisicao de diarias (532)')

    # ── 2. Inferir tipo_solicitacao ──
    resultado['itinerario']['tipo_solicitacao_id'] = inferir_tipo_solicitacao(
        metadata, resultado.get('itinerario', {}))

    # ── 3. Parsear Quadro Orcamentario (723) ──
    arqs_qo = buscar_arquivos(pasta_path, '723')
    if arqs_qo:
        # Usar o ultimo (mais recente)
        arq_qo = arqs_qo[-1]
        if arq_qo.endswith('.html'):
            try:
                resultado['quadro_orcamentario'] = parsear_quadro_orcamentario(arq_qo)
            except Exception as e:
                notas.append(f'Erro parsear quadro orcamentario: {e}')
    else:
        notas.append('Sem quadro orcamentario (723)')

    # ── 4. Extrair referencias de documentos SEI ──
    resultado['documentos_sei'] = extrair_todos_docs_sei(metadata)

    # ── 5. Determinar confianca ──
    tem_req = bool(resultado['itinerario'].get('objetivo'))
    tem_integrantes = len(resultado['integrantes']) > 0
    tem_valor = bool(resultado['itinerario'].get('valor_total'))
    tem_qo = bool(resultado['quadro_orcamentario'].get('ug'))

    if tem_req and tem_integrantes and tem_valor and tem_qo:
        resultado['confidence'] = 'high'
    elif tem_req and (tem_integrantes or tem_valor):
        resultado['confidence'] = 'medium'
    else:
        resultado['confidence'] = 'low'

    resultado['notes'] = '; '.join(notas) if notas else ''

    return resultado


def main():
    parser = argparse.ArgumentParser(description='Analisar documentos de diarias e gerar _extracted.json')
    parser.add_argument('--processo', help='Pasta do processo (ex: 00002.000065_2026-82)')
    parser.add_argument('--limit', type=int, default=0, help='Limite de processos (0=todos)')
    parser.add_argument('--force', action='store_true', help='Re-analisar mesmo se ja tem _extracted.json')
    args = parser.parse_args()

    if not os.path.isdir(DEST_DIR):
        print(f"Diretorio nao encontrado: {DEST_DIR}")
        return

    # Listar processos
    if args.processo:
        pastas = [args.processo]
    else:
        pastas = sorted([
            d for d in os.listdir(DEST_DIR)
            if os.path.isdir(os.path.join(DEST_DIR, d)) and not d.startswith('_')
        ])

    total = len(pastas)
    analisados = 0
    erros = 0
    pulados = 0

    print(f"Total de processos: {total}")
    print(f"Modo: {'FORCE' if args.force else 'incremental'}\n")

    for i, pasta in enumerate(pastas, 1):
        pasta_path = os.path.join(DEST_DIR, pasta)
        protocolo = pasta.replace('_', '/')
        extracted_path = os.path.join(pasta_path, '_extracted.json')

        # Pular se ja analisado
        if not args.force and os.path.exists(extracted_path):
            pulados += 1
            continue

        if args.limit > 0 and analisados >= args.limit:
            break

        print(f"[{i}/{total}] {protocolo}...", end=' ')

        try:
            resultado = analisar_processo(pasta_path, protocolo)
            if resultado:
                with open(extracted_path, 'w', encoding='utf-8') as f:
                    json.dump(resultado, f, ensure_ascii=False, indent=2)
                conf = resultado['confidence']
                n_int = len(resultado.get('integrantes', []))
                n_docs = len(resultado.get('documentos_sei', {}))
                print(f"OK (conf={conf}, {n_int} integ., {n_docs} docs)")
                analisados += 1
            else:
                print("SEM METADATA")
                erros += 1
        except Exception as e:
            print(f"ERRO: {e}")
            erros += 1

    print(f"\n{'='*60}")
    print(f"RESULTADO")
    print(f"  Analisados: {analisados}")
    print(f"  Pulados (ja existiam): {pulados}")
    print(f"  Erros: {erros}")


if __name__ == '__main__':
    main()
