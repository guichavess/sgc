"""
Dry-run para normalizar saldos de contratos a partir de planilha Excel.

Fonte de verdade: aba "CONTRATOS VIGENTES" de ContratosSEAD.xlsx.

O script:
1. Le a planilha (cabecalho com VALOR GLOBAL + colunas QTD/VALOR por mes/ano).
2. Agrega por contrato: saldo = VALOR GLOBAL - soma(execucoes mensais).
3. (Opcional) consulta producao somente-leitura via .env.prod.
4. Gera SQL de RESET de saldo_contrato (DELETE + INSERT recalculado).
5. Gera relatorio CSV + SQL revisavel de execucoes da planilha que nao existem
   em execucoes em prod (apenas relatorio, nada e inserido automaticamente).

Uso:
    python scripts/importar_saldo_contratos.py
    python scripts/importar_saldo_contratos.py --sem-producao
    python scripts/importar_saldo_contratos.py \\
        --sql-output scripts/saldo_contratos_producao.sql \\
        --exec-csv-output relatorio_execucoes_diff.csv \\
        --exec-sql-output scripts/execucoes_faltantes.sql
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXCEL_PATH = Path(r"C:\Users\guilh\OneDrive\Documentos\ContratosSEAD.xlsx")
DEFAULT_SHEET_NAME = "CONTRATOS VIGENTES"
DEFAULT_HEADER_ROW = 1  # linha 0 = meses (serial Excel), linha 1 = cabecalho
DEFAULT_ENV_PROD = PROJECT_ROOT / ".env.prod"
CENT = Decimal("0.01")
EXCEL_EPOCH = date(1899, 12, 30)


@dataclass
class CodigoInvalido:
    codigo: str
    linha: int
    motivo: str


@dataclass
class ExecucaoPlanilha:
    codigo_contrato: str
    ano: int
    mes: int
    data: date
    valor: Decimal
    quantidade: int
    item_descricao: str


@dataclass
class ContratoSaldoCandidato:
    codigo: str
    contrato: str = ""
    contratado: str = ""
    linhas: int = 0
    valor_global_total: Decimal = Decimal("0.00")
    valor_mensal_executado_total: Decimal = Decimal("0.00")
    valores_por_mes: dict[str, Decimal] = field(default_factory=lambda: defaultdict(lambda: Decimal("0.00")))

    @property
    def saldo_restante_calculado(self) -> Decimal:
        return quantize_money(self.valor_global_total - self.valor_mensal_executado_total)


@dataclass
class ColunaMensal:
    indice: int
    label: str  # "YYYY-MM"
    ano: int
    mes: int
    data: date


@dataclass
class ResultadoPlanilha:
    contratos: dict[str, ContratoSaldoCandidato]
    codigos_invalidos: list[CodigoInvalido]
    colunas_mensais: list[ColunaMensal]
    total_linhas_dados: int
    codigos_brutos_planilha: set[str]
    execucoes_planilha: list[ExecucaoPlanilha]


@dataclass
class ReconciliacaoBanco:
    contratos_existentes: set[str]
    contratos_com_saldo: set[str]
    total_contratos: int
    total_contratos_com_saldo: int
    detalhes_com_saldo: dict[str, dict[str, str]] = field(default_factory=dict)
    saldo_contrato_total_linhas: int = 0
    saldo_contrato_item_total_linhas: int = 0
    movimentacao_saldo_total_linhas: int = 0
    execucoes_agregadas: dict[tuple[str, int, int], dict[str, object]] = field(default_factory=dict)


@dataclass
class ReconciliacaoResultado:
    producao_sem_saldo: list[str]
    planilha_nao_encontrados: list[str]
    planilha_existentes_sem_saldo: list[str]
    planilha_existentes_com_saldo: list[str]


@dataclass
class DiffExecucao:
    codigo_contrato: str
    ano: int
    mes: int
    valor_planilha: Decimal
    valor_db: Decimal
    qtd_planilha: int
    qtd_db: int
    delta: Decimal
    item_descricao_sample: str


@dataclass
class ResultadoExecucoes:
    match: list[DiffExecucao]
    divergentes: list[DiffExecucao]
    apenas_planilha: list[DiffExecucao]
    apenas_db: list[DiffExecucao]


@dataclass
class SqlSaldosGerado:
    conteudo: str
    total_inserts: int
    contratos_ignorados_negativos: list[str]
    contratos_ignorados_sem_candidato: list[str]


# ---------- conversoes/utilitarios ----------


def quantize_money(valor: Decimal) -> Decimal:
    return valor.quantize(CENT, rounding=ROUND_HALF_UP)


def serial_excel_para_data(serial: int) -> date:
    return EXCEL_EPOCH + timedelta(days=int(serial))


def normalizar_header(valor) -> str:
    texto = str(valor or "").strip().upper()
    troca = {
        "Ã": "A", "Á": "A", "À": "A", "Â": "A",
        "É": "E", "Ê": "E",
        "Í": "I",
        "Ó": "O", "Ô": "O", "Õ": "O",
        "Ú": "U",
        "Ç": "C",
        "°": "", "º": "",
    }
    for origem, destino in troca.items():
        texto = texto.replace(origem, destino)
    return re.sub(r"\s+", " ", texto)


def normalizar_codigo(valor) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    if not texto:
        return ""
    try:
        decimal = Decimal(texto)
    except InvalidOperation:
        return texto
    if decimal == decimal.to_integral_value():
        return str(int(decimal))
    return texto


def codigo_eh_valido(codigo: str) -> bool:
    if not codigo:
        return False
    return codigo.upper() not in {"S/N", "S/C", "-"}


def parse_decimal_br(valor) -> Decimal:
    if valor is None:
        return Decimal("0.00")
    if isinstance(valor, Decimal):
        return quantize_money(valor)
    texto = str(valor).strip()
    if not texto or texto == "-":
        return Decimal("0.00")

    texto = re.sub(r"[R$\s]", "", texto)
    if not texto:
        return Decimal("0.00")

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    else:
        partes = texto.split(".")
        if len(partes) > 2:
            texto = "".join(partes)

    try:
        return quantize_money(Decimal(texto))
    except InvalidOperation:
        return Decimal("0.00")


def parse_int_quantidade(valor) -> int:
    if valor is None:
        return 0
    if isinstance(valor, bool):
        return int(valor)
    try:
        return int(Decimal(str(valor).strip().replace(",", ".")))
    except (InvalidOperation, ValueError):
        return 0


def localizar_coluna(headers: list, candidatos: Iterable[str]) -> int | None:
    headers_norm = [normalizar_header(h) for h in headers]
    for candidato in candidatos:
        alvo = normalizar_header(candidato)
        if alvo in headers_norm:
            return headers_norm.index(alvo)
    return None


def _try_serial_excel(valor) -> int | None:
    """Tenta interpretar o valor como serial Excel inteiro (>= 1000)."""
    if valor is None or valor == "":
        return None
    try:
        num = Decimal(str(valor).strip())
    except (InvalidOperation, AttributeError):
        return None
    if num == num.to_integral_value() and num >= 1000:
        return int(num)
    return None


def localizar_colunas_mensais(row_meses: list, row_metricas: list) -> list[ColunaMensal]:
    colunas = []
    mes_corrente: date | None = None
    max_cols = max(len(row_meses), len(row_metricas))
    for indice in range(max_cols):
        bruto = row_meses[indice] if indice < len(row_meses) else None
        metrica = row_metricas[indice] if indice < len(row_metricas) else None

        serial = _try_serial_excel(bruto)
        if serial is not None:
            mes_corrente = serial_excel_para_data(serial).replace(day=1)
        elif bruto not in (None, ""):
            try:
                mes_corrente = date.fromisoformat(str(bruto).strip()[:10])
            except ValueError:
                pass

        if normalizar_header(metrica) == "VALOR" and mes_corrente is not None:
            label = f"{mes_corrente.year:04d}-{mes_corrente.month:02d}"
            colunas.append(
                ColunaMensal(
                    indice=indice,
                    label=label,
                    ano=mes_corrente.year,
                    mes=mes_corrente.month,
                    data=mes_corrente,
                )
            )
    return colunas


def get_cell(row: list, indice: int | None):
    if indice is None or indice >= len(row):
        return None
    return row[indice]


# ---------- analise da planilha ----------


def analisar_planilha_rows(rows: list[list], header_row: int = DEFAULT_HEADER_ROW) -> ResultadoPlanilha:
    if len(rows) < header_row + 2:
        raise ValueError("A planilha precisa ter pelo menos uma linha de meses, uma de cabecalho e uma de dados.")

    row_meses = rows[header_row - 1] if header_row > 0 else rows[header_row]
    row_headers = rows[header_row]
    colunas_mensais = localizar_colunas_mensais(row_meses, row_headers)

    idx_codigo = localizar_coluna(row_headers, ["N SIAFE", "N° SIAFE", "Nº SIAFE", "SIAFE"])
    idx_contrato = localizar_coluna(row_headers, ["CONTRATO"])
    idx_contratada = localizar_coluna(row_headers, ["CONTRATADA"])
    idx_valor_global = localizar_coluna(
        row_headers,
        ["VALOR(R$) Global do Contrato", "VALOR GLOBAL", "VALOR GLOBAL DO CONTRATO"],
    )
    idx_item = localizar_coluna(row_headers, ["ITEM"])

    if idx_codigo is None:
        raise ValueError("Coluna 'N SIAFE' nao encontrada.")
    if idx_valor_global is None:
        raise ValueError("Coluna 'VALOR GLOBAL' nao encontrada.")

    contratos: dict[str, ContratoSaldoCandidato] = {}
    codigos_invalidos: list[CodigoInvalido] = []
    codigos_brutos_planilha: set[str] = set()
    execucoes: list[ExecucaoPlanilha] = []

    primeira_linha_dados = header_row + 1
    for row_idx, row in enumerate(rows[primeira_linha_dados:], start=primeira_linha_dados + 1):
        codigo = normalizar_codigo(get_cell(row, idx_codigo))
        if codigo:
            codigos_brutos_planilha.add(codigo)
        if not any(c not in (None, "") for c in row):
            continue

        if not codigo_eh_valido(codigo):
            codigos_invalidos.append(
                CodigoInvalido(codigo=codigo or "(vazio)", linha=row_idx, motivo="codigo invalido")
            )
            continue

        contrato = contratos.setdefault(codigo, ContratoSaldoCandidato(codigo=codigo))
        contrato.linhas += 1
        contrato.contrato = contrato.contrato or str(get_cell(row, idx_contrato) or "").strip()
        contrato.contratado = contrato.contratado or str(get_cell(row, idx_contratada) or "").strip()
        contrato.valor_global_total += parse_decimal_br(get_cell(row, idx_valor_global))
        contrato.valor_global_total = quantize_money(contrato.valor_global_total)

        item_descricao = str(get_cell(row, idx_item) or "").strip() if idx_item is not None else ""

        for coluna in colunas_mensais:
            valor = parse_decimal_br(get_cell(row, coluna.indice))
            if not valor:
                continue
            contrato.valores_por_mes[coluna.label] += valor
            contrato.valores_por_mes[coluna.label] = quantize_money(contrato.valores_por_mes[coluna.label])
            contrato.valor_mensal_executado_total += valor
            contrato.valor_mensal_executado_total = quantize_money(contrato.valor_mensal_executado_total)

            qtd_celula = get_cell(row, coluna.indice - 1) if coluna.indice > 0 else None
            quantidade = parse_int_quantidade(qtd_celula) or 1
            execucoes.append(
                ExecucaoPlanilha(
                    codigo_contrato=codigo,
                    ano=coluna.ano,
                    mes=coluna.mes,
                    data=coluna.data,
                    valor=valor,
                    quantidade=quantidade,
                    item_descricao=item_descricao,
                )
            )

    return ResultadoPlanilha(
        contratos=contratos,
        codigos_invalidos=codigos_invalidos,
        colunas_mensais=colunas_mensais,
        total_linhas_dados=max(0, len(rows) - primeira_linha_dados),
        codigos_brutos_planilha=codigos_brutos_planilha,
        execucoes_planilha=execucoes,
    )


# ---------- reconciliacao ----------


def reconciliar_contratos(
    codigos_planilha: Iterable[str],
    banco: ReconciliacaoBanco,
) -> ReconciliacaoResultado:
    codigos = {normalizar_codigo(c) for c in codigos_planilha if normalizar_codigo(c)}
    existentes = banco.contratos_existentes
    com_saldo = banco.contratos_com_saldo

    return ReconciliacaoResultado(
        producao_sem_saldo=sorted(existentes - com_saldo),
        planilha_nao_encontrados=sorted(codigos - existentes),
        planilha_existentes_sem_saldo=sorted((codigos & existentes) - com_saldo),
        planilha_existentes_com_saldo=sorted(codigos & com_saldo),
    )


def reconciliar_execucoes(
    execucoes_planilha: Iterable[ExecucaoPlanilha],
    execucoes_db: dict[tuple[str, int, int], dict[str, object]],
) -> ResultadoExecucoes:
    """Compara execucoes da planilha com agregados por (contrato, ano, mes) do DB.

    `execucoes_db` formato: {(codigo, ano, mes): {"valor": Decimal, "quantidade": int}}
    """
    agregados_planilha: dict[tuple[str, int, int], dict[str, object]] = defaultdict(
        lambda: {"valor": Decimal("0.00"), "quantidade": 0, "itens": []}
    )
    for execucao in execucoes_planilha:
        chave = (execucao.codigo_contrato, execucao.ano, execucao.mes)
        agregados_planilha[chave]["valor"] = quantize_money(
            agregados_planilha[chave]["valor"] + execucao.valor
        )
        agregados_planilha[chave]["quantidade"] += execucao.quantidade
        agregados_planilha[chave]["itens"].append(execucao.item_descricao)

    match: list[DiffExecucao] = []
    divergentes: list[DiffExecucao] = []
    apenas_planilha: list[DiffExecucao] = []
    apenas_db: list[DiffExecucao] = []

    chaves_planilha = set(agregados_planilha.keys())
    chaves_db = set(execucoes_db.keys())

    for chave in sorted(chaves_planilha & chaves_db):
        codigo, ano, mes = chave
        valor_planilha = quantize_money(agregados_planilha[chave]["valor"])
        valor_db = quantize_money(Decimal(str(execucoes_db[chave].get("valor") or 0)))
        qtd_planilha = int(agregados_planilha[chave]["quantidade"])
        qtd_db = int(execucoes_db[chave].get("quantidade") or 0)
        itens = agregados_planilha[chave]["itens"]
        descricao = " | ".join(sorted(set(d for d in itens if d)))[:200]

        diff = DiffExecucao(
            codigo_contrato=codigo,
            ano=ano,
            mes=mes,
            valor_planilha=valor_planilha,
            valor_db=valor_db,
            qtd_planilha=qtd_planilha,
            qtd_db=qtd_db,
            delta=quantize_money(valor_planilha - valor_db),
            item_descricao_sample=descricao,
        )
        if valor_planilha == valor_db:
            match.append(diff)
        else:
            divergentes.append(diff)

    for chave in sorted(chaves_planilha - chaves_db):
        codigo, ano, mes = chave
        valor_planilha = quantize_money(agregados_planilha[chave]["valor"])
        qtd_planilha = int(agregados_planilha[chave]["quantidade"])
        itens = agregados_planilha[chave]["itens"]
        descricao = " | ".join(sorted(set(d for d in itens if d)))[:200]
        apenas_planilha.append(
            DiffExecucao(
                codigo_contrato=codigo,
                ano=ano,
                mes=mes,
                valor_planilha=valor_planilha,
                valor_db=Decimal("0.00"),
                qtd_planilha=qtd_planilha,
                qtd_db=0,
                delta=valor_planilha,
                item_descricao_sample=descricao,
            )
        )

    for chave in sorted(chaves_db - chaves_planilha):
        codigo, ano, mes = chave
        valor_db = quantize_money(Decimal(str(execucoes_db[chave].get("valor") or 0)))
        qtd_db = int(execucoes_db[chave].get("quantidade") or 0)
        apenas_db.append(
            DiffExecucao(
                codigo_contrato=codigo,
                ano=ano,
                mes=mes,
                valor_planilha=Decimal("0.00"),
                valor_db=valor_db,
                qtd_planilha=0,
                qtd_db=qtd_db,
                delta=quantize_money(-valor_db),
                item_descricao_sample="",
            )
        )

    return ResultadoExecucoes(
        match=match,
        divergentes=divergentes,
        apenas_planilha=apenas_planilha,
        apenas_db=apenas_db,
    )


# ---------- carga de planilha (zip/xlsx) ----------


def carregar_planilha_xlsx(path: Path, sheet_name: str) -> list[list]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

    with zipfile.ZipFile(path) as zf:
        sheet_xml = resolver_sheet_xml(zf, sheet_name)
        shared = carregar_shared_strings(zf)
        return carregar_rows_xml(zf, sheet_xml, shared)


def resolver_sheet_xml(zf: zipfile.ZipFile, sheet_name: str) -> str:
    ns = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rels = {rel.get("Id"): rel.get("Target") for rel in rels_root.findall("pkgrel:Relationship", ns)}

    for sheet in workbook.findall(".//main:sheet", ns):
        if sheet.get("name") == sheet_name:
            rel_id = sheet.get(f"{{{ns['rel']}}}id")
            target = rels.get(rel_id)
            if not target:
                break
            target = target.lstrip("/")
            return target if target.startswith("xl/") else f"xl/{target}"

    disponiveis = [sheet.get("name") for sheet in workbook.findall(".//main:sheet", ns)]
    raise ValueError(f"Aba '{sheet_name}' nao encontrada. Abas disponiveis: {', '.join(disponiveis)}")


def carregar_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []

    strings = []
    for item in root.findall("main:si", ns):
        strings.append("".join((t.text or "") for t in item.findall(".//main:t", ns)))
    return strings


def carregar_rows_xml(zf: zipfile.ZipFile, sheet_xml: str, shared: list[str]) -> list[list]:
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    cell_ref_re = re.compile(r"^([A-Z]+)(\d+)$")
    rows_map: dict[int, dict[int, object]] = {}
    max_row = 0
    max_col = 0

    for _, elem in ET.iterparse(zf.open(sheet_xml), events=("end",)):
        if elem.tag.endswith("}row"):
            row_idx = int(elem.get("r"))
            valores: dict[int, object] = {}
            for cell in elem.findall("main:c", ns):
                match = cell_ref_re.match(cell.get("r") or "")
                if not match:
                    continue
                col_idx = coluna_para_indice(match.group(1))
                valor = valor_celula(cell, shared)
                if valor not in (None, ""):
                    valores[col_idx] = valor
                    max_col = max(max_col, col_idx)
            if valores:
                rows_map[row_idx] = valores
                max_row = max(max_row, row_idx)
            elem.clear()

    rows = []
    for row_idx in range(1, max_row + 1):
        valores = rows_map.get(row_idx, {})
        rows.append([valores.get(col_idx) for col_idx in range(1, max_col + 1)])
    return rows


def coluna_para_indice(coluna: str) -> int:
    total = 0
    for char in coluna:
        total = total * 26 + ord(char) - 64
    return total


def valor_celula(cell, shared: list[str]):
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    tipo = cell.get("t")
    if tipo == "inlineStr":
        return "".join((t.text or "") for t in cell.findall(".//main:t", ns))

    value = cell.find("main:v", ns)
    if value is None or value.text is None:
        return None

    raw = value.text
    if tipo == "s":
        try:
            return shared[int(raw)]
        except (IndexError, ValueError):
            return raw
    if tipo == "b":
        return raw == "1"
    return raw


# ---------- conexao .env.prod ----------


def carregar_env(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de ambiente nao encontrado: {path}")

    env = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def consultar_banco_somente_leitura(env_path: Path, codigos_planilha: set[str]) -> ReconciliacaoBanco:
    import pymysql

    env = {**carregar_env(env_path), **os.environ}
    conn = pymysql.connect(
        host=env.get("DB_HOST", "localhost"),
        port=int(env.get("DB_PORT", "3306") or 3306),
        user=env.get("DB_USER", "root"),
        password=env.get("DB_PASS", "root"),
        database=env.get("DB_NAME", "sgc"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=15,
        read_timeout=60,
        write_timeout=30,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT codigo FROM contratos")
            contratos = {str(row["codigo"]) for row in cur.fetchall()}

            cur.execute("SELECT DISTINCT codigo_contrato FROM saldo_contrato")
            com_saldo = {str(row["codigo_contrato"]) for row in cur.fetchall()}

            cur.execute("SELECT COUNT(*) AS n FROM saldo_contrato")
            saldo_total = int(cur.fetchone()["n"])
            cur.execute("SELECT COUNT(*) AS n FROM saldo_contrato_item")
            saldo_item_total = int(cur.fetchone()["n"])
            cur.execute("SELECT COUNT(*) AS n FROM movimentacao_saldo")
            mov_total = int(cur.fetchone()["n"])

            detalhes = {}
            cur.execute(
                """
                SELECT c.codigo, c.numeroOriginal, c.nomeContratado, c.situacao,
                       c.codigoUG, s.saldo_global
                FROM contratos c
                JOIN saldo_contrato s ON s.codigo_contrato = c.codigo
                ORDER BY c.codigo
                """
            )
            for row in cur.fetchall():
                detalhes[str(row["codigo"])] = {
                    "numeroOriginal": str(row.get("numeroOriginal") or ""),
                    "nomeContratado": str(row.get("nomeContratado") or ""),
                    "situacao": str(row.get("situacao") or ""),
                    "codigoUG": str(row.get("codigoUG") or ""),
                    "saldo_global": str(row.get("saldo_global") or ""),
                }

            execucoes_agregadas: dict[tuple[str, int, int], dict[str, object]] = {}
            codigos_validos = [c for c in codigos_planilha if c and codigo_eh_valido(c)]
            if codigos_validos:
                placeholders = ",".join(["%s"] * len(codigos_validos))
                cur.execute(
                    f"""
                    SELECT
                        codigo_contrato,
                        YEAR(`data`)  AS ano,
                        MONTH(`data`) AS mes,
                        SUM(valor)       AS valor,
                        SUM(quantidade)  AS quantidade
                    FROM execucoes
                    WHERE codigo_contrato IN ({placeholders})
                      AND `data` IS NOT NULL
                    GROUP BY codigo_contrato, YEAR(`data`), MONTH(`data`)
                    """,
                    codigos_validos,
                )
                for row in cur.fetchall():
                    chave = (str(row["codigo_contrato"]), int(row["ano"]), int(row["mes"]))
                    execucoes_agregadas[chave] = {
                        "valor": Decimal(str(row["valor"] or 0)),
                        "quantidade": int(row["quantidade"] or 0),
                    }

            return ReconciliacaoBanco(
                contratos_existentes=contratos,
                contratos_com_saldo=com_saldo,
                total_contratos=len(contratos),
                total_contratos_com_saldo=len(com_saldo),
                detalhes_com_saldo=detalhes,
                saldo_contrato_total_linhas=saldo_total,
                saldo_contrato_item_total_linhas=saldo_item_total,
                movimentacao_saldo_total_linhas=mov_total,
                execucoes_agregadas=execucoes_agregadas,
            )
    finally:
        conn.close()


# ---------- formatacao ----------


def format_money(valor: Decimal) -> str:
    texto = f"{valor:,.2f}"
    return "R$ " + texto.replace(",", "X").replace(".", ",").replace("X", ".")


def sql_quote(valor: str) -> str:
    return "'" + str(valor).replace("'", "''") + "'"


# ---------- relatorios ----------


def gerar_linhas_relatorio(resultado: ResultadoPlanilha) -> list[dict[str, object]]:
    linhas = []
    for contrato in sorted(resultado.contratos.values(), key=lambda c: c.codigo):
        linhas.append(
            {
                "codigo": contrato.codigo,
                "contrato": contrato.contrato,
                "contratado": contrato.contratado,
                "linhas": contrato.linhas,
                "valor_global_total": str(contrato.valor_global_total),
                "valor_mensal_executado_total": str(contrato.valor_mensal_executado_total),
                "saldo_restante_calculado": str(contrato.saldo_restante_calculado),
            }
        )
    return linhas


def gerar_sql_reset_saldo_contrato(
    resultado: ResultadoPlanilha,
    reconciliacao: ReconciliacaoResultado,
    permitir_negativos: bool = False,
    banco: ReconciliacaoBanco | None = None,
) -> SqlSaldosGerado:
    """Gera SQL com DELETE de saldo_contrato + INSERT recalculado a partir da planilha.

    Inclui apenas contratos existentes em producao (FK valida).
    Saldos negativos sao ignorados por padrao.
    """
    candidatos = sorted(
        set(reconciliacao.planilha_existentes_sem_saldo) | set(reconciliacao.planilha_existentes_com_saldo)
    )

    inserts = []
    ignorados_negativos = []
    ignorados_sem_candidato = []

    for codigo in candidatos:
        candidato = resultado.contratos.get(codigo)
        if not candidato:
            ignorados_sem_candidato.append(codigo)
            continue

        saldo = candidato.saldo_restante_calculado
        if saldo < 0 and not permitir_negativos:
            ignorados_negativos.append(codigo)
            continue

        inserts.append(
            f"  ({sql_quote(codigo)}, {saldo:.2f}, NULL, NULL, NOW(), NOW())"
        )

    cabecalho = [
        "-- SQL gerado por scripts/importar_saldo_contratos.py",
        "-- Origem: ContratosSEAD.xlsx, aba 'CONTRATOS VIGENTES'",
        "-- Estrategia: RESET (DELETE + INSERT) de saldo_contrato.",
        "-- ATENCAO: revisar os valores antes de executar em producao.",
        "",
        f"-- Contratos da planilha nao encontrados em producao: {len(reconciliacao.planilha_nao_encontrados)}",
    ]
    if reconciliacao.planilha_nao_encontrados:
        cabecalho.append("-- " + ", ".join(reconciliacao.planilha_nao_encontrados))

    cabecalho.append(
        f"-- Contratos ignorados por saldo negativo: {len(ignorados_negativos)}"
    )
    if ignorados_negativos:
        cabecalho.append("-- " + ", ".join(ignorados_negativos))

    if banco:
        cabecalho.extend([
            f"-- Estado atual em prod: saldo_contrato={banco.saldo_contrato_total_linhas}, "
            f"saldo_contrato_item={banco.saldo_contrato_item_total_linhas}, "
            f"movimentacao_saldo={banco.movimentacao_saldo_total_linhas}",
        ])

    cabecalho.extend([
        f"-- Inserts gerados: {len(inserts)}",
        "",
    ])

    corpo = ["START TRANSACTION;", ""]
    if banco and (banco.saldo_contrato_item_total_linhas or banco.movimentacao_saldo_total_linhas):
        corpo.extend([
            "-- Tabelas dependentes possuem linhas. Descomentar APENAS se quiser limpar tudo:",
            "-- DELETE FROM saldo_contrato_item;",
            "-- DELETE FROM movimentacao_saldo;",
            "",
        ])
    corpo.append("DELETE FROM saldo_contrato;")
    corpo.append("")

    if inserts:
        corpo.extend([
            "INSERT INTO saldo_contrato (",
            "    codigo_contrato, saldo_global, data_inicio, usuario_id, created_at, updated_at",
            ") VALUES",
            ",\n".join(inserts) + ";",
            "",
        ])
    else:
        corpo.append("-- Nenhum INSERT gerado.")

    corpo.extend([
        "-- Conferencia pos-execucao:",
        "-- SELECT COUNT(*) FROM saldo_contrato;",
        "-- SELECT codigo_contrato, saldo_global FROM saldo_contrato ORDER BY codigo_contrato;",
        "",
        "COMMIT;",
    ])

    conteudo = "\n".join(cabecalho + corpo) + "\n"

    return SqlSaldosGerado(
        conteudo=conteudo,
        total_inserts=len(inserts),
        contratos_ignorados_negativos=ignorados_negativos,
        contratos_ignorados_sem_candidato=ignorados_sem_candidato,
    )


def salvar_csv_execucoes(path: Path, resultado: ResultadoExecucoes) -> None:
    fieldnames = [
        "status", "codigo_contrato", "ano", "mes",
        "valor_planilha", "valor_db", "delta",
        "qtd_planilha", "qtd_db", "item_descricao_sample",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for status, items in [
            ("apenas_planilha", resultado.apenas_planilha),
            ("divergente", resultado.divergentes),
            ("apenas_db", resultado.apenas_db),
            ("match", resultado.match),
        ]:
            for diff in items:
                writer.writerow(
                    {
                        "status": status,
                        "codigo_contrato": diff.codigo_contrato,
                        "ano": diff.ano,
                        "mes": diff.mes,
                        "valor_planilha": f"{diff.valor_planilha:.2f}",
                        "valor_db": f"{diff.valor_db:.2f}",
                        "delta": f"{diff.delta:.2f}",
                        "qtd_planilha": diff.qtd_planilha,
                        "qtd_db": diff.qtd_db,
                        "item_descricao_sample": diff.item_descricao_sample,
                    }
                )


def salvar_sql_execucoes_faltantes(
    path: Path,
    execucoes_planilha: list[ExecucaoPlanilha],
    resultado: ResultadoExecucoes,
    contratos_existentes: set[str],
    ativo: bool = False,
) -> int:
    """Gera SQL revisavel para INSERTs em execucoes (faltantes na planilha).

    Por padrao todas as linhas saem comentadas (modo relatorio).
    Use ativo=True para liberar (ainda assim revisar antes de aplicar).
    """
    chaves_faltantes = {(d.codigo_contrato, d.ano, d.mes) for d in resultado.apenas_planilha}
    linhas_insert = []
    for execucao in execucoes_planilha:
        chave = (execucao.codigo_contrato, execucao.ano, execucao.mes)
        if chave not in chaves_faltantes:
            continue
        if execucao.codigo_contrato not in contratos_existentes:
            continue
        linhas_insert.append(
            "  ({contrato}, '{data}', {mes}, {ano}, {valor:.2f}, {qtd}, 'S', NULL, NULL, NULL)".format(
                contrato=sql_quote(execucao.codigo_contrato),
                data=execucao.data.isoformat(),
                mes=execucao.mes,
                ano=execucao.ano,
                valor=execucao.valor,
                qtd=execucao.quantidade or 1,
            )
        )

    cabecalho = [
        "-- Execucoes presentes na planilha mas ausentes do DB.",
        "-- REVISAR antes de aplicar. Inseridas como tipo='S' com FKs NULL.",
        f"-- Total de linhas a inserir: {len(linhas_insert)}",
        "",
    ]
    if not linhas_insert:
        path.write_text("\n".join(cabecalho + ["-- Nenhuma execucao faltante detectada."]) + "\n", encoding="utf-8")
        return 0

    bloco = [
        "INSERT INTO execucoes",
        "    (codigo_contrato, `data`, mes, ano, valor, quantidade, tipo, catserv_servico_id, catmat_item_id, item_vinculado_id)",
        "VALUES",
        ",\n".join(linhas_insert) + ";",
    ]

    if not ativo:
        bloco = ["-- " + linha if linha else "--" for linha in bloco]

    conteudo = "\n".join(cabecalho + bloco) + "\n"
    path.write_text(conteudo, encoding="utf-8")
    return len(linhas_insert)


# ---------- impressao ----------


def imprimir_relatorio(
    resultado: ResultadoPlanilha,
    reconciliacao: ReconciliacaoResultado | None,
    banco: ReconciliacaoBanco | None,
    diff_execucoes: ResultadoExecucoes | None,
) -> None:
    print("=" * 72)
    print("DRY-RUN - SALDOS DE CONTRATOS")
    print("=" * 72)
    print("Nenhuma alteracao sera feita no banco.")
    print()
    print(f"Linhas de dados na planilha: {resultado.total_linhas_dados}")
    print(f"Contratos validos agregados: {len(resultado.contratos)}")
    print(f"Codigos brutos distintos na planilha: {len(resultado.codigos_brutos_planilha)}")
    print(f"Colunas mensais VALOR identificadas: {len(resultado.colunas_mensais)}")
    if resultado.colunas_mensais:
        primeiro = resultado.colunas_mensais[0].label
        ultimo = resultado.colunas_mensais[-1].label
        print(f"Faixa: {primeiro} ... {ultimo}")
    print(f"Execucoes por item extraidas da planilha: {len(resultado.execucoes_planilha)}")

    total_global = sum((c.valor_global_total for c in resultado.contratos.values()), Decimal("0.00"))
    total_executado = sum((c.valor_mensal_executado_total for c in resultado.contratos.values()), Decimal("0.00"))
    total_saldo = sum((c.saldo_restante_calculado for c in resultado.contratos.values()), Decimal("0.00"))
    print(f"Valor global total: {format_money(total_global)}")
    print(f"Valor mensal executado total: {format_money(total_executado)}")
    print(f"Saldo restante calculado total: {format_money(total_saldo)}")

    negativos = [c for c in resultado.contratos.values() if c.saldo_restante_calculado < 0]
    if negativos:
        print(f"ATENCAO: {len(negativos)} contrato(s) com saldo restante negativo.")

    if resultado.codigos_invalidos:
        codigos = sorted({c.codigo for c in resultado.codigos_invalidos})
        print(f"Codigos invalidos na planilha: {len(codigos)} -> {', '.join(codigos)}")

    print()
    print("Top 10 contratos por saldo restante calculado:")
    for contrato in sorted(resultado.contratos.values(), key=lambda c: c.saldo_restante_calculado, reverse=True)[:10]:
        print(
            f"  {contrato.codigo} | linhas={contrato.linhas} | "
            f"global={format_money(contrato.valor_global_total)} | "
            f"executado={format_money(contrato.valor_mensal_executado_total)} | "
            f"saldo={format_money(contrato.saldo_restante_calculado)}"
        )

    if banco and reconciliacao:
        print()
        print("-" * 72)
        print("RECONCILIACAO PRODUCAO (.env.prod, somente SELECT)")
        print("-" * 72)
        print(f"Contratos em producao: {banco.total_contratos}")
        print(f"Contratos com saldo em producao: {banco.total_contratos_com_saldo}")
        print(
            f"Estado: saldo_contrato={banco.saldo_contrato_total_linhas}, "
            f"saldo_contrato_item={banco.saldo_contrato_item_total_linhas}, "
            f"movimentacao_saldo={banco.movimentacao_saldo_total_linhas}"
        )
        print(
            "Contratos da planilha existentes em producao sem saldo: "
            f"{len(reconciliacao.planilha_existentes_sem_saldo)}"
        )
        print(
            "Contratos da planilha ja com saldo em producao: "
            f"{len(reconciliacao.planilha_existentes_com_saldo)}"
        )
        print(
            "Contratos da planilha nao encontrados em producao: "
            f"{len(reconciliacao.planilha_nao_encontrados)}"
        )
        if reconciliacao.planilha_nao_encontrados:
            print("  " + ", ".join(reconciliacao.planilha_nao_encontrados))

    if diff_execucoes:
        print()
        print("-" * 72)
        print("DIFF DE EXECUCOES (planilha vs execucoes em prod)")
        print("-" * 72)
        print(f"Match: {len(diff_execucoes.match)}")
        print(f"Divergentes (mesma chave, valor diferente): {len(diff_execucoes.divergentes)}")
        print(f"Apenas na planilha (candidatos a INSERT): {len(diff_execucoes.apenas_planilha)}")
        print(f"Apenas no banco: {len(diff_execucoes.apenas_db)}")


def salvar_json(path: Path, resultado: ResultadoPlanilha, reconciliacao: ReconciliacaoResultado | None) -> None:
    payload = {
        "contratos": gerar_linhas_relatorio(resultado),
        "codigos_invalidos": [item.__dict__ for item in resultado.codigos_invalidos],
        "reconciliacao": reconciliacao.__dict__ if reconciliacao else None,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def salvar_csv(path: Path, resultado: ResultadoPlanilha) -> None:
    linhas = gerar_linhas_relatorio(resultado)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(linhas[0].keys()) if linhas else [])
        if linhas:
            writer.writeheader()
            writer.writerows(linhas)


# ---------- CLI ----------


def parse_args():
    parser = argparse.ArgumentParser(description="Dry-run de saldos de contratos (ContratosSEAD.xlsx).")
    parser.add_argument("arquivo", nargs="?", default=str(DEFAULT_EXCEL_PATH), help="Caminho da planilha .xlsx")
    parser.add_argument("--sheet", default=DEFAULT_SHEET_NAME, help="Nome da aba da planilha")
    parser.add_argument("--header-row", type=int, default=DEFAULT_HEADER_ROW, help="Indice 0-based da linha de cabecalho")
    parser.add_argument("--env-prod", default=str(DEFAULT_ENV_PROD), help="Caminho do .env.prod")
    parser.add_argument("--sem-producao", action="store_true", help="Nao consultar producao")
    parser.add_argument("--sql-output", help="Arquivo SQL para RESET (DELETE+INSERT) de saldo_contrato")
    parser.add_argument(
        "--permitir-negativos",
        action="store_true",
        help="Inclui saldos negativos no SQL gerado (nao recomendado)",
    )
    parser.add_argument("--exec-csv-output", help="CSV com diff de execucoes (planilha vs DB)")
    parser.add_argument("--exec-sql-output", help="SQL revisavel com INSERTs de execucoes faltantes (comentados)")
    parser.add_argument("--exec-sql-ativo", action="store_true", help="Descomenta os INSERTs do --exec-sql-output")
    parser.add_argument("--json-output", help="Arquivo JSON opcional para conferencia")
    parser.add_argument("--csv-output", help="Arquivo CSV opcional para conferencia")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = carregar_planilha_xlsx(Path(args.arquivo), args.sheet)
    resultado = analisar_planilha_rows(rows, header_row=args.header_row)

    banco = None
    reconciliacao = None
    diff_execucoes = None
    if not args.sem_producao:
        banco = consultar_banco_somente_leitura(Path(args.env_prod), resultado.codigos_brutos_planilha)
        reconciliacao = reconciliar_contratos(resultado.codigos_brutos_planilha, banco)
        diff_execucoes = reconciliar_execucoes(resultado.execucoes_planilha, banco.execucoes_agregadas)

    imprimir_relatorio(resultado, reconciliacao, banco, diff_execucoes)

    if args.sql_output:
        if not reconciliacao or not banco:
            raise SystemExit("--sql-output exige reconciliacao com producao. Remova --sem-producao.")
        sql = gerar_sql_reset_saldo_contrato(
            resultado, reconciliacao, permitir_negativos=args.permitir_negativos, banco=banco
        )
        sql_path = Path(args.sql_output)
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text(sql.conteudo, encoding="utf-8")
        print(f"\nSQL salvo em: {sql_path}")
        print(f"Inserts gerados: {sql.total_inserts}")
        if sql.contratos_ignorados_negativos:
            print(f"Contratos ignorados por saldo negativo: {len(sql.contratos_ignorados_negativos)}")

    if args.exec_csv_output:
        if not diff_execucoes:
            raise SystemExit("--exec-csv-output exige reconciliacao com producao. Remova --sem-producao.")
        csv_exec_path = Path(args.exec_csv_output)
        csv_exec_path.parent.mkdir(parents=True, exist_ok=True)
        salvar_csv_execucoes(csv_exec_path, diff_execucoes)
        print(f"CSV diff de execucoes salvo em: {csv_exec_path}")

    if args.exec_sql_output:
        if not diff_execucoes or not banco:
            raise SystemExit("--exec-sql-output exige reconciliacao com producao. Remova --sem-producao.")
        sql_exec_path = Path(args.exec_sql_output)
        sql_exec_path.parent.mkdir(parents=True, exist_ok=True)
        total = salvar_sql_execucoes_faltantes(
            sql_exec_path,
            resultado.execucoes_planilha,
            diff_execucoes,
            banco.contratos_existentes,
            ativo=args.exec_sql_ativo,
        )
        marca = "ATIVO" if args.exec_sql_ativo else "COMENTADO"
        print(f"SQL execucoes faltantes ({marca}) salvo em: {sql_exec_path} (linhas={total})")

    if args.json_output:
        salvar_json(Path(args.json_output), resultado, reconciliacao)
        print(f"JSON salvo em: {args.json_output}")
    if args.csv_output:
        salvar_csv(Path(args.csv_output), resultado)
        print(f"CSV salvo em: {args.csv_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
