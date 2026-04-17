"""
Utilitário para extrair a superintendência canônica a partir da Sigla
de uma unidade SEI (ex: 'SEAD-PI/GAB/SGACG/DFIN/GEO').

A lógica lida com três convenções de nomeação que coexistem no SEI:
  1. Nova:  SEAD-PI/GAB/{SUPER}/{DIRETORIA}/...
  2. Antiga sem GAB:  SEAD-PI/{SUPER}/{DIRETORIA}/...
  3. Diretorias órfãs (old flat): SEAD-PI/GAB/{DIRETORIA}/... ou SEAD-PI/{DIRETORIA}/...

Retorna uma sigla canônica (ex: 'SGACG') ou 'GAB' para unidades subordinadas
diretamente ao Gabinete sem superintendência.
"""

# Siglas oficialmente reconhecidas como Superintendência no SEI da SEAD-PI.
SUPERINTENDENCIAS_SEAD = {
    'SGACG',   # Superintendência de Gestão Administrativa e Controle dos Gastos
    'SGP',     # Superintendência de Gestão de Pessoas
    'SLC',     # Superintendência de Licitação e Contratos
    'SUPARC',  # Superintendência de Parcerias Público Privadas e Concessões
    'SPI',     # Superintendência de Patrimônio Imobiliário
    'NTGD',    # Núcleo Estratégico de Tecnologia e Governo Digital
    'NUFAF',   # Núcleo de Formação Continuada (Escola de Governo)
    'DEGEPI',  # Diretoria Pedagógica da Escola de Governo
}

# Diretorias "órfãs" — aparecem na Sigla sem o nível de Superintendência
# explícito (old flat naming). Mapeiam para a superintendência real.
MAPEAMENTO_DIRETORIA_SUPER = {
    # SGACG (Superintendência de Gestão Administrativa)
    'DFIN': 'SGACG', 'DLOG': 'SGACG', 'DGCA': 'SGACG', 'DINFRA': 'SGACG',
    'DRAEGP': 'SGACG', 'DUAF': 'SGACG', 'DUMA': 'SGACG', 'DTEC': 'SGACG',
    'DGA': 'SGACG', 'DGI': 'SGACG', 'DAC': 'SGACG',
    # SLC (Superintendência de Licitação e Contratos)
    'DCON': 'SLC', 'DIP': 'SLC', 'DL': 'SLC', 'DLS': 'SLC', 'DEP': 'SLC',
    'CNPP': 'SLC',
    # SGP (Superintendência de Gestão de Pessoas)
    'DUGP': 'SGP', 'DDP': 'SGP', 'DPP': 'SGP', 'DFPG': 'SGP',
    'DPPE': 'SGP', 'CIASPI': 'SGP',
    # SPI (Superintendência de Patrimônio Imobiliário)
    'DREG': 'SPI',
    # DGPAT é ambíguo (aparece sob SGA e sob SPI). Resolvido pela Sigla
    # completa no caller — não incluído aqui para evitar mapeamento errado.
}

# Prefixo da secretaria de interesse. Siglas que começam com outro prefixo
# (SEADPREV-PI, SAF-PI etc.) são ignoradas.
SECRETARIA_PREFIXO = 'SEAD-PI'

# Sigla canônica da Superintendência de Gestão Administrativa (SGA).
# É a superintendência responsável por autorizar solicitações de diárias
# de TODAS as superintendências — antes do Secretário.
SUPER_SGA_SIGLA = 'SGACG'


def extrair_superintendencia(sigla_unidade):
    """Extrai a sigla canônica da superintendência a partir da Sigla SEI.

    Args:
        sigla_unidade: string no formato 'SEAD-PI/GAB/SGACG/...' ou similar.

    Returns:
        Tupla (super_key, tipo) onde:
          - super_key: sigla canônica ('SGACG', 'SLC', ...) ou 'GAB' para
                       unidades do Gabinete-direto, ou None para externas.
          - tipo: 'superintendencia' | 'gabinete' | 'externa' | 'desconhecida'

    Exemplos:
        'SEAD-PI/GAB/SGACG/DFIN/GEO' → ('SGACG', 'superintendencia')
        'SEAD-PI/SGACG/DFIN'         → ('SGACG', 'superintendencia')
        'SEAD-PI/GAB/NCI'            → ('GAB', 'gabinete')
        'SEAD-PI/GAB/DFIN/GEO'       → ('SGACG', 'superintendencia')  # via mapeamento
        'SEADPREV-PI/GAB/...'        → (None, 'externa')
    """
    if not sigla_unidade:
        return (None, 'desconhecida')

    partes = [p.strip() for p in sigla_unidade.split('/') if p.strip()]
    if not partes:
        return (None, 'desconhecida')

    # Secretarias externas são ignoradas.
    if partes[0] != SECRETARIA_PREFIXO:
        return (None, 'externa')

    # Remove 'GAB' se presente (não é nível estrutural real).
    nivel_topo = partes[1] if len(partes) > 1 else None
    if nivel_topo == 'GAB':
        if len(partes) < 3:
            # Usuário está exatamente no Gabinete raiz
            return ('GAB', 'gabinete')
        candidato = partes[2]
    else:
        candidato = nivel_topo

    if not candidato:
        return (None, 'desconhecida')

    # 1. Superintendência conhecida
    if candidato in SUPERINTENDENCIAS_SEAD:
        return (candidato, 'superintendencia')

    # 2. Diretoria órfã (old flat) → resolve via mapeamento
    if candidato in MAPEAMENTO_DIRETORIA_SUPER:
        return (MAPEAMENTO_DIRETORIA_SUPER[candidato], 'superintendencia')

    # 3. Unidade direta do Gabinete (NCI, CJUR, OUVIDORIA etc.)
    return ('GAB', 'gabinete')
