#!/bin/bash
# =============================================================================
# SGC - Script de Atualização Noturna
#
# Executa os 5 scripts de atualização em sequência:
#   1. contratos    - Atualiza dados contratuais (SGA)
#   2. empenho      - Busca empenhos do SIAFE
#   3. liquidacao   - Busca liquidações do SIAFE
#   4. pd           - Busca programações de desembolso do SIAFE
#   5. ob           - Busca ordens bancárias do SIAFE
#
# Uso:
#   Agendado via crontab (ver deploy/crontab.conf)
#   Ou manualmente: bash /home/sead/sgc_novo/scripts/cron_noturno.sh
#
# Logs:
#   Cada execução gera log em /home/sead/sgc_novo/logs/cron_noturno_YYYYMMDD.log
# =============================================================================

set -euo pipefail

# --- Configurações ---
PROJECT_DIR="/home/sead/sgc_novo"
VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python3"
SCRIPTS_DIR="${PROJECT_DIR}/scripts"
LOGS_DIR="${PROJECT_DIR}/logs"

# Arquivo de log com data
DATA_ATUAL=$(date +%Y%m%d)
LOG_FILE="${LOGS_DIR}/cron_noturno_${DATA_ATUAL}.log"

# --- Funções auxiliares ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

executar_script() {
    local nome="$1"
    local arquivo="$2"

    log "--- INICIANDO: ${nome} ---"
    local inicio=$(date +%s)

    if ${VENV_PYTHON} "${SCRIPTS_DIR}/${arquivo}" >> "$LOG_FILE" 2>&1; then
        local fim=$(date +%s)
        local duracao=$((fim - inicio))
        log "✅ ${nome} concluído com sucesso (${duracao}s)"
        return 0
    else
        local exit_code=$?
        local fim=$(date +%s)
        local duracao=$((fim - inicio))
        log "❌ ${nome} FALHOU (exit code: ${exit_code}, ${duracao}s)"
        return $exit_code
    fi
}

# --- Validações ---
if [ ! -f "$VENV_PYTHON" ]; then
    echo "ERRO: Python do venv não encontrado em ${VENV_PYTHON}" >&2
    exit 1
fi

if [ ! -d "$SCRIPTS_DIR" ]; then
    echo "ERRO: Diretório de scripts não encontrado: ${SCRIPTS_DIR}" >&2
    exit 1
fi

mkdir -p "$LOGS_DIR"

# --- Início da execução ---
INICIO_TOTAL=$(date +%s)
log "=========================================="
log "  SGC - ATUALIZAÇÃO NOTURNA"
log "=========================================="

TOTAL_SCRIPTS=5
SUCESSO=0
FALHAS=0

# 1. Atualizar Contratos (SGA - não depende do SIAFE)
if executar_script "Atualizar Contratos" "atualizar_contratos.py"; then
    SUCESSO=$((SUCESSO + 1))
else
    FALHAS=$((FALHAS + 1))
fi

# 2. Atualizar Empenhos (SIAFE)
if executar_script "Atualizar Empenhos" "atualizar_empenho.py"; then
    SUCESSO=$((SUCESSO + 1))
else
    FALHAS=$((FALHAS + 1))
fi

# 3. Atualizar Liquidações (SIAFE)
if executar_script "Atualizar Liquidações" "atualizar_liquidacao.py"; then
    SUCESSO=$((SUCESSO + 1))
else
    FALHAS=$((FALHAS + 1))
fi

# 4. Atualizar PD (SIAFE)
if executar_script "Atualizar PD" "atualizar_pd.py"; then
    SUCESSO=$((SUCESSO + 1))
else
    FALHAS=$((FALHAS + 1))
fi

# 5. Atualizar OB (SIAFE)
if executar_script "Atualizar OB" "atualizar_ob.py"; then
    SUCESSO=$((SUCESSO + 1))
else
    FALHAS=$((FALHAS + 1))
fi

# --- Resumo ---
FIM_TOTAL=$(date +%s)
DURACAO_TOTAL=$((FIM_TOTAL - INICIO_TOTAL))
MINUTOS=$((DURACAO_TOTAL / 60))
SEGUNDOS=$((DURACAO_TOTAL % 60))

log "=========================================="
log "  RESUMO: ${SUCESSO}/${TOTAL_SCRIPTS} OK | ${FALHAS} falha(s)"
log "  Tempo total: ${MINUTOS}min ${SEGUNDOS}s"
log "=========================================="

# Limpar logs antigos (manter últimos 30 dias)
find "$LOGS_DIR" -name "cron_noturno_*.log" -mtime +30 -delete 2>/dev/null || true

# Exit code: 0 se todos OK, 1 se houve falhas
if [ "$FALHAS" -gt 0 ]; then
    exit 1
fi
exit 0
