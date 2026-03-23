#!/bin/bash
# =============================================================================
# Backup automático do banco MySQL - SGC
# Roda via cron a cada 1 hora
# Mantém apenas o backup ATUAL (substitui o anterior)
# =============================================================================

# Carrega variáveis do .env
ENV_FILE="/home/sead/sgc_novo/.env"
if [ -f "$ENV_FILE" ]; then
    export $(grep -E '^(DB_USER|DB_PASS|DB_HOST|DB_NAME)=' "$ENV_FILE" | xargs)
fi

BACKUP_DIR="/home/sead/backups"
FILENAME="sgc_ultimo_backup.sql.gz"
LOG_FILE="$BACKUP_DIR/backup.log"

# Cria diretório se não existir
mkdir -p "$BACKUP_DIR"

# Dump compactado (substitui o anterior)
mysqldump -u "$DB_USER" -p"$DB_PASS" -h "$DB_HOST" \
    --single-transaction \
    --routines \
    --triggers \
    --quick \
    "$DB_NAME" 2>/dev/null | gzip > "$BACKUP_DIR/${FILENAME}.tmp"

# Verifica se deu certo
if [ $? -eq 0 ] && [ -s "$BACKUP_DIR/${FILENAME}.tmp" ]; then
    mv "$BACKUP_DIR/${FILENAME}.tmp" "$BACKUP_DIR/$FILENAME"
    TAMANHO=$(du -h "$BACKUP_DIR/$FILENAME" | cut -f1)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup OK: $FILENAME ($TAMANHO)" >> "$LOG_FILE"
else
    rm -f "$BACKUP_DIR/${FILENAME}.tmp"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERRO no backup!" >> "$LOG_FILE"
fi

# Manter log com no máximo 500 linhas
if [ -f "$LOG_FILE" ]; then
    tail -500 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi
