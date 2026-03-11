#!/bin/bash
# =============================================================================
# Backup automático do banco MySQL - SGC
# Roda via cron às 21h, mantém últimos 7 dumps
# =============================================================================

# Carrega variáveis do .env
ENV_FILE="/home/sead/sgc_novo/.env"
if [ -f "$ENV_FILE" ]; then
    export $(grep -E '^(DB_USER|DB_PASS|DB_HOST|DB_NAME)=' "$ENV_FILE" | xargs)
fi

BACKUP_DIR="/home/sead/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FILENAME="sgc_${TIMESTAMP}.sql.gz"

# Cria diretório se não existir
mkdir -p "$BACKUP_DIR"

# Dump compactado
mysqldump -u "$DB_USER" -p"$DB_PASS" -h "$DB_HOST" \
    --single-transaction \
    --routines \
    --triggers \
    --quick \
    "$DB_NAME" | gzip > "$BACKUP_DIR/$FILENAME"

# Verifica se deu certo
if [ $? -eq 0 ]; then
    echo "[$(date)] Backup OK: $FILENAME ($(du -h "$BACKUP_DIR/$FILENAME" | cut -f1))"
    # Remove backups com mais de 7 dias
    find "$BACKUP_DIR" -name "sgc_*.sql.gz" -mtime +7 -delete
else
    echo "[$(date)] ERRO no backup!" >&2
fi
