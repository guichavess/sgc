#!/bin/bash
# ================================================================
# Deploy SGC - Rodar no servidor para atualizar o sistema
#
# Uso:
#   ./deploy.sh              # deploy padrao (git pull + restart)
#   ./deploy.sh --migrate    # deploy + rodar flask db upgrade
#   ./deploy.sh --full       # deploy + migrate + pip install
# ================================================================
set -e

PROJECT_DIR="/home/sead/sgc_novo"
cd "$PROJECT_DIR"

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

MIGRATE=false
FULL=false

for arg in "$@"; do
    case $arg in
        --migrate) MIGRATE=true ;;
        --full)    FULL=true; MIGRATE=true ;;
    esac
done

echo -e "${GREEN}=========================================="
echo "  SGC Deploy - $(date '+%d/%m/%Y %H:%M:%S')"
echo -e "==========================================${NC}"

# 1. Git pull
echo -e "\n${YELLOW}[1] Atualizando codigo...${NC}"
git pull origin main
echo "  -> Commit atual: $(git log --oneline -1)"

# 2. Dependencias (so com --full)
if $FULL; then
    echo -e "\n${YELLOW}[2] Atualizando dependencias...${NC}"
    source .venv/bin/activate
    pip install -r requirements.txt --quiet
    echo "  -> Dependencias atualizadas"
else
    echo -e "\n[2] Dependencias: pulando (use --full para atualizar)"
fi

# 3. Migrations (com --migrate ou --full)
if $MIGRATE; then
    echo -e "\n${YELLOW}[3] Rodando migracoes do banco...${NC}"
    source .venv/bin/activate
    flask db upgrade
    echo "  -> Migracoes aplicadas"
else
    echo -e "\n[3] Migracoes: pulando (use --migrate para rodar)"
fi

# 4. Garantir permissoes
mkdir -p logs flask_session
chmod 775 logs flask_session

# 5. Restart com zero downtime (graceful reload)
echo -e "\n${YELLOW}[4] Reiniciando servico...${NC}"

if systemctl is-active --quiet sgc; then
    # Graceful reload - workers novos sobem antes dos antigos morrerem
    sudo systemctl reload-or-restart sgc
else
    sudo systemctl start sgc
fi

sleep 3

# 6. Verificar
if systemctl is-active --quiet sgc; then
    echo -e "\n${GREEN}=========================================="
    echo "  DEPLOY CONCLUIDO COM SUCESSO!"
    echo "  Commit: $(git log --oneline -1)"
    echo -e "==========================================${NC}"
else
    echo -e "\n${RED}ERRO: Servico nao esta rodando!${NC}"
    echo "Verifique: journalctl -u sgc -n 30"
    exit 1
fi
