#!/bin/bash
# ================================================================
# Setup inicial do servidor (rodar UMA VEZ com sudo)
# Configura Git, systemd e permissoes
# ================================================================
set -e

PROJECT_DIR="/home/sead/sgc_novo"
REPO_URL="https://github.com/guichavess/sgc.git"

echo "=========================================="
echo "  SGC - Setup do Servidor de Producao"
echo "=========================================="

# 1. Instalar git se nao tiver
if ! command -v git &> /dev/null; then
    echo "[1/5] Instalando git..."
    apt-get update && apt-get install -y git
else
    echo "[1/5] Git ja instalado: $(git --version)"
fi

# 2. Inicializar git no diretorio existente (sem perder nada)
echo "[2/5] Configurando Git no projeto..."
cd "$PROJECT_DIR"

if [ ! -d ".git" ]; then
    git init
    git remote add origin "$REPO_URL"
    git fetch origin
    # Reset para alinhar com o remoto SEM apagar arquivos locais
    git reset origin/main
    git checkout -- .gitignore
    echo "  -> Git inicializado e alinhado com GitHub"
else
    echo "  -> Git ja configurado"
fi

# 3. Criar diretorios necessarios
echo "[3/5] Criando diretorios..."
mkdir -p logs flask_session
chmod 775 logs flask_session
chown -R sead:sead logs flask_session

# 4. Instalar servico systemd
echo "[4/5] Instalando servico systemd..."
cp "$PROJECT_DIR/deploy/sgc.service" /etc/systemd/system/sgc.service
systemctl daemon-reload
systemctl enable sgc
echo "  -> Servico sgc habilitado no boot"

# 5. Matar processos antigos (nohup) se existirem
echo "[5/5] Limpando processos antigos..."
pkill -u sead gunicorn 2>/dev/null || true
sleep 2

# Iniciar via systemd
systemctl start sgc
sleep 3

if systemctl is-active --quiet sgc; then
    echo ""
    echo "=========================================="
    echo "  SETUP COMPLETO!"
    echo "  SGC rodando via systemd na porta 8081"
    echo ""
    echo "  Comandos uteis:"
    echo "    systemctl status sgc     # ver status"
    echo "    systemctl restart sgc    # reiniciar"
    echo "    journalctl -u sgc -f     # ver logs ao vivo"
    echo ""
    echo "  Para deploy: ~/sgc_novo/deploy.sh"
    echo "=========================================="
else
    echo "ERRO: Servico nao iniciou. Verifique:"
    echo "  journalctl -u sgc -n 50"
fi
