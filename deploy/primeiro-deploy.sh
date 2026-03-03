#!/bin/bash
# ================================================================
# PRIMEIRO DEPLOY - Migrar de SFTP para Git + aplicar schema
#
# Este script faz a transicao do deploy manual (SFTP) para o
# deploy via Git, e aplica TODAS as migracoes de banco pendentes
# SEM perder dados em producao.
#
# IMPORTANTE: Faca backup do banco ANTES de rodar!
#
# Uso:
#   bash deploy/primeiro-deploy.sh              # dry-run (so mostra)
#   bash deploy/primeiro-deploy.sh --executar   # aplica tudo
# ================================================================
set -e

PROJECT_DIR="/home/sead/sgc_novo"
cd "$PROJECT_DIR"

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

EXECUTAR=false
if [[ "$1" == "--executar" ]]; then
    EXECUTAR=true
fi

MODO="DRY-RUN (simulacao)"
if $EXECUTAR; then
    MODO="EXECUTAR (aplicando de verdade!)"
fi

echo -e "${CYAN}=========================================="
echo "  SGC - Primeiro Deploy Git"
echo "  Modo: $MODO"
echo "  Data: $(date '+%d/%m/%Y %H:%M:%S')"
echo -e "==========================================${NC}"

# ──────────────────────────────────────────────────────────
# PASSO 0: Verificacoes
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[0/8] Verificacoes iniciais...${NC}"

# Verificar se .env existe
if [ ! -f ".env" ]; then
    echo -e "${RED}  ERRO: Arquivo .env nao encontrado!${NC}"
    echo "  O .env de producao deve existir em $PROJECT_DIR/.env"
    exit 1
fi
echo "  [OK] .env encontrado"

# Verificar se .venv existe
if [ ! -d ".venv" ]; then
    echo -e "${RED}  ERRO: .venv nao encontrado!${NC}"
    echo "  Crie com: python3 -m venv .venv"
    exit 1
fi
echo "  [OK] .venv encontrado"

# Verificar conexao MySQL
source .venv/bin/activate
python3 -c "
import pymysql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymysql.connect(
    host=os.getenv('DB_HOST','localhost'),
    user=os.getenv('DB_USER','root'),
    password=os.getenv('DB_PASS',''),
    database=os.getenv('DB_NAME','sgc')
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s', (os.getenv('DB_NAME','sgc'),))
n = cur.fetchone()[0]
print(f'  [OK] MySQL conectado - {n} tabelas encontradas')
conn.close()
" || { echo -e "${RED}  ERRO: Falha ao conectar no MySQL${NC}"; exit 1; }

# ──────────────────────────────────────────────────────────
# PASSO 1: Backup do banco
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[1/8] Backup do banco...${NC}"

BACKUP_FILE="$HOME/sgc_backup_$(date +%Y%m%d_%H%M%S).sql"

if $EXECUTAR; then
    # Ler credenciais do .env
    DB_USER=$(grep DB_USER .env | cut -d= -f2 | tr -d "'" | tr -d '"' | tr -d ' ')
    DB_PASS=$(grep DB_PASS .env | cut -d= -f2 | tr -d "'" | tr -d '"' | tr -d ' ')
    DB_NAME=$(grep DB_NAME .env | cut -d= -f2 | tr -d "'" | tr -d '"' | tr -d ' ')
    DB_HOST=$(grep DB_HOST .env | cut -d= -f2 | tr -d "'" | tr -d '"' | tr -d ' ')

    mysqldump -h"$DB_HOST" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" > "$BACKUP_FILE" 2>/dev/null
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo -e "  ${GREEN}[OK] Backup criado: $BACKUP_FILE ($BACKUP_SIZE)${NC}"
else
    echo "  [DRY-RUN] Backup seria criado em: $BACKUP_FILE"
fi

# ──────────────────────────────────────────────────────────
# PASSO 2: Configurar Git
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[2/8] Configurando Git...${NC}"

if [ -d ".git" ]; then
    echo "  [OK] Git ja inicializado"
else
    if $EXECUTAR; then
        git init
        git remote add origin https://github.com/guichavess/sgc.git
        echo "  [OK] Git inicializado + remote adicionado"
    else
        echo "  [DRY-RUN] git init + remote add seria executado"
    fi
fi

# ──────────────────────────────────────────────────────────
# PASSO 3: Puxar codigo do GitHub
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[3/8] Puxando codigo do GitHub...${NC}"

if $EXECUTAR; then
    # Fetch do remoto
    git fetch origin main

    # Reset para alinhar com o GitHub (mantem arquivos locais nao-rastreados como .env)
    git reset origin/main

    # Restaurar todos os arquivos rastreados para a versao do GitHub
    git checkout -- .

    echo "  [OK] Codigo atualizado para: $(git log --oneline -1)"
else
    echo "  [DRY-RUN] git fetch + reset + checkout seria executado"
fi

# ──────────────────────────────────────────────────────────
# PASSO 4: Instalar/atualizar dependencias
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[4/8] Atualizando dependencias Python...${NC}"

if $EXECUTAR; then
    source .venv/bin/activate
    pip install -r requirements.txt --quiet 2>&1 | tail -3
    echo "  [OK] Dependencias atualizadas"
else
    echo "  [DRY-RUN] pip install -r requirements.txt seria executado"
fi

# ──────────────────────────────────────────────────────────
# PASSO 5: Stampar Alembic (marcar migrations como ja aplicadas)
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[5/8] Configurando Alembic (stamp)...${NC}"
echo "  NOTA: A migracao inicial (4785bde49aeb) e' um snapshot que"
echo "  dropa tabelas legadas. Nao podemos roda-la - vamos STAMPAR"
echo "  para marcar como 'ja aplicada' e pular direto para o estado atual."

if $EXECUTAR; then
    source .venv/bin/activate

    # Verificar se tabela alembic_version existe
    ALEMBIC_EXISTS=$(python3 -c "
import pymysql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymysql.connect(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'),
    password=os.getenv('DB_PASS',''), database=os.getenv('DB_NAME','sgc'))
cur = conn.cursor()
cur.execute(\"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME='alembic_version'\", (os.getenv('DB_NAME','sgc'),))
print(cur.fetchone()[0])
conn.close()
")

    if [ "$ALEMBIC_EXISTS" == "0" ]; then
        # Stampar direto no head (pula as 2 migrations perigosas)
        flask db stamp head
        echo "  [OK] Alembic stampado no HEAD (e6f219fd2404)"
    else
        CURRENT=$(flask db current 2>/dev/null | head -1)
        echo "  [INFO] Alembic ja tem versao: $CURRENT"
        flask db stamp head
        echo "  [OK] Re-stampado no HEAD"
    fi

    # Agora aplicar a logica da segunda migracao manualmente (e' segura)
    python3 -c "
import pymysql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymysql.connect(host=os.getenv('DB_HOST','localhost'), user=os.getenv('DB_USER','root'),
    password=os.getenv('DB_PASS',''), database=os.getenv('DB_NAME','sgc'))
cur = conn.cursor()
# Inserir status 'Empenho Nao Solicitado' se nao existir
cur.execute(\"\"\"INSERT INTO sis_status_empenho (id, nome, cor_badge)
    VALUES (3, 'Empenho Não Solicitado', 'secondary')
    ON DUPLICATE KEY UPDATE nome = 'Empenho Não Solicitado', cor_badge = 'secondary'\"\"\")
# Atualizar NULLs
cur.execute('UPDATE sis_solicitacoes SET status_empenho_id = 3 WHERE status_empenho_id IS NULL')
# Definir DEFAULT
cur.execute('ALTER TABLE sis_solicitacoes ALTER COLUMN status_empenho_id SET DEFAULT 3')
conn.commit()
print('  [OK] Status empenho nao-solicitado aplicado')
conn.close()
"
else
    echo "  [DRY-RUN] flask db stamp head seria executado"
    echo "  [DRY-RUN] Status empenho nao-solicitado seria aplicado"
fi

# ──────────────────────────────────────────────────────────
# PASSO 6: Criar tabelas dos modulos novos
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[6/8] Criando tabelas de modulos novos...${NC}"
echo "  Todos os scripts verificam IF NOT EXISTS - seguro rodar varias vezes"

if $EXECUTAR; then
    source .venv/bin/activate

    echo ""
    echo "  --- Modulo Usuarios (perfis + permissoes) ---"
    python3 scripts/criar_tabelas_usuarios.py --seed

    echo ""
    echo "  --- Modulo Diarias (13 tabelas) ---"
    python3 scripts/criar_tabelas_diarias.py

    echo ""
    echo "  --- Diarias Timeline (etapas + historico) ---"
    python3 scripts/criar_tabelas_diarias_timeline.py

    echo ""
    echo "  --- Notificacoes (4 tabelas + colunas em sis_usuarios e contratos) ---"
    python3 scripts/criar_tabelas_notificacoes.py

    echo ""
    echo "  --- Tipo Pagamento ---"
    python3 scripts/criar_tabela_tipo_pagamento.py

    echo ""
    echo "  --- Colunas Passagens (diarias_itinerario) ---"
    python3 scripts/adicionar_colunas_passagens.py

    echo -e "\n  ${GREEN}[OK] Todas as tabelas criadas/verificadas${NC}"
else
    echo "  [DRY-RUN] Os seguintes scripts seriam executados:"
    echo "    - criar_tabelas_usuarios.py --seed"
    echo "    - criar_tabelas_diarias.py"
    echo "    - criar_tabelas_diarias_timeline.py"
    echo "    - criar_tabelas_notificacoes.py"
    echo "    - criar_tabela_tipo_pagamento.py"
    echo "    - adicionar_colunas_passagens.py"
fi

# ──────────────────────────────────────────────────────────
# PASSO 7: Deploy catch-up (colunas em contratos/execucoes + tipificacao)
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[7/8] Schema catch-up (colunas extras)...${NC}"

if $EXECUTAR; then
    source .venv/bin/activate
    echo "  Rodando deploy_producao_catchup.py --executar..."
    python3 scripts/deploy_producao_catchup.py --executar
    echo -e "\n  ${GREEN}[OK] Catch-up concluido${NC}"
else
    echo "  [DRY-RUN] Simulando deploy_producao_catchup.py..."
    source .venv/bin/activate
    python3 scripts/deploy_producao_catchup.py
fi

# ──────────────────────────────────────────────────────────
# PASSO 8: Reiniciar servico
# ──────────────────────────────────────────────────────────
echo -e "\n${YELLOW}[8/8] Reiniciando servico...${NC}"

if $EXECUTAR; then
    # Garantir pastas
    mkdir -p logs flask_session
    chmod 775 logs flask_session

    # Dar permissao aos scripts
    chmod +x deploy.sh

    # Verificar se systemd esta configurado
    if systemctl list-unit-files | grep -q "sgc.service"; then
        echo "  Reiniciando via systemd..."
        sudo systemctl restart sgc
        sleep 3
        if systemctl is-active --quiet sgc; then
            echo -e "  ${GREEN}[OK] SGC rodando via systemd${NC}"
        else
            echo -e "  ${RED}[ERRO] Servico nao iniciou. Verificando...${NC}"
            journalctl -u sgc -n 10 --no-pager
        fi
    else
        echo "  systemd nao configurado. Reiniciando com nohup..."
        pkill -u sead gunicorn 2>/dev/null || true
        sleep 2
        source .venv/bin/activate
        nohup python3 -m gunicorn -c gunicorn.conf.py wsgi:app > /dev/null 2>&1 &
        sleep 3
        if pgrep -u sead gunicorn > /dev/null; then
            echo -e "  ${GREEN}[OK] Gunicorn rodando via nohup${NC}"
            echo ""
            echo -e "  ${YELLOW}DICA: Configure o systemd para deploy profissional:${NC}"
            echo "    sudo cp deploy/sgc.service /etc/systemd/system/"
            echo "    sudo systemctl daemon-reload"
            echo "    sudo systemctl enable sgc"
            echo "    sudo systemctl start sgc"
        else
            echo -e "  ${RED}[ERRO] Gunicorn nao iniciou. Verifique nohup.out${NC}"
        fi
    fi
else
    echo "  [DRY-RUN] Servico seria reiniciado"
fi

# ──────────────────────────────────────────────────────────
# Resumo final
# ──────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}=========================================="
if $EXECUTAR; then
    echo -e "  PRIMEIRO DEPLOY CONCLUIDO!"
    echo ""
    echo "  Backup: $BACKUP_FILE"
    echo "  Commit: $(git log --oneline -1 2>/dev/null || echo 'N/A')"
    echo ""
    echo "  Proximos deploys: ./deploy.sh"
    echo "  Com migracao:     ./deploy.sh --migrate"
else
    echo "  DRY-RUN CONCLUIDO (nada foi alterado)"
    echo ""
    echo "  Revise a saida acima. Se tudo OK, rode:"
    echo "    bash deploy/primeiro-deploy.sh --executar"
fi
echo -e "==========================================${NC}"
