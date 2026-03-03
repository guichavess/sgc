"""adicionar status empenho nao solicitado e default na coluna

Revision ID: e6f219fd2404
Revises: 4785bde49aeb
Create Date: 2026-03-02 17:55:17.950638

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e6f219fd2404'
down_revision = '4785bde49aeb'
branch_labels = None
depends_on = None


def upgrade():
    # 1. Inserir novo status "Empenho Não Solicitado" (id=3)
    op.execute(
        "INSERT INTO sis_status_empenho (id, nome, cor_badge) "
        "VALUES (3, 'Empenho Não Solicitado', 'secondary') "
        "ON DUPLICATE KEY UPDATE nome = 'Empenho Não Solicitado', cor_badge = 'secondary'"
    )

    # 2. Atualizar registros com NULL para o novo status
    op.execute(
        "UPDATE sis_solicitacoes SET status_empenho_id = 3 "
        "WHERE status_empenho_id IS NULL"
    )

    # 3. Definir DEFAULT 3 na coluna
    op.execute(
        "ALTER TABLE sis_solicitacoes ALTER COLUMN status_empenho_id SET DEFAULT 3"
    )


def downgrade():
    # Reverter: remover o default e voltar NULLs
    op.execute(
        "ALTER TABLE sis_solicitacoes ALTER COLUMN status_empenho_id DROP DEFAULT"
    )
    op.execute(
        "UPDATE sis_solicitacoes SET status_empenho_id = NULL "
        "WHERE status_empenho_id = 3"
    )
    op.execute(
        "DELETE FROM sis_status_empenho WHERE id = 3"
    )
