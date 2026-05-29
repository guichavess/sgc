# Contexto de Deploy — SGC (Sistema de Gestão de Contratos / Pagamentos)

> Arquivo de referência para gerar e executar deploys do SGC em produção.
> Mantenha atualizado se servidor, porta ou fluxo mudarem.

## Servidor de produção
- **SO**: Ubuntu 22.04.5 LTS (AWS EC2) · Python 3.10.12 · MySQL 8.0
- **Host SSH**: `sead@AWSXSEAD-GESTAO-CONTRATOS-PROD` (acesso via Bitvise; o hostname só resolve dentro da rede SEAD — o assistente **não** consegue SSH direto, então gera os comandos para o usuário colar no Bitvise)
- **Usuário**: `sead` (não-root; `sudo` exige senha)
- **Diretório do app**: `/home/sead/sgc_novo`
- **Porta**: 8081
- **WSGI**: Gunicorn (gthread, 2 workers × 4 threads, timeout 300s)
- **venv**: `.venv` · **Logs**: `logs/gunicorn.log` · **PID**: `sgc.pid`
- **Branch remota**: `master` espelha `origin/main` · **Repo**: github.com/guichavess/sgc
- **Banco**: MySQL local no servidor, schema `sgc`

## Banco local (dev) para verificação
- `root` / `root` @ `localhost`, schema `sgc`
- Repositório local: `C:\Users\guilh\OneDrive\Documentos\SEAD\Projetos\Nova pasta\pagamentos`

## Fluxo padrão de deploy
1. `git add <arquivos>` (nunca `.env`, nunca `tiposDoc.json`)
2. `git commit`
3. `git push origin main` (**só com autorização explícita do usuário**)
4. No servidor via Bitvise (SSH):

```bash
cd /home/sead/sgc_novo && source .venv/bin/activate && git pull origin main:master && rm -f sgc.pid && fuser -k 8081/tcp 2>/dev/null; sleep 2 && nohup gunicorn --bind 0.0.0.0:8081 --workers 2 --threads 4 --worker-class gthread --timeout 300 --pid sgc.pid 'app:create_app()' > logs/gunicorn.log 2>&1 & sleep 5 && tail -20 logs/gunicorn.log
```

## Comandos avulsos úteis
| Ação | Comando |
|------|---------|
| Versão no ar | `cd /home/sead/sgc_novo && git log --oneline -1` |
| Ver logs | `tail -50 /home/sead/sgc_novo/logs/gunicorn.log` |
| Derrubar | `fuser -k 8081/tcp 2>/dev/null; rm -f sgc.pid` |
| Subir sem pull | `cd /home/sead/sgc_novo && source .venv/bin/activate && nohup gunicorn --bind 0.0.0.0:8081 --workers 2 --threads 4 --worker-class gthread --timeout 300 --pid sgc.pid 'app:create_app()' > logs/gunicorn.log 2>&1 &` |

## Regras
- Antes de cada deploy: **ler `DEPLOY_PENDENTE.md`** e executar pendências de banco (SQL/ALTER) **antes** do `git pull`.
- Alteração de schema (nova tabela/coluna/índice) → registrar em `DEPLOY_PENDENTE.md`; **remover o item após aplicar em produção**.
- SQLs no servidor: entrar no MySQL (`mysql -u root -p sgc`) ou usar heredoc — **não** colar SQL direto no bash.
- **Nunca**: `git push`/`--force` sem autorização · commitar `.env` ou `tiposDoc.json` · rodar script que altere banco de produção sem ok do usuário.
- A tabela `sis_usuarios` **não** tem coluna `login` (usar `nome` / `id`).

## Estado das pendências (atualizado em 2026-05-29)
- Produção e localhost **sincronizados** em todos os itens verificados.
- **Única pendência aberta**: definir `cargo_gestao='secretario_exercicio'` para **Bruno Gomes**
  → **BLOQUEADO** até o 1º login dele no sistema (o registro em `sis_usuarios` só é criado no acesso).
- Verificação rápida do estado do banco (read-only):

```sql
SELECT id, nome, cargo_gestao FROM sis_usuarios WHERE cargo_gestao IS NOT NULL;
SELECT id, nome, ordem FROM diarias_etapas ORDER BY ordem;          -- timeline deve estar 1..6
SELECT COUNT(*) FROM diarias_itinerario WHERE etapa_atual_id = 2;   -- deve ser 0
```
