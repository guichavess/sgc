# Guia de Contribuicao - SGC

## Regra de Ouro: Nunca trabalhe direto na main

A branch `main` so recebe codigo via **Pull Request** revisado.

## Fluxo de Trabalho

```
1. git pull origin main              # Atualizar main local
2. git checkout -b feature/minha-tarefa   # Criar branch
3. (fazer alteracoes e commits)
4. git push origin feature/minha-tarefa   # Enviar pro GitHub
5. Abrir Pull Request no GitHub      # Pedir revisao
6. Revisor aprova e faz merge        # Entra na main
```

## Convencao de Nomes de Branch

| Tipo | Prefixo | Exemplo |
|------|---------|---------|
| Nova funcionalidade | `feature/` | `feature/modulo-diarias` |
| Correcao de bug | `bugfix/` | `bugfix/erro-conexao-banco` |
| Melhoria de UI | `ui/` | `ui/redesign-hub` |
| Scripts/dados | `scripts/` | `scripts/importar-loa` |
| Documentacao | `docs/` | `docs/guia-contribuicao` |

## Convencao de Commits

Usar prefixos descritivos:

- `feat:` nova funcionalidade
- `fix:` correcao de bug
- `ui:` alteracao visual/template
- `refactor:` refatoracao sem mudar comportamento
- `scripts:` scripts de importacao/atualizacao

Exemplos:
```
feat: adicionar tela de fornecedores sem contrato
fix: corrigir calculo de diarias nacionais
ui: redesenhar cards do hub
scripts: script de atualizacao LOA via SIAFE
```

## Antes de Criar um PR

1. Testar localmente (`python run.py`)
2. Verificar se nao quebrou nada existente
3. Nao commitar arquivos .env, .xlsx, .sql, ou dados sensiveis

## Setup Local

Ver o prompt de configuracao no README ou pedir ao Claude Code:
```
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
npm install && npm run build
copy .env.example .env      # Preencher credenciais
python run.py
```
