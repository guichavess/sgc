# Relatorio CGFR - De-Para de Deliberacoes

Data: 2026-06-02

## Resumo

Foi aplicada uma normalizacao nos valores de deliberacao do modulo CGFR para evitar itens duplicados no filtro do dashboard e manter o mesmo valor em exibicao, salvamento, vinculacao manual e API.

## De-Para aplicado

| De | Para |
|---|---|
| `Aprovado, com redução.` | `Aprovado com redução` |
| `Retirado de pauta` | `Retirado` |
| `Retirado de Pauta` | `Retirado` |

## Arquivos modificados

| Arquivo | Mudanca |
|---|---|
| `app/constants.py` | Removidos os aliases antigos de `CGFR_DELIBERACAO_OPTIONS`; criado `CGFR_DELIBERACAO_DE_PARA` e `normalizar_cgfr_deliberacao()`. |
| `app/cgfr/services/processo_service.py` | O salvamento de classificacao normaliza a deliberacao antes da validacao contra a lista oficial. |
| `app/cgfr/repositories/processo_local_repo.py` | A camada de persistencia tambem normaliza `deliberacao` antes de gravar. |
| `app/cgfr/models.py` | A serializacao `to_dict()` devolve deliberacao normalizada para a API do dashboard; criada propriedade `deliberacao_normalizada`. |
| `app/cgfr/routes/vincular.py` | A vinculacao manual de processo normaliza a deliberacao ao criar registro. |
| `app/templates/cgfr/detalhes.html` | A tela de detalhes exibe e envia ao modal a deliberacao normalizada. |
| `tests/cgfr/test_deliberacao_modal.py` | Adicionados testes para garantir que aliases antigos nao aparecem nas opcoes, que o salvamento normaliza e que a API nao alimenta filtros duplicados. |

## Efeito esperado no filtro

Registros antigos que ainda estejam no banco com `Aprovado, com redução.` passam a aparecer no dashboard como `Aprovado com redução`.

Registros antigos que ainda estejam no banco com `Retirado de pauta` passam a aparecer no dashboard como `Retirado`.

Com isso, o filtro de Deliberacao deixa de criar opcoes duplicadas para o mesmo significado.

## Validacao local

Comandos executados:

```bash
pytest tests/cgfr/test_deliberacao_modal.py -v
pytest tests/cgfr -v
git diff --check
```

Resultado:

- `tests/cgfr/test_deliberacao_modal.py`: 12 testes passaram.
- `tests/cgfr`: 12 testes passaram.
- `git diff --check`: sem erros; apenas avisos de LF/CRLF do Git no Windows.

## Pendencia de producao

Nao houve alteracao de schema, indice, dependencia, arquivo de configuracao ou script de migracao.

Nao foi necessario adicionar novo item ao `DEPLOY_PENDENTE.md`.

## Deploy

Depois do push para `origin/main`, o deploy de codigo no servidor deve ser executado via Bitvise SSH:

```bash
cd /home/sead/sgc_novo && source .venv/bin/activate && git pull origin main:master && rm -f sgc.pid && fuser -k 8081/tcp 2>/dev/null; sleep 2 && nohup gunicorn --bind 0.0.0.0:8081 --workers 2 --threads 4 --worker-class gthread --timeout 300 --pid sgc.pid 'app:create_app()' > logs/gunicorn.log 2>&1 & sleep 5 && tail -20 logs/gunicorn.log
```
