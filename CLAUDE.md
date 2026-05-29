# SGC — Sistema de Gestão de Contratos (Pagamentos)

## REGRAS ABSOLUTAS

- **TDD obrigatório**: ler arquivos → escrever testes (red) → implementar → green → commit
- **NUNCA** fazer `git push` ou executar ações em produção sem autorização explícita
- **NUNCA** rodar scripts que alterem banco de produção
- Ações destrutivas/irreversíveis: confirmar com usuário antes de executar
- Ler os arquivos completos antes de qualquer edição — verificar imports, nomes de campos, existência de funções
- `DEPLOY_PENDENTE.md`: registrar toda alteração estrutural (nova tabela/coluna/índice/dependência Python)

## DEPLOY

Antes de qualquer deploy do SGC, ler `DEPLOY_CONTEXT.md` (servidor, fluxo, comandos, regras) e `DEPLOY_PENDENTE.md` (pendências de banco a aplicar antes do `git pull`).

## TESTES

```bash
pytest tests/ -v                          # todos
pytest tests/diarias/ -v                  # módulo
pytest tests/ --cov=app --cov-report=html # cobertura
```

- SQLite in-memory, transação revertida por teste — isolado do banco real
- Fixtures globais: `tests/conftest.py`
- Estrutura: `tests/{diarias,solicitacoes,financeiro}/test_[funcionalidade].py`

| Situação | Ação |
|---|---|
| Nova função em service | `tests/[modulo]/test_[nome].py` |
| Nova rota | Testar 200, 401, 404 |
| Bug fix | Teste que falha antes do fix, passa depois |
| Feature | Happy path + edge cases |

## PADRÕES DE ENGENHARIA

- `.first()` para "mais recente" → **sempre com `.order_by()`**
- Operações dependentes → **único `commit()` no final** (atomicidade)
- Listagens → batch loading `.in_()` — nunca N+1
- Constantes → `app/constants.py` (`EtapaID`, `DiariasEtapaID`) — nunca IDs literais
- Logs → `current_app.logger`, prefixo `[MODULO] mensagem` — nunca `print()`
- Threads com `db.session` → `db.session.remove()` no `finally`
- Endpoints AJAX → IDs explícitos + validar input antes de processar
- Verificar existência de objeto → `abort(404)` antes de renderizar
- Colunas de filtro/JOIN/ORDER BY → `index=True`

## ARQUITETURA

**Stack**: Flask 3.1 · SQLAlchemy + MySQL (PyMySQL) · Flask-Login · APScheduler · Bootstrap 5.3 · Vite+React (módulo CGFR)

**Padrão de módulo** (obrigatório para módulos novos):
```
app/modulo/routes/{dashboard,crud,api,admin}.py  — rotas finas (HTTP only)
app/services/modulo_service.py                    — lógica de negócio
app/models/entidade.py                            — modelos SQLAlchemy (passivos)
app/repositories/entidade_repository.py           — DAL (queries)
app/templates/modulo/{base,navbar,dashboard,detalhes}.html
app/static/css/components/modulo.css
```

**Blueprints registrados**:

| Blueprint | Prefixo URL | Cor Navbar | Acesso |
|---|---|---|---|
| `auth_bp` | `/auth` | — | Público |
| `solicitacoes_bp` | `/solicitacoes` | Azul | `solicitacoes.*` |
| `financeiro_bp` | `/financeiro` | Azul escuro | `financeiro.*` |
| `prestacoes_contratos_bp` | `/prestacoes-contratos` | Verde | `prestacoes.*` |
| `diarias_bp` | `/diarias` | Laranja `#E07A24` | `diarias.*` |
| `usuarios_bp` | `/usuarios` | Roxo | `is_admin=True` |
| `dashboards_bp` | `/dashboards` | Teal `#1B998B` | `is_admin=True` |
| `cgfr_bp` | `/cgfr` | — | `cgfr.*` |
| `notificacoes_bp` | `/notificacoes` | — | Autenticado |

## MODELOS — CAMPOS-CHAVE

**Solicitações**
- `Solicitacao` (sis_solicitacoes): `codigo_contrato` FK, `etapa_atual_id`, `protocolo_gerado_sei`, `id_procedimento_sei`, `num_ne/nl/pd/ob`
- `SolicitacaoEmpenho` (sis_solicitacoes_empenho): `id_solicitacao` FK, `valor`, `data` ← campo é `data`, não `data_solicitacao`
- `SeiMovimentacao` (sei_movimentacao): `protocolo`, `id_procedimento`, `id_serie`, `documento_formatado`

**Diárias** (todos models em `app/models/diaria.py`)
- `DiariasItinerario` (diarias_itinerario): `etapa_atual_id` FK, `sei_protocolo`, `sei_id_procedimento`, `tipo_itinerario`, `status_id`, `valor_total`
- `DiariasItemItinerario` (diarias_itens_itinerario): `id_itinerario` FK, `cpf_pessoa`, `cargo_id`, `valor_cargo`
- `DiariasDocumento` (diarias_itinerario_documentos): `itinerario_id` FK, `tipo_documento`, `sei_id`, `sei_formatado`, `assinado`
- `DiariasNotaReserva` (diarias_notas_reserva): `itinerario_id` FK, `item_itinerario_id` FK, `codigo`, `valor`
- `DiariasNotaEmpenho` (diarias_notas_empenho): `itinerario_id` FK, `codigo`, `valor`
- `DiariasValorCargo` (diarias_valor_cargo): `cargo_id` FK, `tipo_itinerario_id`, `valor`

**Contratos/Prestações**
- `Contrato` (contratos): `codigo` PK, `tipo_contrato` (S/M/SM), `catserv_classe_id`, `catmat_pdm_id`
- `Prestacao` (execucoes): `codigo_contrato` FK, `quantidade`, `valor`, `item_vinculado_id` FK
- `ItemVinculado` (itens_vinculados): `tipo` (S/M), `catserv_id`, `catmat_id`

**Financeiro/Orçamentário**
- `Empenho` (empenho): `codigo`, `codContrato`, `valor`, `statusDocumento`
- `Loa` (loa_2026): `acao`, `natureza`, `fonte`, `saldo_disponivel`
- `Reserva` (reserva): `codContrato`, `valor`

**Usuários/Permissões**
- `Usuario` (sis_usuarios): `login`, `is_admin`, `perfil_id` FK, `cargo_gestao`, `unidade_sei_id`
- `PerfilPermissao` (perfil_permissoes): `perfil_id` FK, `modulo`, `acao`
- `is_admin` bypassa todas as verificações de permissão

## FLUXOS DE ETAPAS

**Diárias** — usar `DiariasEtapaID` de `constants.py`, nunca IDs literais:
```
Etapa 1 (Solicitação: memorando, requisição, autorização secretário)
  ↓ autorização detectada
Etapa 3 (Análise GPO + CCDP em paralelo: nota reserva, despacho GEO)
  ↓ ambos concluem
Etapa 2 (Escolha do Voo — só nacional/internacional)
  ↓
Etapa 6 (NCI: SCDP, nota empenho, despacho SGA)
  ↓
Etapa 4 (Concessão: NL, PD, OB por servidor)
  ↓
Etapa 5 (Prestação de Contas: relatório, comprovantes)
```

**Pagamentos** — SEI avança etapas via `atualizar_etapas_sei.py`:
```
1 (Criada) → 2 (Em Análise) → 3 (Aguardando NE) → 4 (NE Inserida) → 5 (Em Execução) → 6 (Concluída)
```

## INTEGRAÇÕES EXTERNAS

| Sistema | URL base | Auth | Observações |
|---|---|---|---|
| SEI | `https://api.sei.pi.gov.br` | Bearer via `sei_token.py` | `verify=False`, timeout 60s, retry 3x |
| SIAFE | `https://tesouro.sefaz.pi.gov.br/siafe-api` | Bearer via POST `/auth` | UG padrão `210101`, `verify=False` |
| SGA | `https://gestor.sead.pi.gov.br/api/pessoaSGA` | Hashkey via header | Dados de servidores para diárias |
| Trino | `10.0.122.75:8443` | usuário/senha | Catalog `iceberg`, schema `sei` (CGFR) |

## PROBLEMAS COMUNS

| Problema | Causa | Solução |
|---|---|---|
| `NoneType` em template | Rota passa `None` | `if not obj: abort(404)` |
| `.first()` retorna errado | Sem `order_by()` | `.order_by(Model.id.desc()).first()` |
| Estado inconsistente | Dois `commit()` separados | Único commit no final |
| `IntegrityError` no duplo-clique | `set_doc()` não idempotente | get_or_create com proteção |
| `500` em vez de `404` | Falta `abort(404)` | Verificar objeto antes de usar |
| Dropdown clippado (CSS) | `overflow: hidden` no card pai | `overflow: visible; z-index` no pai |
| N+1 em listagem | Query por item em template | Pre-load com `.in_()` no controller |
| `page=0` crash | Sem validação | `page = max(1, int(request.args.get('page', 1) or 1))` |
| CATSERV classes erradas | Sheet CLASSE ≠ sheet ITEM | Extrair classes do sheet ITEM |

## CHECKLIST PRÉ-ENTREGA

- [ ] Testes escritos e passando (green)
- [ ] Toda `.first()` recente tem `order_by()`
- [ ] Operações dependentes em único `commit()`
- [ ] Listagens com batch loading `.in_()`
- [ ] Constantes de `constants.py` (sem cópias locais)
- [ ] Templates recebem todas as variáveis que referenciam
- [ ] Sem imports circulares ou ausentes
- [ ] `DEPLOY_PENDENTE.md` atualizado se banco ou infra mudou

## DEPLOY_PENDENTE.md — quando registrar

| Alteração | Registrar |
|---|---|
| Nova tabela | `CREATE TABLE` SQL |
| Nova coluna | `ALTER TABLE ADD COLUMN` SQL |
| Novo índice | `CREATE INDEX` SQL |
| Nova dependência Python | `pip install` + requirements |
| Script de migração de dados | Instrução para executar em produção |

Não registrar: mudanças só em HTML, correções de lógica/bug sem impacto em banco.
