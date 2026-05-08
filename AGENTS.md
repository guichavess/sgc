# SGC — Sistema de Gestão de Contratos (Pagamentos)

> Instruções obrigatórias. Este arquivo é lido antes de qualquer ação.

---

## 0. IDENTIDADE E POSTURA

Você atua como **engenheiro de software sênior** neste projeto. Isso significa:
- Código limpo, consistente e produção-ready em cada alteração.
- Cada decisão técnica deve ser justificável e alinhada com a arquitetura existente.
- Priorizar manutenibilidade e legibilidade sobre "funcionar rápido".
- Manter identidade visual e funcional coerente entre todos os módulos do sistema.

---

## 1. RACIOCÍNIO E PLANEJAMENTO (OBRIGATÓRIO ANTES DE CADA AÇÃO)

Antes de qualquer chamada de ferramenta ou resposta, raciocinar sobre:

### 1.1 Dependências lógicas e restrições
- Analisar a ação pretendida contra regras, pré-requisitos e restrições do projeto.
- Verificar ordem das operações: uma ação não pode impedir uma ação subsequente necessária.
- O usuário pode solicitar ações em ordem aleatória — reordená-las se necessário para maximizar sucesso.
- Respeitar restrições e preferências explícitas do usuário.
- Resolver conflitos na ordem: políticas > ordem de operações > pré-requisitos > preferências.

### 1.2 Avaliação de risco
- Quais são as consequências da ação? O novo estado causará problemas futuros?
- Para tarefas exploratórias (pesquisas), a falta de parâmetros opcionais é risco BAIXO — prosseguir sem perguntar.
- Para ações destrutivas ou irreversíveis — SEMPRE confirmar com o usuário.

### 1.3 Raciocínio abdutivo
- Identificar a causa mais lógica e provável para qualquer problema.
- Olhar além das causas óbvias — a razão mais provável pode exigir inferência mais profunda.
- Priorizar hipóteses por probabilidade, mas não descartar prematuramente as menos prováveis.

### 1.4 Adaptabilidade
- Após cada ação, avaliar: o resultado exige alteração no plano?
- Se hipóteses iniciais forem refutadas, gerar novas com base nas informações coletadas.
- Não repetir a mesma ação falhada — mudar estratégia ou argumentos.

### 1.5 Fontes de informação (usar todas)
- Ferramentas disponíveis e suas capacidades.
- Políticas, regras, checklists e restrições (este arquivo, ENGINEERING_STANDARDS.md).
- Observações anteriores e histórico da conversa.
- Memory do projeto (`.Codex/projects/.../memory/`).
- Perguntar ao usuário quando a informação não está disponível de outra forma.

### 1.6 Precisão e fundamentação
- Verificar afirmações citando informações exatas.
- Não presumir — confirmar lendo o código real.

### 1.7 Completude
- Todos os requisitos, restrições e preferências devem ser incorporados.
- Não concluir prematuramente: pode haver múltiplas opções relevantes.

### 1.8 Persistência inteligente
- Não desistir por tempo gasto ou frustração do usuário.
- Em erros transitórios, tentar novamente (até limite razoável).
- Em outros erros, mudar estratégia — nunca repetir a mesma chamada falhada.

---

## 2. ⚠️ OBRIGAÇÃO DE TESTES — TODA MUDANÇA PASSA POR TESTES PRIMEIRO

> **REGRA INVIOLÁVEL**: Nenhuma implementação de funcionalidade nova ou correção de bug
> é considerada finalizada sem testes que a cubram.

### 2.1 Fluxo obrigatório para cada mudança

```
1. Ler os arquivos envolvidos
2. Escrever o(s) teste(s) que cobrem o comportamento esperado
3. Confirmar que os testes FALHAM (red) — provando que o bug existe / feature não existe
4. Implementar a mudança
5. Confirmar que os testes PASSAM (green)
6. Commit apenas quando green
```

### 2.2 Onde ficam os testes

```
tests/
├── conftest.py                    ← fixtures globais (app, db, client)
├── diarias/
│   ├── test_bug01_valor_cargo.py  ← BUG-01: get_valor_cargo sem order_by
│   ├── test_bug03_atomicidade.py  ← BUG-03: commits não atômicos
│   ├── test_bug04_bug06_rotas.py  ← BUG-04: 404 vs 500 / BUG-06: page validation
│   └── test_bug07_set_doc_race.py ← BUG-07: race condition em set_doc
├── solicitacoes/
│   └── (testes do módulo de pagamentos)
├── financeiro/
│   └── (testes do módulo financeiro)
└── [modulo]/
    └── test_[funcionalidade].py
```

### 2.3 Como rodar os testes

```bash
# Instalar dependências de teste (uma vez)
pip install -r requirements-test.txt

# Rodar todos os testes
pytest tests/ -v

# Rodar apenas um módulo
pytest tests/diarias/ -v

# Rodar apenas um arquivo
pytest tests/diarias/test_bug01_valor_cargo.py -v

# Rodar apenas testes de bugs
pytest tests/ -v -m bug

# Ver cobertura (após instalar pytest-cov)
pytest tests/ --cov=app --cov-report=html
```

### 2.4 Banco de testes

- Usa **SQLite in-memory** — completamente isolado do banco de desenvolvimento/produção.
- Configuração em `app/config.py → TestingConfig`.
- Cada teste roda em transação que é **revertida** no teardown — zero contaminação entre testes.

### 2.5 Quando criar novos testes

| Situação | Ação obrigatória |
|----------|-----------------|
| Nova função em service | Criar `tests/[modulo]/test_[nome_service].py` |
| Nova rota | Adicionar testes de resposta HTTP (200, 401, 404) |
| Correção de bug | Criar teste que falha ANTES do fix e passa DEPOIS |
| Nova feature | Criar testes de happy path + edge cases |
| Refatoração | Garantir que testes existentes ainda passam |

---

## 3. AMBIENTE DE DESENVOLVIMENTO

- **TODAS as alterações são feitas no ambiente LOCAL.**
- NUNCA executar comandos em produção sem autorização explícita.
- NUNCA fazer `git push` sem autorização explícita.
- NUNCA executar scripts que alterem banco de dados de produção.
- Quando o usuário pedir para "implementar" ou "fazer" algo, assume-se LOCAL.
- Deploy e produção são etapas separadas que o usuário controla manualmente.

---

## 4. DISCIPLINA DE BUGS E VERIFICAÇÃO

### 4.1 Antes de implementar
- SEMPRE ler os arquivos completos que serão modificados para entender o contexto.
- Verificar imports existentes — não duplicar, não quebrar.
- Verificar se a função/variável/classe que será usada realmente existe no código.
- Conferir nomes exatos de campos nos modelos (ex: `data` vs `data_solicitacao`).

### 4.2 Durante a implementação
- Após cada alteração, verificar mentalmente se introduziu:
  - Import circular ou ausente.
  - Variável referenciada antes de ser definida.
  - Tipo incompatível (IntEnum vs int, string vs number).
  - Query sem `order_by()` quando precisa do "mais recente".
  - `commit()` duplicado em operações que deveriam ser atômicas.
  - Template referenciando variável que a rota não passa.
- Se o código envolve condicionais, testar mentalmente todos os branches.

### 4.3 Após implementar
- Revisar o diff completo antes de considerar finalizado.
- Rodar os testes relevantes e confirmar que passam.
- Verificar se a alteração não quebrou nada adjacente.
- Se criou nova rota: verificar que o blueprint está registrado e o URL prefix está correto.
- Se alterou modelo: verificar se a tabela/coluna existe no banco (ou se precisa de migration).

---

## 5. PADRÕES DE ENGENHARIA

### 5.1 Banco de dados
- Operações dependentes = um único `commit()` no final (atomicidade).
- Toda `.first()` para "mais recente" DEVE ter `order_by()`.
- Colunas de filtro/JOIN/ORDER BY devem ter `index=True`.
- Usar `EXISTS` para verificação de existência, não `.first() is not None`.

### 5.2 Performance
- Eliminar N+1: batch loading com `.in_()`, nunca query-por-item em listagens.
- Queries escopadas: nunca `.all()` sem filtro em tabelas grandes.
- Lazy computation: em páginas com abas, só processar dados da aba ativa.
- Pre-load no dashboard para properties que fazem queries.

### 5.3 Constantes
- Importar de `app/constants.py` — nunca duplicar mapas/constantes localmente.
- Usar Enums (`EtapaID`, `DiariasEtapaID`) — nunca IDs numéricos literais.

### 5.4 APIs/AJAX
- Endpoints devem receber IDs explícitos.
- Validar todos os inputs antes de processar.

### 5.5 Logging
- `current_app.logger`, nunca `print()`.
- Prefixos de contexto: `[MODULO] mensagem`.

### 5.6 Thread safety
- Threads com `db.session` devem fazer `db.session.remove()` no `finally`.

---

## 6. ARQUITETURA DO SISTEMA

### 6.1 Stack tecnológico

| Camada | Tecnologia | Versão |
|--------|-----------|--------|
| Framework Web | Flask | 3.1.2 |
| ORM | SQLAlchemy + Flask-SQLAlchemy | 2.0.46 / 3.1.1 |
| Banco de Dados | MySQL via PyMySQL | 1.1.2 |
| Migrações | Flask-Migrate + Alembic | 4.1.0 / 1.18.3 |
| Autenticação | Flask-Login | 0.6.3 |
| Sessões | Flask-Session (filesystem) | 0.8.0 |
| Scheduler | APScheduler | 3.10.4 |
| WSGI Server | Gunicorn | 25.0.1 |
| Frontend | Bootstrap 5.3 + Bootstrap Icons |  |
| Build Tool | Vite 5.0.0 + React 19.2.4 | (módulo CGFR) |
| HTTP Client | requests | 2.32.5 |
| Data Lake | trino | 0.329.0 |
| Notificações | Telethon / Telegram Bot API | 1.42.0 |
| Excel/Pandas | pandas + openpyxl | latest |

### 6.2 Arquitetura de módulos (padrão obrigatório)

```
app/modulo/routes/{dashboard,crud,api,admin}.py   — Rotas (finas)
app/services/modulo_service.py                     — Lógica de negócio
app/models/entidade.py                             — Modelos SQLAlchemy
app/repositories/entidade_repository.py            — Data Access Layer
app/templates/modulo/{base,navbar,dashboard,detalhes}.html
app/static/css/components/modulo.css               — CSS modular
```

### 6.3 Separação de responsabilidades

| Camada | Faz | Acessa |
|--------|-----|--------|
| Routes | HTTP, request/response, flash | Services |
| Services | Lógica de negócio, validações | Models, Repositories, db.session |
| Repositories | Queries específicas, filtros | Models, db.session |
| Models | Definição de tabelas, relationships | Nada (passivo) |

### 6.4 Blueprints registrados

| Blueprint | Prefixo URL | Cor Navbar | Acesso |
|-----------|-------------|------------|--------|
| `auth_bp` | `/auth` | — | Público |
| `solicitacoes_bp` | `/solicitacoes` | Azul | Perfil solicitacoes.* |
| `financeiro_bp` | `/financeiro` | Azul escuro | Perfil financeiro.* |
| `prestacoes_contratos_bp` | `/prestacoes-contratos` | Verde | Perfil prestacoes.* |
| `diarias_bp` | `/diarias` | Laranja `#E07A24` | Perfil diarias.* |
| `usuarios_bp` | `/usuarios` | Roxo | is_admin=True |
| `dashboards_bp` | `/dashboards` | Teal `#1B998B` | is_admin=True |
| `cgfr_bp` | `/cgfr` | — | Perfil cgfr.* |
| `notificacoes_bp` | `/notificacoes` | — | Autenticado |

---

## 7. ESTRUTURA DE DIRETÓRIOS

```
pagamentos/
├── app/
│   ├── auth/routes.py              — Login/logout SEI
│   ├── cgfr/routes/                — Vinculação/acompanhamento CGFR
│   ├── clients/trino_client.py     — Data Lake Trino
│   ├── dashboards/routes/          — Dashboards analíticos (admin)
│   ├── diarias/routes/             — Módulo de viagens/diárias
│   │   ├── api.py
│   │   └── admin.py
│   ├── financeiro/routes/          — Módulo financeiro
│   │   ├── dashboard.py, pendencias.py, api.py
│   │   ├── fornecedores.py, execucoes.py, orcamentaria.py
│   │   └── diarias.py              — Fluxo financeiro das diárias
│   ├── models/                     — Todos os modelos SQLAlchemy
│   │   ├── contrato.py, solicitacao.py, empenho.py
│   │   ├── diaria.py               — TODOS os models do módulo diárias
│   │   ├── catserv.py, catmat.py   — Catálogos CATSERV/CATMAT
│   │   ├── notificacao.py, usuario.py, perfil.py
│   │   └── loa.py, reserva.py, fornecedor.py, execucao_orcamentaria.py
│   ├── notificacoes/               — Sistema de notificações in-app
│   ├── prestacoes_contratos/routes/ — Prestações de contratos
│   ├── repositories/               — Data Access Layer
│   │   ├── contrato_repository.py
│   │   ├── saldo_repository.py
│   │   └── notificacao_repository.py
│   ├── services/                   — Toda a lógica de negócio
│   │   ├── diaria_service.py       — Serviço principal de diárias
│   │   ├── sei_service.py          — Integração SEI API
│   │   ├── sei_auth.py, sei_token.py
│   │   ├── siafe_service.py        — Integração SIAFE API
│   │   ├── sga_service.py          — Consulta servidores SGA
│   │   ├── notification_engine.py  — Orquestrador de notificações
│   │   ├── email_service.py
│   │   ├── telegram_bot_service.py
│   │   ├── usuario_service.py
│   │   └── scheduler.py            — Jobs APScheduler (3 jobs)
│   ├── solicitacoes/routes/        — Módulo de solicitações de pagamento
│   ├── usuarios/routes/            — Gestão de usuários (admin)
│   ├── utils/                      — Helpers: formatters, vite, permissions
│   ├── config.py                   — Todas as configurações/variáveis de ambiente
│   ├── constants.py                — Enums, constantes, mapeamentos
│   ├── extensions.py               — db, login_manager, session, migrate
│   └── __init__.py                 — Factory create_app()
├── scripts/                        — 90+ scripts de manutenção/sync
│   ├── atualizar_{reserva,empenho,liquidacao,pd,ob,contratos,loa}.py
│   └── criar_tabelas_*.py, migrar_*.py, importar_*.py
├── migrations/                     — Alembic migrations
├── tests/                          — ⚠️ AMBIENTE DE TESTES (obrigatório)
│   ├── conftest.py
│   └── diarias/test_bug*.py
├── data/                           — CSVs de dados iniciais
├── static/                         — CSS/JS/imagens
│   ├── css/components/             — CSS modular por módulo
│   └── dist/                       — Build do Vite
├── templates/                      — Templates base globais
├── .env                            — ⚠️ NUNCA commitar
├── requirements.txt
├── requirements-test.txt
├── pytest.ini
└── DEPLOY_PENDENTE.md
```

---

## 8. VARIÁVEIS DE AMBIENTE

> **Para deploy via Bitvise SSH**: Todas as variáveis são carregadas do arquivo `.env`
> na raiz do projeto. Copiar `.env.example` para `.env` e preencher antes de iniciar.
> O gunicorn lê o `.env` automaticamente via `python-dotenv` no `create_app()`.

### 8.1 Variáveis CRÍTICAS (obrigatórias em produção)

```bash
# Flask
SECRET_KEY=gere_uma_chave_segura_com_python_-c_"import_secrets;print(secrets.token_hex(32))"
FLASK_ENV=production
FLASK_DEBUG=0

# Banco de Dados MySQL
DB_USER=usuario_mysql
DB_PASS=senha_mysql
DB_HOST=localhost
DB_NAME=sgc

# SEI — Sistema Eletrônico de Informações
SEI_API_URL=https://api.sei.pi.gov.br
SEI_USUARIO_ADMIN=email@sead.pi.gov.br
SEI_SENHA_ADMIN=senha_sei
SEI_UNIDADE=110006213
SEI_ORGAO=SEAD-PI

# SIAFE — Financeiro
SIAFE_URL=https://tesouro.sefaz.pi.gov.br/siafe-api
SIAFE_USUARIO=cpf_siafe
SIAFE_SENHA=senha_siafe
```

### 8.2 Variáveis importantes (configurar se usar a funcionalidade)

```bash
# Telegram Bot (notificações)
TELEGRAM_BOT_TOKEN=token_do_bot_telegram

# Email SMTP
MAIL_SERVER=smtp.gmail.com
MAIL_PORT=587
MAIL_USE_TLS=True
MAIL_USERNAME=email@gmail.com
MAIL_PASSWORD=senha_de_app_gmail

# SGA — Consulta de servidores (diárias)
SGA_API_URL=https://gestor.sead.pi.gov.br/api/pessoaSGA
SGA_API_HASHKEY=hash_da_api_sga

# Trino Data Lake (módulo CGFR)
TRINO_HOST=10.0.122.75
TRINO_PORT=8443
TRINO_USER=usuario_trino
TRINO_PASSWORD=senha_trino
TRINO_CATALOG=iceberg
TRINO_SCHEMA=sei

# CATMAT — Banco externo (scripts de sync)
CATMAT_DB_HOST=10.0.122.94
CATMAT_DB_USER=usuario
CATMAT_DB_PASS=senha
CATMAT_DB_NAME=catmatpi
CATMAT_DB_PORT=3306

# Logging
LOG_DIR=logs
LOG_FILE=sistema_pagamentos.log
```

### 8.3 Variáveis com valores padrão (raramente alterar)

```bash
SEI_TOKEN_MAX_AGE=1500        # Segundos antes de renovar token SEI (25 min)
TRINO_TIMEOUT=120             # Timeout Trino em segundos
LOG_MAX_BYTES=10240
LOG_BACKUP_COUNT=10
VITE_DEV=0                    # 1 apenas em desenvolvimento com Vite dev server
```

---

## 9. MODELOS POR MÓDULO

### SOLICITAÇÕES

| Model | Tabela | Campos-chave |
|-------|--------|--------------|
| `Solicitacao` | `sis_solicitacoes` | `codigo_contrato` FK, `etapa_atual_id`, `protocolo_gerado_sei`, `id_procedimento_sei`, `link_processo_sei`, `especificacao`, `competencia`, `num_ne/nl/pd/ob` |
| `SolicitacaoEmpenho` | `sis_solicitacoes_empenho` | `id_solicitacao` FK, `valor`, `data` |
| `Etapa` | `sis_etapas_fluxo` | `nome`, `ordem` |
| `HistoricoMovimentacao` | `sis_historico_movimentacao` | `id_solicitacao` FK, `id_etapa_anterior`, `id_etapa_atual`, `data_movimentacao` |
| `SeiMovimentacao` | `sei_movimentacao` | `protocolo`, `id_procedimento`, `id_serie`, `documento_formatado` |

### CONTRATOS / PRESTAÇÕES

| Model | Tabela | Campos-chave |
|-------|--------|--------------|
| `Contrato` | `contratos` | `codigo` PK, `tipo_contrato` (S/M/SM), `catserv_classe_id`, `catmat_pdm_id`, `vigência`, `valor` |
| `Prestacao` | `execucoes` | `codigo_contrato` FK, `quantidade`, `valor`, `item_vinculado_id` FK |
| `SaldoContrato` | `saldo_contrato` | `codigo_contrato` FK, `saldo_global` |
| `ItemVinculado` | `itens_vinculados` | `tipo` (S/M), `catserv_id`, `catmat_id` |

### DIÁRIAS

| Model | Tabela | Campos-chave |
|-------|--------|--------------|
| `DiariasItinerario` | `diarias_itinerario` | `etapa_atual_id` FK, `sei_protocolo`, `sei_id_procedimento`, `unidade_geradora_id`, `tipo_itinerario`, `status_id`, `valor_total` |
| `DiariasItemItinerario` | `diarias_itens_itinerario` | `id_itinerario` FK, `cpf_pessoa`, `cargo_id`, `valor_cargo` |
| `DiariasDocumento` | `diarias_itinerario_documentos` | `itinerario_id` FK, `tipo_documento`, `sei_id`, `sei_formatado`, `assinado` |
| `DiariasNotaReserva` | `diarias_notas_reserva` | `itinerario_id` FK, `item_itinerario_id` FK, `codigo`, `valor` |
| `DiariasNotaEmpenho` | `diarias_notas_empenho` | `itinerario_id` FK, `codigo`, `valor` |
| `DiariasValorCargo` | `diarias_valor_cargo` | `cargo_id` FK, `tipo_itinerario_id`, `valor` |
| `DiariasEtapa` | `diarias_etapas` | `id` (enum), `nome`, `ordem` |
| `DiariasHistoricoMovimentacao` | `diarias_historico_movimentacao` | `itinerario_id` FK, `etapa_id`, `usuario_id`, `descricao` |

### USUÁRIOS / PERMISSÕES

| Model | Tabela | Campos-chave |
|-------|--------|--------------|
| `Usuario` | `sis_usuarios` | `login`, `is_admin`, `perfil_id` FK, `cargo_gestao`, `unidade_sei_id`, `superintendencia_sigla` |
| `Perfil` | `perfis` | `nome`, `ativo` |
| `PerfilPermissao` | `perfil_permissoes` | `perfil_id` FK, `modulo`, `acao` |

### FINANCEIRO / ORÇAMENTÁRIO

| Model | Tabela | Campos-chave |
|-------|--------|--------------|
| `Empenho` | `empenho` | `codigo`, `codContrato`, `valor`, `statusDocumento` |
| `Liquidacao` | `liquidacao` | `codigo`, `codProcesso`, `valor` |
| `Reserva` | `reserva` | `codContrato`, `valor` |
| `Loa` | `loa_2026` | `acao`, `natureza`, `fonte`, `saldo_disponivel` |

---

## 10. SERVIÇOS E JOBS

### Services

| Serviço | Arquivo | Responsabilidade |
|---------|---------|-----------------|
| `DiariaService` | `diaria_service.py` | Cálculos, CRUD de itinerários, timeline, obter_timeline() |
| `SolicitacaoService` | `solicitacao_service.py` | Notificações pós-criação, avanço de etapa |
| `SaldoService` | `saldo_service.py` | Movimentações de saldo de contratos |
| `SeiService` | `sei_service.py` | Consulta procedimentos, lista documentos SEI |
| `SiafeService` | `siafe_service.py` | Validação de NE, autenticação SIAFE |
| `SgaService` | `sga_service.py` | Consulta dados de servidores |
| `NotificationEngine` | `notification_engine.py` | Orquestra email + Telegram + in-app |
| `UsuarioService` | `usuario_service.py` | Sync com SEI, gestão de perfis |
| `SeiToken` | `sei_token.py` | Gerencia renovação automática do token SEI |

### Jobs APScheduler (em `scheduler.py`)

| Job | Schedule | O que faz |
|-----|----------|-----------|
| `verificar_vigencias` | Diário 08:00 | Alerta contratos expirando em ≤90 dias (3 níveis de escalação) |
| `lembrete_ne_pendentes` | Diário 09:00 | Notifica NEs pendentes de inserção |
| `limpar_expiradas` | Domingo 03:00 | Remove notificações >30 dias do banco |

### Scripts de Sincronização (`scripts/`)

| Script | Quando rodar | O que faz |
|--------|-------------|-----------|
| `atualizar_empenho.py` | Diário automático | Sincroniza empenhos do SIAFE |
| `atualizar_reserva.py` | Diário automático | Sincroniza reservas orçamentárias |
| `atualizar_liquidacao.py` | Diário automático | Sincroniza liquidações |
| `atualizar_pd.py` | Diário automático | Sincroniza PDs |
| `atualizar_ob.py` | Diário automático | Sincroniza OBs |
| `atualizar_loa.py` | Manual/mensal | Atualiza LOA do ano (`--years 2026`) |
| `atualizar_etapas_sei.py` | Diário automático | Sincroniza etapas pelo SEI |
| `criar_tabelas_*.py` | Deploy (uma vez) | Cria estrutura de banco |

---

## 11. INTEGRAÇÕES EXTERNAS

### SEI API

- **Base URL**: `https://api.sei.pi.gov.br`
- **Auth**: Token Bearer, renovado automaticamente via `sei_token.py`
- **Timeout**: 60s com retry 3x
- **SSL**: `verify=False` (certificado self-signed do PI)
- **Endpoints usados**:
  - `GET /v1/unidades/{id}/procedimentos/consulta` — consulta processo
  - `GET /v1/unidades/{id}/procedimentos/documentos` — lista documentos
  - `POST /v1/autenticar` — autenticação

### SIAFE API

- **Base URL**: `https://tesouro.sefaz.pi.gov.br/siafe-api`
- **Auth**: Bearer token via POST `/auth`
- **UG padrão**: `210101` (hardcoded)
- **Endpoints usados**:
  - `POST /auth` — autenticação
  - `POST /nota-empenho/{exercicio}` — validação de NE

### SGA (Servidores)

- **URL**: `https://gestor.sead.pi.gov.br/api/pessoaSGA`
- **Auth**: Hashkey via header
- **Uso**: Busca dados de servidores para criação de diárias

### Trino Data Lake

- **Host**: `10.0.122.75:8443`
- **Catálogo**: `iceberg`, schema `sei`
- **Uso**: Módulo CGFR para leitura de dados SEI do Data Lake

---

## 12. FLUXO DE ETAPAS — DIÁRIAS

```
[Etapa 1] Solicitação Inicial
    → Memorando, Requisição, Autorização Secretário
    → sei_protocolo é vinculado aqui
    ↓ (autorização do Secretário detectada)
[Etapa 3] Análise 1ª Parte (GPO + CCDP em paralelo)
    GPO:  → Nota de Reserva (1 por servidor) + Quadro Orçamentário
    CCDP: → Despacho GEO + Análise de Habilitação
    ↓ (quando GPO e CCDP concluem)
[Etapa 2] Escolha do Voo (apenas tipos nacional/internacional)
    → Cotações aéreas, justificativa
    ↓
[Etapa 6] Análise 2ª Parte (NCI)
    → SCDP, Nota Empenho, Despacho SGA, Análise NCI
    ↓
[Etapa 4] Concessão de Diárias
    → NL, PD, OB (1 por servidor)
    ↓
[Etapa 5] Prestação de Contas
    → Relatório Viagem, Comprovantes, Nota Patrimonial
```

**Constantes**: usar `DiariasEtapaID` do `app/constants.py` — nunca IDs literais.

---

## 13. FLUXO DE SOLICITAÇÃO DE PAGAMENTO

```
[Etapa 1] Criada → [Etapa 2] Em Análise → [Etapa 3] Aguardando NE
→ [Etapa 4] NE Inserida → [Etapa 5] Em Execução → [Etapa 6] Concluída
```

SEI detecta e avança etapas automaticamente via `atualizar_etapas_sei.py`.

---

## 14. PADRÕES DE DESIGN DO PROJETO

1. **Factory Pattern** — `create_app()` para inicialização
2. **Repository Pattern** — Data Access Layer isolada em `repositories/`
3. **Service Layer** — Lógica de negócio separada de rotas
4. **Blueprint Pattern** — Módulos Flask independentes
5. **Strategy Pattern** — `get_config()` retorna config por ambiente
6. **Observer Pattern** — `NotificationEngine` dispara notificações por eventos
7. **Template Method** — `base_*.html` define estrutura; `navbar_*.html` personaliza
8. **Singleton (implícito)** — `db`, `login_manager` via extensões Flask
9. **Command Pattern** — scripts em `scripts/` são comandos executáveis isolados
10. **Facade Pattern** — `SeiService`, `SiafeService` encapsulam APIs complexas
11. **State Pattern** — `etapa_atual_id` representa estado da solicitação/itinerário
12. **Namespace Pattern (Jinja)** — `{% set pn = namespace(n=0) %}` para contadores em templates
13. **Idempotency Pattern** — `set_doc()` deve ser idempotente (get_or_create)
14. **Circuit Breaker (implícito)** — retry 3x com timeout nos clients SEI/SIAFE

---

## 15. PROBLEMAS COMUNS E SOLUÇÕES

| # | Problema | Causa | Solução |
|---|---------|-------|---------|
| 1 | `AttributeError: 'NoneType' has no attribute 'x'` em template | Route passa `None` sem verificar | `if not obj: abort(404)` antes de renderizar |
| 2 | Query retorna registro errado quando há múltiplos | `.first()` sem `order_by()` | Adicionar `.order_by(Model.id.desc()).first()` |
| 3 | Estado inconsistente (doc salvo, movimentação não) | Dois `commit()` separados | Consolidar em único commit ao final |
| 4 | `IntegrityError` em duplo-clique | `set_doc()` não é idempotente | Implementar get_or_create com proteção |
| 5 | Token SEI expirado em requests longos | Token de 25min sem renovação | `sei_token.py` renova automaticamente via `before_request` |
| 6 | `500` em vez de `404` para ID inexistente | Falta `abort(404)` na rota | Verificar objeto antes de usar |
| 7 | Dropdown do filtro clippado (CSS) | `overflow: hidden` no card pai | Adicionar `overflow: visible; z-index` no pai |
| 8 | N+1 queries em listagem | Property com query em loop de template | Pre-load no controller com `.in_()` |
| 9 | Template não exibe dados do SEI | `sei_protocolo` NULL | Vincular processo SEI antes de gerar documentos |
| 10 | Erro de encoding Windows (`â€œ` em logs) | UTF-8 no DB, cp1252 no terminal | Dados corretos no banco; terminal mostra errado |
| 11 | `page=0` causa crash no paginate | Sem validação de query param | `page = max(1, int(request.args.get('page', 1) or 1))` |
| 12 | CATSERV classe com código 0 overlap com CATMAT | Sheets diferentes têm sistemas de código distintos | Extrair classes do sheet ITEM, não CLASSE |

---

## 16. CHECKLIST PRÉ-ENTREGA

Antes de considerar qualquer tarefa finalizada:

- [ ] **Testes escritos** e passando (obrigatório — seção 2)
- [ ] Toda `.first()` que precisa do "mais recente" tem `order_by()`?
- [ ] Operações dependentes compartilham um único `commit()`?
- [ ] Listagens usam batch loading (`.in_()`) ao invés de query-por-item?
- [ ] Constantes importam de `constants.py` (sem cópias locais)?
- [ ] Endpoints AJAX recebem IDs explícitos?
- [ ] Logs usam `logger` com prefixo de contexto?
- [ ] Templates recebem todas as variáveis que referenciam?
- [ ] Não há imports circulares ou ausentes?
- [ ] O padrão visual é consistente com os módulos existentes?
- [ ] Nenhuma ação destrutiva foi executada sem autorização?

---

## 17. WORKFLOW COM O USUÁRIO

1. Usuário solicita alteração.
2. Ler e entender os arquivos envolvidos (OBRIGATÓRIO antes de qualquer edição).
3. Planejar a abordagem (seção 1 deste arquivo).
4. **Escrever testes** que cobrem o comportamento esperado (seção 2 — OBRIGATÓRIO).
5. Explicar brevemente o que será feito.
6. Implementar de forma incremental e focada.
7. Confirmar que os testes passam.
8. Verificar bugs e consistência (seção 4).
9. **Avaliar se a alteração gera pendência de produção** (seção 18).
10. Usuário testa localmente.
11. Deploy é decisão exclusiva do usuário.

---

## 18. RASTREAMENTO DE PENDÊNCIAS DE PRODUÇÃO

O arquivo `DEPLOY_PENDENTE.md` na raiz do projeto é o **registro oficial** de tudo que foi feito localmente e ainda precisa ser executado em produção.

### 18.1 Quando OBRIGATORIAMENTE adicionar ao DEPLOY_PENDENTE.md

| Tipo de alteração | Ação em produção necessária |
|---|---|
| Nova tabela no modelo | `CREATE TABLE` SQL |
| Nova coluna em modelo existente | `ALTER TABLE ADD COLUMN` SQL |
| Novo índice | `CREATE INDEX` SQL |
| Script de importação de dados | Executar script em produção |
| Nova dependência Python | `pip install xyz` + atualizar requirements |
| Nova dependência de sistema | Instalação no servidor |
| Novo arquivo de configuração | Copiar/criar em produção |
| Migration de dados | Executar script de migração |

### 18.2 O que NÃO vai no DEPLOY_PENDENTE.md

- Alterações apenas em templates HTML.
- Alterações em Python que não mudam estrutura de dados.
- Correções de lógica/bug sem impacto em banco ou infraestrutura.

---

## 19. CHECKLIST DE DEPLOY (Bitvise SSH)

### 19.1 Pré-deploy

- [ ] Backup do banco MySQL: `mysqldump sgc > backup_$(date +%Y%m%d).sql`
- [ ] Verificar `DEPLOY_PENDENTE.md` — executar todos os SQLs pendentes
- [ ] Build do frontend Vite: `npm run build` (se alterou componentes React)
- [ ] Verificar `.env` em produção (todas as variáveis críticas preenchidas)
- [ ] Testar conexões: SEI, SIAFE, DB antes de subir código

### 19.2 Deploy do código

```bash
# Via Bitvise SFTP: copiar arquivos alterados
# Ou via git pull (se repositório configurado):
git pull origin main

# Instalar novas dependências (se houver)
pip install -r requirements.txt

# Executar migrações Alembic (se houver)
flask db upgrade
```

### 19.3 Pós-deploy — checklist obrigatório

- [ ] Acessar `/hub` e confirmar que a aplicação carregou sem erros
- [ ] Verificar logs: `tail -f logs/sistema_pagamentos.log`
- [ ] Testar login com usuário real
- [ ] Testar cada módulo alterado (abrir pelo menos 1 página de cada)
- [ ] Verificar jobs do APScheduler: confirmar que o scheduler iniciou (`[SCHEDULER] iniciado` nos logs)
- [ ] Testar uma integração SEI (ex: buscar um processo) — confirmar token funciona
- [ ] Verificar notificações: confirmar que o sistema de notificações carregou
- [ ] Monitorar logs por 5 minutos após deploy — observar erros inesperados
- [ ] Confirmar que Gunicorn está rodando: `ps aux | grep gunicorn`
- [ ] Confirmar que sessões filesystem funcionam (login persistente)
- [ ] Se ALTER TABLE foi executado: verificar que dados existentes não foram corrompidos

### 19.4 Rollback (se necessário)

```bash
# Restaurar backup do banco
mysql sgc < backup_YYYYMMDD.sql

# Reverter código (git)
git checkout HEAD~1

# Reiniciar gunicorn
pkill -f gunicorn && gunicorn -w 4 -b 0.0.0.0:8000 --timeout 300 "app:create_app()"
```

### 19.5 Comando Gunicorn recomendado

```bash
gunicorn \
  -w 4 \
  -b 0.0.0.0:8000 \
  --timeout 300 \
  --access-logfile logs/access.log \
  --error-logfile logs/error.log \
  "app:create_app()"
```

---

## 20. PIPELINE SEMANAL DE MANUTENÇÃO

| Dia | Horário | Ação automática (APScheduler) | Ação manual (se necessário) |
|-----|---------|-------------------------------|-----------------------------|
| Seg–Sex | 08:00 | Verifica vigências de contratos expirando | — |
| Seg–Sex | 09:00 | Alerta NEs pendentes de inserção | Verificar dashboard financeiro |
| Dom | 03:00 | Limpa notificações expiradas (>30 dias) | — |
| Diário | Manual | `atualizar_empenho.py` via painel SIAFE | Acionar pelo hub quando necessário |
| Diário | Manual | `atualizar_reserva.py` | — |
| Mensal | Manual | `atualizar_loa.py --years 2026` | Início do mês |
| Semanal | Manual | `sync_cgfr_prioritarios.py` | Quando solicitado |
