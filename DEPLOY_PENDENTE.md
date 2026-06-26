# Deploy Pendente — SGC Pagamentos

> Checklist cumulativo de ações a executar em **produção** antes ou durante o próximo deploy.
> **Regra:** cada item executado em produção deve ser removido deste arquivo.
> Adicionar itens aqui sempre que fizer alteração local que exija ação manual em produção.

---

## Como usar

1. Fiz algo local que precisa de ação em produção → **adiciono aqui**.
2. Fui fazer o deploy → **sigo este arquivo de cima para baixo**.
3. Executei o item em produção → **removo a linha**.
4. Arquivo vazio = produção está sincronizada com local.

---

## Pendências

### Tabela de log de sincronização — Pagamentos (2026-06-23)

- [ ] **Criar tabela `sis_sincronizacao_log`**
  - Contexto: o módulo de Pagamentos passou a registrar cada execução da rotina
    "Sincronizar Tudo (SEI + Etapas + Saldos)" — tanto a manual (botão) quanto a nova
    automática (job agendado 00:30/06:30/12:30/18:30). Essa tabela é a fonte única e
    confiável da data de "Atualização Geral" exibida no dashboard de solicitações
    (antes derivada do maior `SaldoEmpenho.data` apenas da página atual, por isso
    sumia). Sem a tabela, a aplicação quebra ao abrir o dashboard.
  - SQL (executar no Workbench):
  ```sql
  CREATE TABLE sis_sincronizacao_log (
    id                 INT AUTO_INCREMENT PRIMARY KEY,
    iniciado_em        DATETIME NOT NULL,
    finalizado_em      DATETIME NULL,
    status             VARCHAR(20) NOT NULL DEFAULT 'em_andamento',
    origem             VARCHAR(20) NOT NULL DEFAULT 'manual',
    docs_atualizados   INT DEFAULT 0,
    etapas_avancadas   INT DEFAULT 0,
    saldos_atualizados INT DEFAULT 0,
    erros              INT DEFAULT 0,
    usuario_id         INT NULL,
    INDEX idx_sync_finalizado (finalizado_em),
    INDEX idx_sync_usuario (usuario_id)
  );
  ```
  - Observação: o job agendado `sincronizar_pagamentos_sei` roda automaticamente após
    o deploy (4x ao dia). Nenhuma dependência Python nova (usa APScheduler já instalado).

### Competência em Empenhos, Liquidações e OBs — Financeiro (2026-06-08)

- [ ] **Adicionar coluna `competencia` em `empenho`, `liquidacao` e `ob`**
  - Contexto: a aba Financeiro do gerenciamento de contrato passou a exibir a coluna Competência nas 4 sub-abas (Empenhos, Liquidações, PDs, Pagamentos/OB). Em produção, apenas a tabela `pd` tinha a coluna `competencia` — porque a API SIAFE de PD devolve o campo direto na resposta. Os endpoints de empenho/liquidação/OB **não** retornam `competencia` direto — confirmado via teste de produção. Os scripts agora extraem a competência dos classificadores `codigoTipoClassificador` 81 (Ano) e 502 (Mês), formato `MM/YYYY` (mesma lógica que `atualizar_liquidacao.py` já usava). Para OB foi adicionada extração recursiva (classificadores podem estar aninhados).
  - SQL (executar no Workbench):
  ```sql
  ALTER TABLE empenho    ADD COLUMN competencia VARCHAR(7) NULL;
  ALTER TABLE liquidacao ADD COLUMN competencia VARCHAR(7) NULL;
  ALTER TABLE ob         ADD COLUMN competencia VARCHAR(7) NULL;
  ```
  - Observacao 2026-06-08: `codFonte` agora e normalizado antes do cast numerico nos scripts `atualizar_empenho.py`, `atualizar_liquidacao.py`, `atualizar_pd.py`, `atualizar_ob.py` e `atualizar_reserva.py`, evitando que fontes no formato SIAFE/classificador (`7.55`, `5.00`) sejam gravadas fora do padrao (`755`, `500`). Reexecutar os scripts listados abaixo corrige os registros recarregados por ano/UG; o backfill reverso preenche `empenho.competencia` restante via NL -> PD -> OB.
  - Após o ALTER, re-rodar os scripts em produção para popular o histórico:
  ```bash
  python scripts/atualizar_empenho.py
  python scripts/atualizar_liquidacao.py
  python scripts/atualizar_pd.py
  python scripts/atualizar_ob.py
  python scripts/atualizar_reserva.py
  python scripts/backfill_competencia_empenho.py --executar
  ```
  - Validação pós-backfill (deve retornar >0 em todas):
  ```sql
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM empenho;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM liquidacao;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM ob;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM pd;
  ```
  - Observação: até o backfill rodar, a coluna Competência das três sub-abas vai aparecer como `—` para registros antigos. PD não precisa de ALTER (coluna já existe), só do deploy do model atualizado.

### Módulo Identidade Visual — Fachadas (2026-06-25)

- [ ] **Criar tabela `identidade_visual_locais` e importar dados**
  - Contexto: módulo temporário para acompanhamento de fachadas dos Espaços/Salas da Cidadania.
  - SQL (executar no Workbench):
  ```sql
  CREATE TABLE identidade_visual_locais (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    cidade          VARCHAR(100) NOT NULL,
    tipo_local      VARCHAR(200) NOT NULL,
    endereco        VARCHAR(500),
    bairro          VARCHAR(200),
    cep             VARCHAR(10),
    custo           DECIMAL(12,2),
    data_acao       DATETIME NULL,
    arquivo_nome    VARCHAR(255),
    arquivo_caminho VARCHAR(500),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  ```
  - Script de importação (após criar a tabela):
  ```bash
  python scripts/importar_identidade_visual.py --executar
  ```
  - Observação: copiar `Acompanhamento Fachada.xlsx` para `~/Downloads/` no servidor antes de rodar, ou usar `--arquivo caminho/do/arquivo.xlsx`.
- [ ] **Criar tabela `identidade_visual_arquivos`** (múltiplos arquivos por local)
  ```sql
  CREATE TABLE identidade_visual_arquivos (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    local_id        INT NOT NULL,
    nome_original   VARCHAR(255) NOT NULL,
    nome_servidor   VARCHAR(500) NOT NULL,
    tipo            VARCHAR(10) NOT NULL,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_iv_arq_local (local_id),
    FOREIGN KEY (local_id) REFERENCES identidade_visual_locais(id) ON DELETE CASCADE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  ```
  - Observação: criar pasta de uploads no servidor: `mkdir -p app/static/uploads/identidade_visual`

- [ ] **Criar tabela `municipios_pi` e adicionar coluna `municipio_id` em `identidade_visual_locais`**
  - Contexto: campo Cidade agora é um select pesquisável vinculado à tabela de municípios do IBGE. Tipo Local validado contra lista fixa. Campos Endereço, Bairro e CEP agora são obrigatórios.
  - Script (executa tudo: cria tabela, adiciona coluna, importa 224 municípios, associa registros existentes):
  ```bash
  python scripts/importar_municipios_pi.py --executar
  ```
  - Observação: o CSV do IBGE já está em `/tmp/1ea51a9afb80a30312ac5186a4804b80.csv` no servidor.

- [ ] **Padronizar tipo_local para apenas "Espaço da Cidadania" e "Sala da Cidadania"**
  - Contexto: as variações "(Auto Mall)" e "(Pi Center Moda)" foram removidas da lista de tipos válidos.
  - SQL (executar no Workbench após o deploy):
  ```sql
  UPDATE identidade_visual_locais
  SET tipo_local = 'Espaço da Cidadania'
  WHERE tipo_local LIKE 'Espaço da Cidadania%'
    AND tipo_local != 'Espaço da Cidadania';
  ```

### Auditoria + Exclusão — Identidade Visual (2026-06-26)

- [ ] **Adicionar colunas de autoria em `identidade_visual_locais` e criar tabela de log `identidade_visual_log`**
  - Contexto: o módulo passou a (1) registrar o usuário que criou/atualizou cada local,
    (2) ordenar a listagem com os PENDENTES primeiro, e (3) permitir exclusão de registros
    **apenas** para usuários com acesso full ao módulo (permissão `identidade_visual.excluir`
    ou `is_admin`). Toda criação/edição/exclusão é gravada em `identidade_visual_log`.
    As colunas novas são `deferred` no model, então a listagem continua funcionando antes
    do ALTER; mas criar/editar/excluir só funcionam após rodar o SQL abaixo.
  - SQL (executar no Workbench):
  ```sql
  ALTER TABLE identidade_visual_locais
    ADD COLUMN criado_por_id     BIGINT NULL,
    ADD COLUMN atualizado_por_id BIGINT NULL;

  CREATE TABLE identidade_visual_log (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    local_id     INT NULL,
    acao         VARCHAR(20) NOT NULL,          -- CRIAR | EDITAR | EXCLUIR
    descricao    VARCHAR(500) NULL,
    usuario_id   BIGINT NULL,
    usuario_nome VARCHAR(255) NULL,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_iv_log_local (local_id),
    INDEX idx_iv_log_usuario (usuario_id),
    INDEX idx_iv_log_data (created_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  ```
  - Permissões (perfis): a ação `excluir` já existe na lista `ACOES`. Para liberar a exclusão
    a um perfil, marcar a permissão **Identidade Visual → Excluir** na tela de Perfis. Papéis
    sugeridos dentro do módulo:
    - **Visualizador**: `identidade_visual.visualizar` (vê e exporta).
    - **Operador**: `visualizar` + `editar` (cria locais, registra ações, anexa/remove arquivos).
    - **Gestor / Full**: `visualizar` + `editar` + `excluir` (tudo + exclusão). `is_admin` tem tudo.
  - Observação: nenhuma dependência Python nova.

### Hierarquia de Autorização — Diárias (2026-05-05)

- [ ] **Definir `cargo_gestao = 'secretario_exercicio'` para Bruno Gomes** ⚠️ BLOQUEADO
  - **Status (2026-05-29)**: Bruno ainda NÃO fez login no sistema, então não existe registro dele em `sis_usuarios`. O `UPDATE` abaixo não terá efeito até ele logar pela primeira vez. Confirmado em prod: só existem Pedro (id 3, `superintendente`) e Samuel (id 7, `secretario`). **Re-executar este item assim que o Bruno acessar o sistema.**
  - Contexto: implementada hierarquia de 3 níveis para autorização de diárias. O Secretário em Exercício precisa do valor `'secretario_exercicio'` no campo `cargo_gestao`. O Secretário titular (Samuel) já deve ter `'secretario'`. O Superintendente (Pedro) já deve ter `'superintendente'`.
  ```sql
  -- Verificar situação atual dos cargos de gestão (tabela não tem coluna `login`):
  SELECT id, nome, cargo_gestao FROM sis_usuarios WHERE cargo_gestao IS NOT NULL;

  -- Definir Secretário em Exercício (ajustar o id do Bruno conforme retorno acima):
  UPDATE sis_usuarios
  SET cargo_gestao = 'secretario_exercicio'
  WHERE nome LIKE '%BRUNO GOMES%';

  -- Confirmar resultado:
  SELECT id, nome, cargo_gestao FROM sis_usuarios WHERE cargo_gestao IS NOT NULL;
  ```
  - Observação: **sem esta atualização**, o Nível 2 (Bruno) nunca será detectado e o sistema escalará direto para Nível 3. Não há alteração de schema — `cargo_gestao` já é `VARCHAR(50)`.

---

## Formato para novos itens

Ao adicionar um novo item, use o formato abaixo:

```markdown
- [ ] **Descrição curta do que fazer**
  - Contexto: por que é necessário / o que foi feito localmente
  - Script: `python scripts/nome_do_script.py` *(se houver)*
  - SQL: *(se for alteração de banco)*
  - Observação: *(qualquer aviso importante)*
```
