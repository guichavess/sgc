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

### 2026-04-17 — Coluna `assinado` em `diarias_itinerario_documentos`

- [ ] **Adicionar coluna `assinado` em `diarias_itinerario_documentos`** — distingue documentos criados no SEI de documentos efetivamente assinados. Resolve dead state onde despacho CCDP era criado mas assinatura falhava e o sistema marcava como concluído.
  - SQL:
    ```sql
    ALTER TABLE diarias_itinerario_documentos
      ADD COLUMN assinado TINYINT(1) NOT NULL DEFAULT 0;
    -- Backfill: documentos que já possuem sei_id são considerados assinados
    -- (todos os existentes foram criados antes do controle de assinatura)
    UPDATE diarias_itinerario_documentos SET assinado = 1 WHERE sei_id IS NOT NULL;
    ```

### 2026-04-16 — Correção gramatical dos nomes das etapas de Diárias

- [ ] **Atualizar `diarias_etapas.nome`** com acentuação correta (linha anterior sem cedilha/acento). Apenas UPDATE, sem mudança de schema.
  - SQL:
    ```sql
    UPDATE diarias_etapas SET nome = 'Solicitação Inicial'                       WHERE nome = 'Solicitacao Inicial';
    UPDATE diarias_etapas SET nome = 'Análise da Solicitação — 1ª Parte'         WHERE nome = 'Analise da Solicitacao - 1a Parte';
    UPDATE diarias_etapas SET nome = 'Análise da Solicitação — 2ª Parte'         WHERE nome = 'Analise da Solicitacao - 2a Parte';
    UPDATE diarias_etapas SET nome = 'Concessão das Diárias'                     WHERE nome = 'Concessao das Diarias';
    UPDATE diarias_etapas SET nome = 'Prestação de Contas'                       WHERE nome = 'Prestacao de Contas';
    ```

### 2026-04-16 — Tabelas NL / PD / OB / NP por servidor

- [ ] **Criar tabelas `diarias_notas_liquidacao`, `diarias_programacoes_desembolso`, `diarias_ordens_bancarias`** — mesmo padrao de `diarias_notas_reserva` e `diarias_notas_empenho`. Cada servidor agora recebe sua propria NL/PD/OB.
  - Script:
    ```bash
    python scripts/criar_tabelas_nl_pd_ob.py
    ```
  - Alternativa SQL (aplicar os 3 CREATEs):
    ```sql
    CREATE TABLE diarias_notas_liquidacao (
      id BIGINT NOT NULL AUTO_INCREMENT,
      itinerario_id INT NOT NULL,
      item_itinerario_id INT NOT NULL,
      codigo VARCHAR(50) NOT NULL,
      sei_id VARCHAR(50) NULL, sei_formatado VARCHAR(50) NULL,
      valor NUMERIC(14,2) NULL,
      data_insercao DATETIME NOT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY uq_nl_itinerario_servidor (itinerario_id, item_itinerario_id),
      INDEX ix_nl_itinerario (itinerario_id),
      INDEX ix_nl_item (item_itinerario_id),
      FOREIGN KEY (itinerario_id) REFERENCES diarias_itinerario(id) ON DELETE CASCADE,
      FOREIGN KEY (item_itinerario_id) REFERENCES diarias_itens_itinerario(id) ON DELETE CASCADE
    );
    CREATE TABLE diarias_programacoes_desembolso LIKE diarias_notas_liquidacao;
    ALTER TABLE diarias_programacoes_desembolso DROP INDEX uq_nl_itinerario_servidor;
    ALTER TABLE diarias_programacoes_desembolso ADD CONSTRAINT uq_pd_itinerario_servidor UNIQUE (itinerario_id, item_itinerario_id);
    CREATE TABLE diarias_ordens_bancarias LIKE diarias_notas_liquidacao;
    ALTER TABLE diarias_ordens_bancarias DROP INDEX uq_nl_itinerario_servidor;
    ALTER TABLE diarias_ordens_bancarias ADD CONSTRAINT uq_ob_itinerario_servidor UNIQUE (itinerario_id, item_itinerario_id);
    CREATE TABLE diarias_notas_patrimoniais LIKE diarias_notas_liquidacao;
    ALTER TABLE diarias_notas_patrimoniais DROP INDEX uq_nl_itinerario_servidor;
    ALTER TABLE diarias_notas_patrimoniais ADD CONSTRAINT uq_np_itinerario_servidor UNIQUE (itinerario_id, item_itinerario_id);
    ```

### 2026-04-16 — Rename `despacho_apoio` → `despacho_sga` (correcao de nomenclatura)

- [ ] **Renomear registros orfaos `despacho_apoio` para `despacho_sga`** em `diarias_itinerario_documentos`. Processos criados via rota financeiro salvavam o Despacho SGA→NCI (IdSerie 2987) com o label errado `despacho_apoio` — a timeline esperava `despacho_sga` (nome canonico em SERIE_TIPO_DOCUMENTO_MAP e DIARIAS_SUBITENS), entao o subitem nao era marcado como concluido. O codigo ja foi corrigido; falta migrar os registros historicos em producao.
  - **SOMENTE** registros `despacho_apoio` que **NAO** tenham `despacho_sga` no mesmo itinerario. Os registros com ambos sao processos importados onde `despacho_apoio` e legitimamente o despacho pos-NCI (serie 754).
  - SQL:
    ```sql
    UPDATE diarias_itinerario_documentos d
    SET tipo_documento = 'despacho_sga'
    WHERE tipo_documento = 'despacho_apoio'
    AND NOT EXISTS (
        SELECT 1 FROM (
            SELECT itinerario_id FROM diarias_itinerario_documentos
            WHERE tipo_documento = 'despacho_sga'
        ) s
        WHERE s.itinerario_id = d.itinerario_id
    );
    ```

### 2026-04-15 — Tabela `diarias_notas_empenho` (1 NE por servidor)

- [ ] **Criar tabela `diarias_notas_empenho`** — mesma estrutura de `diarias_notas_reserva`. Substitui o uso de `diarias_itinerario_documentos` para NEs (que só permitia 1 por solicitação). Agora cada servidor tem sua NE.
  - Script:
    ```bash
    python scripts/criar_tabela_notas_empenho.py
    ```
  - Alternativa SQL:
    ```sql
    CREATE TABLE diarias_notas_empenho (
      id BIGINT NOT NULL AUTO_INCREMENT,
      itinerario_id INT NOT NULL,
      item_itinerario_id INT NOT NULL,
      codigo VARCHAR(50) NOT NULL,
      sei_id VARCHAR(50) NULL,
      sei_formatado VARCHAR(50) NULL,
      valor NUMERIC(14,2) NULL,
      data_insercao DATETIME NOT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY uq_ne_itinerario_servidor (itinerario_id, item_itinerario_id),
      INDEX ix_ne_itinerario (itinerario_id),
      INDEX ix_ne_item (item_itinerario_id),
      FOREIGN KEY (itinerario_id) REFERENCES diarias_itinerario(id) ON DELETE CASCADE,
      FOREIGN KEY (item_itinerario_id) REFERENCES diarias_itens_itinerario(id) ON DELETE CASCADE
    );
    ```

### 2026-04-15 — Coluna `data_criacao` em `diarias_itinerario_documentos`

- [ ] **Adicionar coluna `data_criacao` em `diarias_itinerario_documentos`** — usada pela timeline como fonte de data para processos criados localmente (que ainda não foram sincronizados via scan do SEI em `diarias_movimentacao`).
  - SQL:
    ```sql
    ALTER TABLE diarias_itinerario_documentos
      ADD COLUMN data_criacao DATETIME NULL;
    -- Backfill: atribui "agora" aos documentos já existentes para que a timeline
    -- não quebre. Processos importados continuam usando data de diarias_movimentacao;
    -- a coluna só é consultada quando a movimentação não traz a data.
    UPDATE diarias_itinerario_documentos SET data_criacao = NOW() WHERE data_criacao IS NULL;
    ```

### 2026-04-14 — Tabela `diarias_notas_reserva` (1 NR por servidor)

- [ ] **Criar tabela `diarias_notas_reserva`** — substitui o uso de `diarias_itinerario_documentos` para NRs (que só permitia 1 por solicitação). Agora cada servidor tem sua NR.
  - Script:
    ```bash
    python scripts/criar_tabela_notas_reserva.py
    ```
  - Alternativa SQL:
    ```sql
    CREATE TABLE diarias_notas_reserva (
      id BIGINT NOT NULL AUTO_INCREMENT,
      itinerario_id INT NOT NULL,
      item_itinerario_id INT NOT NULL,
      codigo VARCHAR(50) NOT NULL,
      sei_id VARCHAR(50) NULL,
      sei_formatado VARCHAR(50) NULL,
      valor NUMERIC(14,2) NULL,
      data_insercao DATETIME NOT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY uq_nr_itinerario_servidor (itinerario_id, item_itinerario_id),
      INDEX ix_nr_itinerario (itinerario_id),
      INDEX ix_nr_item (item_itinerario_id),
      FOREIGN KEY (itinerario_id) REFERENCES diarias_itinerario(id) ON DELETE CASCADE,
      FOREIGN KEY (item_itinerario_id) REFERENCES diarias_itens_itinerario(id) ON DELETE CASCADE
    );
    ```

### 2026-04-14 — Superintendência derivada da Sigla SEI

- [ ] **Adicionar colunas `unidade_sei_id`, `unidade_sei_sigla`, `superintendencia_sigla` em `sis_usuarios` + backfill**
  - Contexto: a superintendência do usuário agora é derivada automaticamente da Sigla da Unidade SEI (ex: `SEAD-PI/GAB/SGACG/DFIN/GEO` → `SGACG`) preenchida a cada login. Elimina dependência do setor_id/CSV e do modal de escolha de setor.
  - Script (cria colunas + backfill de usuários com setor já definido):
    ```bash
    python scripts/migrar_superintendencia_sigla.py
    ```
  - Alternativa SQL (se precisar rodar manual):
    ```sql
    ALTER TABLE sis_usuarios
      ADD COLUMN unidade_sei_id VARCHAR(50) NULL,
      ADD COLUMN unidade_sei_sigla VARCHAR(255) NULL,
      ADD COLUMN superintendencia_sigla VARCHAR(50) NULL,
      ADD INDEX idx_usuario_unidade_sei_id (unidade_sei_id),
      ADD INDEX idx_usuario_unidade_sei_sigla (unidade_sei_sigla),
      ADD INDEX idx_usuario_super_sigla (superintendencia_sigla);
    -- Depois rodar:
    -- python scripts/migrar_superintendencia_sigla.py --apenas-backfill
    ```
  - Usuários sem backfill serão preenchidos automaticamente no próximo login.

### 2026-04-14 — Estrutura hierárquica de Setores/Superintendências (SEAD)

- [ ] **Criar tabelas `tipos_entidade` e `setores` + coluna `setor_id` em `sis_usuarios`**
  - Contexto: nova estrutura hierárquica para vincular usuários a seus setores com cascata Superintendência → Setor. Base importada de CSVs (data/tipo_entidade.csv, data/entidades.csv) com 143 entidades e 18 tipos.
  - Script (cria tabelas + adiciona coluna FK + importa dados):
    ```bash
    python scripts/criar_tabelas_setores.py
    ```
  - Alternativa SQL (manual, caso o script não possa rodar):
    ```sql
    CREATE TABLE tipos_entidade (
      codtipoentidade INT PRIMARY KEY,
      nome VARCHAR(100) NOT NULL,
      nivel INT NOT NULL DEFAULT 0,
      nome2 VARCHAR(100)
    );
    CREATE TABLE setores (
      id INT PRIMARY KEY,
      nome VARCHAR(255) NOT NULL,
      sigla VARCHAR(100),
      parent_id INT,
      superintendencia_id INT,
      tipo_entidade_id INT NOT NULL,
      orgao_id INT DEFAULT 1,
      ativo TINYINT(1) NOT NULL DEFAULT 1,
      INDEX idx_setor_nome (nome),
      INDEX idx_setor_sigla (sigla),
      INDEX idx_setor_parent (parent_id),
      INDEX idx_setor_super (superintendencia_id),
      INDEX idx_setor_tipo (tipo_entidade_id),
      INDEX idx_setor_ativo (ativo),
      FOREIGN KEY (parent_id) REFERENCES setores(id) ON DELETE SET NULL,
      FOREIGN KEY (superintendencia_id) REFERENCES setores(id) ON DELETE SET NULL,
      FOREIGN KEY (tipo_entidade_id) REFERENCES tipos_entidade(codtipoentidade)
    );
    ALTER TABLE sis_usuarios
      ADD COLUMN setor_id INT NULL,
      ADD CONSTRAINT fk_usuario_setor FOREIGN KEY (setor_id) REFERENCES setores(id),
      ADD INDEX idx_usuario_setor (setor_id);
    -- Depois: rodar apenas a parte de importação
    -- python scripts/criar_tabelas_setores.py --apenas-importar
    ```

### Pós-deploy de 2026-04-07 (opcional / quando tiver fonte dos dados)

- [ ] **Popular `diarias_servidores.idpessoa` e promover a NOT NULL + UNIQUE**
  - Contexto: coluna `idpessoa INT NULL` já adicionada em produção em 2026-04-07. Falta popular e promover.
  ```sql
  -- Após popular idpessoa (script/manual conforme fonte de dados):
  ALTER TABLE diarias_servidores MODIFY idpessoa INT NOT NULL;
  ALTER TABLE diarias_servidores ADD UNIQUE KEY uq_servidores_idpessoa (idpessoa);
  ```

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
