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

### Fix duplicação massiva — `reserva` e `liquidacao` (2026-05-26)

- [ ] **Limpar duplicatas e aplicar PK composta `(codigoUG, codigo)`**

  **Contexto**: as tabelas `reserva` e `liquidacao` foram criadas pelo `pandas.to_sql` sem PRIMARY KEY, sem UNIQUE e sem índices. O script de sincronização vinha empilhando duplicatas a cada execução (até 21× a mesma reserva). Em 2025, a soma de `reserva` estava em R$ 5,6 bi quando o BI marcava R$ 265 M — divergência de 21×. Após o fix, o dashboard bate exatamente com o BI.

  **Pré-requisito — backup obrigatório**:
  ```bash
  mysqldump --single-transaction --quick sgc reserva liquidacao \
    > backup_reserva_liquidacao_$(date +%Y%m%d).sql
  ```

  **Passo 1 — Limpeza de NULLs em `liquidacao`** (linhas-fantasma, todas as colunas significativas vazias):
  ```sql
  DELETE FROM liquidacao WHERE codigo IS NULL OR codigo = '';
  -- Esperado em local: 269 linhas removidas
  ```

  **Passo 2 — Dedup com tiebreaker** (precisamos de uma coluna `auto_increment` temporária porque a coluna `id` da API tem valores duplicados):
  ```sql
  -- reserva
  ALTER TABLE reserva
    ADD INDEX idx_tmp_dedup (codigoUG(10), codigo(50)),
    ADD COLUMN _row_uid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

  DELETE r1 FROM reserva r1
    INNER JOIN reserva r2
      ON r1.codigoUG = r2.codigoUG
     AND r1.codigo   = r2.codigo
     AND r1._row_uid < r2._row_uid;
  -- Esperado em local: ~10.073 linhas removidas

  ALTER TABLE reserva
    DROP PRIMARY KEY,
    DROP COLUMN _row_uid,
    DROP INDEX idx_tmp_dedup;

  -- liquidacao (mesma estratégia)
  ALTER TABLE liquidacao
    ADD INDEX idx_tmp_dedup (codigoUG(10), codigo(50)),
    ADD COLUMN _row_uid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

  DELETE l1 FROM liquidacao l1
    INNER JOIN liquidacao l2
      ON l1.codigoUG = l2.codigoUG
     AND l1.codigo   = l2.codigo
     AND l1._row_uid < l2._row_uid;
  -- Esperado em local: ~55.192 linhas removidas

  ALTER TABLE liquidacao
    DROP PRIMARY KEY,
    DROP COLUMN _row_uid,
    DROP INDEX idx_tmp_dedup;
  ```

  **Passo 3 — Aplicar PK composta + índices**:
  ```sql
  ALTER TABLE reserva
    MODIFY COLUMN codigoUG VARCHAR(10) NOT NULL,
    MODIFY COLUMN codigo   VARCHAR(50) NOT NULL,
    ADD PRIMARY KEY (codigoUG, codigo),
    ADD INDEX idx_reserva_data_emissao (dataEmissao),
    ADD INDEX idx_reserva_cod_contrato (codContrato(50));

  ALTER TABLE liquidacao
    MODIFY COLUMN codigoUG VARCHAR(10) NOT NULL,
    MODIFY COLUMN codigo   VARCHAR(50) NOT NULL,
    ADD PRIMARY KEY (codigoUG, codigo);
  ```

  **Passo 4 — Deploy do código novo** (scripts refatorados para `INSERT ... ON DUPLICATE KEY UPDATE`):
  - `scripts/atualizar_reserva.py`
  - `scripts/atualizar_liquidacao.py`
  - `app/models/reserva.py` (PK composta no model)
  - `app/models/__init__.py` (export do `Reserva`)

  **Passo 5 — Validação pós-deploy**:
  ```sql
  -- Conferir que não há mais duplicatas
  SELECT COUNT(*) AS total, COUNT(DISTINCT codigoUG, codigo) AS distintos FROM reserva;
  -- total deve ser igual a distintos

  -- Conferir valor de Reserva 2025 contra BI SIAFE
  SELECT COALESCE(SUM(CASE WHEN tipoAlteracao='ANULACAO' THEN -valor ELSE valor END),0) AS reservado
    FROM reserva
   WHERE statusDocumento='CONTABILIZADO'
     AND dataEmissao >= '2025-01-01' AND dataEmissao < '2026-01-01';
  -- Esperado: R$ 265.068.321,87 (alinhado com BI)
  ```

  **Rollback** (se algo der errado entre os passos):
  ```bash
  mysql sgc < backup_reserva_liquidacao_YYYYMMDD.sql
  ```

  **Observações**:
  - Os 5 testes que falham na suite full (`test_bug01_valor_cargo`, `test_bug03_atomicidade`, `test_hierarquia_autorizacao`, `test_vincular_processo`) são pré-existentes (test pollution) e NÃO foram introduzidos por esta correção — confirmado rodando a suite no `main` antes do patch.
  - Os scripts novos são **idempotentes**: podem ser rodados N vezes sem produzir duplicatas.
  - As tabelas `empenho`, `pd`, `ob` foram auditadas e estão **limpas** — sem ação necessária nelas.

---

### Credenciais Trino Data Lake — CGFR (2026-05-12)

- [ ] **Atualizar credenciais do Trino no `.env` de produção**
  - Contexto: o módulo CGFR passou a usar as novas credenciais do Data Lake localmente. Em produção, o `.env` sobrescreve os defaults do código.
  ```bash
  TRINO_USER=admin
  TRINO_PASSWORD=LOC35q3dgZn
  ```
  - Observação: reiniciar a aplicação após alterar o `.env`.

---

### Correção da Timeline — Diárias (2026-05-07)

- [ ] **Corrigir ordem das etapas na timeline de diárias**
  - Contexto: As etapas 2 (Escolha do Voo) e 3 (Análise 1ª Parte) tiveram seus valores de `ordem` trocados. Etapa 6 (Análise 2ª Parte) também estava faltando. Banco em produção foi copiado para desenvolvimento com dados desconfigurados.
  - **CRÍTICO**: Executar estes SQLs em PRODUÇÃO para restaurar a timeline correta:
  ```sql
  -- Verificar estado ANTES:
  SELECT id, nome, alias, ordem FROM diarias_etapas ORDER BY id;

  -- Corrigir ordem das etapas:
  UPDATE diarias_etapas SET ordem = 2 WHERE id = 3;  -- Análise 1ª Parte
  UPDATE diarias_etapas SET ordem = 3 WHERE id = 2;  -- Escolha do Voo
  UPDATE diarias_etapas SET ordem = 5 WHERE id = 4;  -- Concessão (deslocar)
  UPDATE diarias_etapas SET ordem = 6 WHERE id = 5;  -- Prestação (deslocar)

  -- Criar Etapa 6 (se não existir):
  INSERT INTO diarias_etapas (id, nome, alias, ordem, cor_hex, icone)
  VALUES (6, 'Análise 2ª Parte', 'analise_2_parte', 4, '#17a2b8', 'fas fa-microscope')
  ON DUPLICATE KEY UPDATE
    nome='Análise 2ª Parte',
    alias='analise_2_parte',
    ordem=4,
    cor_hex='#17a2b8',
    icone='fas fa-microscope';

  -- Verificar estado DEPOIS:
  SELECT id, nome, alias, ordem FROM diarias_etapas ORDER BY ordem;
  ```
  - **Resultado esperado**: 6 etapas com ordem 1-6, sendo a ordem 2=ID3 (Análise), 3=ID2 (Escolha do Voo), 4=ID6 (Análise 2ª Parte).
  - Local: Já foi corrigido via script `scripts/corrigir_timeline_diarias.py`.

- [ ] **Verificar se processo 00002.004523/2026-52 foi avançado indevidamente em produção**
  - Contexto: bug no `verificar_autorizacao_diaria()` fazia o processo avançar de etapa 1 → 3 ao detectar assinaturas na Requisição de Diárias (532), quando deveria esperar o Autorizo do Secretário (574). Bug já corrigido no código.
  ```sql
  -- Verificar estado atual do processo:
  SELECT id, etapa_atual_id, sei_protocolo
  FROM diarias_itinerario
  WHERE sei_protocolo = '00002.004523/2026-52' OR n_processo = '00002.004523/2026-52';

  -- Se etapa_atual_id = 3 (avançou indevidamente), reverter:
  UPDATE diarias_itinerario
  SET etapa_atual_id = 1
  WHERE (sei_protocolo = '00002.004523/2026-52' OR n_processo = '00002.004523/2026-52')
    AND etapa_atual_id = 3;

  -- Remover histórico de transições indevidas:
  DELETE FROM diarias_historico_movimentacoes
  WHERE id_itinerario = (
    SELECT id FROM diarias_itinerario
    WHERE sei_protocolo = '00002.004523/2026-52' OR n_processo = '00002.004523/2026-52'
  )
  AND id_etapa_anterior = 1 AND id_etapa_nova = 3;
  ```
  - Local: Já corrigido via `scripts/reverter_etapa_processo.py`.

---

### Hierarquia de Autorização — Diárias (2026-05-05)

- [ ] **Definir `cargo_gestao = 'secretario_exercicio'` para Bruno Gomes**
  - Contexto: implementada hierarquia de 3 níveis para autorização de diárias. O Secretário em Exercício precisa do valor `'secretario_exercicio'` no campo `cargo_gestao`. O Secretário titular (Samuel) já deve ter `'secretario'`. O Superintendente (Pedro) já deve ter `'superintendente'`.
  ```sql
  -- Verificar situação atual dos cargos de gestão:
  SELECT id, login, nome, cargo_gestao FROM sis_usuarios WHERE cargo_gestao IS NOT NULL;

  -- Definir Secretário em Exercício (ajustar o id/login do Bruno conforme retorno acima):
  UPDATE sis_usuarios
  SET cargo_gestao = 'secretario_exercicio'
  WHERE nome LIKE '%BRUNO GOMES%' OR login LIKE '%bruno%';

  -- Confirmar resultado:
  SELECT id, login, nome, cargo_gestao FROM sis_usuarios WHERE cargo_gestao IS NOT NULL;
  ```
  - Observação: **sem esta atualização**, o Nível 2 (Bruno) nunca será detectado e o sistema escalará direto para Nível 3. Não há alteração de schema — `cargo_gestao` já é `VARCHAR(50)`.

---

### Campos de Assinatura e Negação — Diárias (2026-05-08)

- [ ] **Adicionar colunas de rastreamento de assinaturas e negação na tabela `diarias_itinerario`**
  - Contexto: corrigido bug onde `superintendente_assinou_data` gravava `datetime.now()` ao invés do timestamp real do SEI. Adicionados campos para nome do superintendente, rastreamento completo da assinatura do secretário, persistência da descrição da unidade SEI solicitante e flag de processos negados.
  - SQL:
  ```sql
  ALTER TABLE diarias_itinerario ADD COLUMN unidade_geradora_sigla VARCHAR(255) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN unidade_geradora_descricao VARCHAR(500) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN superintendente_assinou_nome VARCHAR(200) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN secretario_assinou TINYINT(1) NOT NULL DEFAULT 0;
  ALTER TABLE diarias_itinerario ADD COLUMN secretario_assinou_data DATETIME NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN secretario_assinou_nome VARCHAR(200) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado TINYINT(1) NOT NULL DEFAULT 0;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_data DATETIME NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_por_id BIGINT NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_por_nome VARCHAR(200) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_justificativa TEXT NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_doc_sei_id VARCHAR(50) NULL;
  ALTER TABLE diarias_itinerario ADD COLUMN processo_negado_doc_sei_formatado VARCHAR(50) NULL;
  CREATE INDEX idx_diarias_itinerario_processo_negado ON diarias_itinerario (processo_negado);
  ```
  - Observação: colunas nullable/com default — seguro para dados existentes. Rows antigos terão NULL/0. Solicitações antigas sem `unidade_geradora_descricao` não poderão ser negadas até a descrição ser preenchida.

---

### Pós-deploy de 2026-04-07 (opcional / quando tiver fonte dos dados)

- [ ] **Popular `diarias_servidores.idpessoa` e promover a NOT NULL + UNIQUE**
  - Contexto: coluna `idpessoa INT NULL` já adicionada em produção em 2026-04-07. Falta popular e promover.
  ```sql
  ALTER TABLE diarias_servidores MODIFY idpessoa INT NOT NULL;
  ALTER TABLE diarias_servidores ADD UNIQUE KEY uq_servidores_idpessoa (idpessoa);
  ```

---

### Migração: Passagens Desacopladas do Fluxo (2026-05-14)

- [ ] **Migrar itinerários da etapa 2 (Escolha do Voo) para etapa 6 (Análise 2ª Parte)**
  - Contexto: A cotação de passagens foi desacoplada do fluxo de etapas e agora funciona como fluxo paralelo independente. A etapa 2 não é mais usada no fluxo ativo.
  - **Opção 1** — Via script (recomendado):
  ```bash
  python scripts/migrar_etapa2_para_etapa6.py              # DRY-RUN primeiro
  python scripts/migrar_etapa2_para_etapa6.py --executar    # Aplicar
  ```
  - **Opção 2** — SQL direto (fallback):
  ```sql
  -- Verificar antes:
  SELECT id, sei_protocolo, etapa_atual_id FROM diarias_itinerario WHERE etapa_atual_id = 2;

  -- Migrar:
  UPDATE diarias_itinerario SET etapa_atual_id = 6 WHERE etapa_atual_id = 2;
  ```
  - Verificação: após executar, nenhum itinerário deve estar na etapa 2:
  ```sql
  SELECT COUNT(*) FROM diarias_itinerario WHERE etapa_atual_id = 2;  -- deve retornar 0
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
