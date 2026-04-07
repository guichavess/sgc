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

### Banco de Dados — Migrações

- [ ] **Criar tabela `diarias_itinerario_quadro_orcamentario`**
  ```sql
  CREATE TABLE `diarias_itinerario_quadro_orcamentario` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `itinerario_id` INT NOT NULL,
    `ug` VARCHAR(20) NULL,
    `funcao` VARCHAR(10) NULL,
    `subfuncao` VARCHAR(10) NULL,
    `programa` VARCHAR(10) NULL,
    `plano_interno` VARCHAR(10) NULL,
    `fonte_recursos` VARCHAR(20) NULL,
    `natureza_despesa` VARCHAR(20) NULL,
    `valor_inicial_nr` DECIMAL(14,2) NULL,
    `saldo_nr` DECIMAL(14,2) NULL,
    `valor_despesa` DECIMAL(14,2) NULL,
    `saldo_atual_nr` DECIMAL(14,2) NULL,
    UNIQUE KEY `uq_quadro_itinerario_id` (`itinerario_id`),
    CONSTRAINT `fk_quadro_itinerario` FOREIGN KEY (`itinerario_id`)
        REFERENCES `diarias_itinerario` (`id`) ON DELETE CASCADE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  ```

- [ ] **Criar tabela `diarias_itinerario_documentos`** — normalização dos documentos SEI (substitui colunas `sei_*` antigas, que permanecem em produção como fallback)
  ```sql
  CREATE TABLE `diarias_itinerario_documentos` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `itinerario_id` INT NOT NULL,
    `tipo_documento` VARCHAR(50) NOT NULL,
    `sei_id` VARCHAR(50) NULL,
    `sei_formatado` VARCHAR(50) NULL,
    `codigo` VARCHAR(50) NULL,
    UNIQUE KEY `uq_doc_itin_tipo` (`itinerario_id`, `tipo_documento`),
    CONSTRAINT `fk_doc_itinerario` FOREIGN KEY (`itinerario_id`)
        REFERENCES `diarias_itinerario` (`id`) ON DELETE CASCADE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  ```

- [ ] **Migrar dados sei_* → `diarias_itinerario_documentos`**
  - Contexto: copia os valores das colunas antigas `sei_*` para a nova tabela normalizada. NÃO usar `--drop-columns` — manter colunas antigas como fallback.
  ```bash
  python scripts/migrar_diarias_sei.py              # dry-run (inspecionar)
  python scripts/migrar_diarias_sei.py --executar   # aplicar data copy
  ```

- [ ] **Restruturação de etapas Diárias (11 → 5)**
  - Contexto: substitui as 11 etapas antigas por 5 novas (SOLICITACAO_INICIAL, ESCOLHA_VOO, ANALISE_SOLICITACAO, CONCESSAO_DIARIAS, PRESTACAO_CONTAS) e remapeia `diarias_itinerario.etapa_atual_id`. **Rodar sempre dry-run primeiro.**
  ```bash
  python scripts/migrar_etapas_diarias_v2.py              # dry-run
  python scripts/migrar_etapas_diarias_v2.py --executar   # aplicar
  ```

- [ ] **Criar tabela `diarias_movimentacao`**
  - Script: `python scripts/criar_tabela_diarias_movimentacao.py`

- [ ] **Popular `diarias_movimentacao`** (sync SEI)
  ```bash
  python scripts/sync_diarias_movimentacao.py --workers 6
  ```

- [ ] **Adicionar coluna `idpessoa` na tabela `diarias_servidores`**
  - Contexto: vincula servidor ao cadastro centralizado de pessoas. Aplicar como NULL primeiro, popular, depois promover a NOT NULL + UNIQUE.
  ```sql
  -- Passo 1: adicionar como NULL (seguro em tabela populada)
  ALTER TABLE diarias_servidores ADD COLUMN idpessoa INT NULL AFTER id;

  -- Passo 2: popular idpessoa via script/manual conforme a fonte de dados

  -- Passo 3: após popular, promover a NOT NULL + UNIQUE
  ALTER TABLE diarias_servidores MODIFY idpessoa INT NOT NULL;
  ALTER TABLE diarias_servidores ADD UNIQUE KEY uq_servidores_idpessoa (idpessoa);
  ```

- [ ] **Fix estados duplicados** — remover registros com `cod_ibge` 1 e 2 e adicionar constraint UNIQUE
  ```sql
  DELETE FROM estados WHERE cod_ibge IN (1, 2);
  ALTER TABLE estados ADD UNIQUE KEY uq_estados_cod_ibge (cod_ibge);
  ```

- [ ] **Adicionar coluna `fonte` na tabela `diarias_cotacoes_voos`**
  - Contexto: rastreia se a cotação foi cadastrada manualmente ('manual') ou extraída via OCR do SEI ('ocr_sei')
  ```sql
  ALTER TABLE diarias_cotacoes_voos ADD COLUMN fonte VARCHAR(20) DEFAULT 'manual';
  ```

- [ ] **Adicionar colunas `escolha_via_sei` e `escolha_sei_opcoes` na tabela `diarias_itinerario`**
  - Contexto: `escolha_via_sei` indica quando a escolha veio do SEI; `escolha_sei_opcoes` armazena opcoes extraidas do PDF (ex: "1,2")
  ```sql
  ALTER TABLE diarias_itinerario ADD COLUMN escolha_via_sei TINYINT(1) DEFAULT 0;
  ALTER TABLE diarias_itinerario ADD COLUMN escolha_sei_opcoes VARCHAR(100) NULL;
  ```

### Dependências de Sistema

- [ ] **Instalar Tesseract OCR** no servidor de produção (necessário para leitura de PDFs de cotação)
  ```bash
  sudo apt-get install tesseract-ocr tesseract-ocr-por
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
