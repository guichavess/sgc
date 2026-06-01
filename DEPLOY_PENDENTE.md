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

### Módulo Gestão do Fundo Rotativo — Tabela de Saldos (2026-06-01)

> Local (`localhost/sgc`): **aplicado em 2026-06-01** (CREATE TABLE + coluna `natureza`). Restam apenas as ações em produção.

- [ ] **Criar tabela `fundo_rotativo_saldos` em produção**
  - Contexto: 1ª aba ("Saldo") do novo módulo "Gestão do Fundo Rotativo" (sub-seção do Financeiro). CRUD para registrar saldos por fonte, natureza e exercício; filtros por ano (derivado da data) e natureza na listagem.
  - SQL (aplicado em local — usar este exato em produção; já inclui a coluna `natureza`):
    ```sql
    CREATE TABLE fundo_rotativo_saldos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        valor DECIMAL(15, 2) NOT NULL,
        data DATETIME NOT NULL,
        fonte_codigo VARCHAR(10) NOT NULL,
        natureza VARCHAR(20) NULL,
        id_exercicio VARCHAR(2) NOT NULL,
        criado_por BIGINT UNSIGNED NULL,
        data_criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
        data_atualizacao DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        CONSTRAINT fk_fr_saldo_fonte FOREIGN KEY (fonte_codigo) REFERENCES class_fonte(codigo),
        CONSTRAINT fk_fr_saldo_user  FOREIGN KEY (criado_por)   REFERENCES sis_usuarios(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    CREATE INDEX idx_fr_saldo_data      ON fundo_rotativo_saldos (data);
    CREATE INDEX idx_fr_saldo_fonte     ON fundo_rotativo_saldos (fonte_codigo);
    CREATE INDEX idx_fr_saldo_natureza  ON fundo_rotativo_saldos (natureza);
    CREATE INDEX idx_fr_saldo_exercicio ON fundo_rotativo_saldos (id_exercicio);
    ```
  - Observação: `criado_por` é `BIGINT UNSIGNED` para combinar com `sis_usuarios.id` (que é `BIGINT UNSIGNED AUTO_INCREMENT`). Usar `INT` falha com erro 3780 "Referencing column ... are incompatible". A tabela `class_fonte` e a tabela `loa` (origem do dropdown de natureza) já devem estar populadas via SIAFE.

- [ ] **Conceder permissão `fundo_rotativo` aos perfis necessários em produção**
  - Contexto: novo módulo aparece na tela de Perfis em `/usuarios/perfis`. Marcar as ações `visualizar` e `criar` nos perfis que devem ter acesso (admins já têm acesso automaticamente via `is_admin`).

---

### Suporte à UG 210102 — Contratos (2026-05-29)

- [ ] **Adicionar coluna `codigoUG` na tabela `contratos`**
  - Contexto: `atualizar_contratos.py` agora busca contratos das UGs 210101 e 210102. A coluna `codigoUG` identifica qual UG cada contrato pertence e habilita o filtro de UG no módulo Execução de Contratos.
  - SQL:
    ```sql
    ALTER TABLE contratos ADD COLUMN codigoUG VARCHAR(10) NULL AFTER codigo;
    CREATE INDEX idx_contratos_codigoUG ON contratos (codigoUG);
    ```
  - Observação: O script `atualizar_contratos.py` também faz essa migração automaticamente via `ensure_tables()` — mas rodar o SQL manual garante o índice e evita depender da próxima atualização SIAFE.

---

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
