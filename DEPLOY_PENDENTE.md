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

### Competência em Empenhos, Liquidações e OBs — Financeiro (2026-06-08)

- [ ] **Adicionar coluna `competencia` em `empenho`, `liquidacao` e `ob`**
  - Contexto: a aba Financeiro do gerenciamento de contrato passou a exibir a coluna Competência nas 4 sub-abas (Empenhos, Liquidações, PDs, Pagamentos/OB). Em produção, apenas a tabela `pd` tinha a coluna `competencia` — porque a API SIAFE de PD devolve o campo direto na resposta. Os endpoints de empenho/liquidação/OB **não** retornam `competencia` direto — confirmado via teste de produção. Os scripts agora extraem a competência dos classificadores `codigoTipoClassificador` 81 (Ano) e 502 (Mês), formato `MM/YYYY` (mesma lógica que `atualizar_liquidacao.py` já usava). Para OB foi adicionada extração recursiva (classificadores podem estar aninhados).
  - SQL (executar no Workbench):
  ```sql
  ALTER TABLE empenho    ADD COLUMN competencia VARCHAR(7) NULL;
  ALTER TABLE liquidacao ADD COLUMN competencia VARCHAR(7) NULL;
  ALTER TABLE ob         ADD COLUMN competencia VARCHAR(7) NULL;
  ```
  - Após o ALTER, re-rodar os scripts em produção para popular o histórico:
  ```bash
  python scripts/atualizar_empenho.py
  python scripts/atualizar_liquidacao.py
  python scripts/atualizar_ob.py
  ```
  - Validação pós-backfill (deve retornar >0 em todas):
  ```sql
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM empenho;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM liquidacao;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM ob;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM pd;
  ```
  - Observação: até o backfill rodar, a coluna Competência das três sub-abas vai aparecer como `—` para registros antigos. PD não precisa de ALTER (coluna já existe), só do deploy do model atualizado.

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
