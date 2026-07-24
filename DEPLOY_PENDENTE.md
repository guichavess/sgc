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

### Índices para acelerar a Fase 3 (Saldos) — Pagamentos (2026-07-24)

- [ ] **Criar índices em `empenho` e `liquidacao`**
  - Contexto: a Fase 3 do "Sincronizar Tudo" (e o cálculo de saldo em geral)
    agrega `empenho`/`liquidacao` filtrando por `codigoUG`, `statusDocumento`,
    `dataEmissao` e `codContrato`. Sem índice composto essas colunas, cada
    agregação é um full scan → lentidão. O código já foi otimizado para calcular
    1x por `(contrato, ano)` (dedup por competência), mas o índice reduz o custo
    de cada agregação restante. Somente performance — não altera resultado.
  - SQL (executar no Workbench). **Atenção — os tipos diferem entre as tabelas**
    (schema real de produção, verificado em 2026-07-24):
    - `empenho`: `codigoUG` e `statusDocumento` são `TEXT` → ambos exigem prefixo.
    - `liquidacao`: `codigoUG` é `VARCHAR(10)` (entra sem prefixo); só
      `statusDocumento` é `TEXT` (exige prefixo).
    - Prefixo em coluna numérica/data ou maior que a coluna → erro 1089;
      coluna `TEXT` sem prefixo → erro 1170.
  ```sql
  CREATE INDEX idx_empenho_saldo    ON empenho    (codContrato, codigoUG(20), statusDocumento(30), dataEmissao);
  CREATE INDEX idx_liquidacao_saldo ON liquidacao (codContrato, codigoUG,     statusDocumento(30), dataEmissao);
  ```

### Competência em Empenhos e Liquidações — Financeiro (2026-06-08)

- [ ] **Rodar backfill de `competencia` em `empenho` e `liquidacao`** (colunas e `ob`/`pd` já OK)
  - Status verificado em produção (2026-07-24): colunas já existem nas 3 tabelas
    (`empenho`/`liquidacao` = `VARCHAR(7)`, `ob` = `TEXT` — igual ao model `app/models/ob.py`,
    não é divergência). `ob` (4.398/4.398) e `pd` (6.891/6.891) já estão 100% preenchidos.
    **Faltam popular**: `empenho` (2.355/7.053 = 33%) e `liquidacao` (4.435/262.597 = 1,7%).
  - Rodar em produção:
  ```bash
  python scripts/atualizar_empenho.py
  python scripts/atualizar_liquidacao.py
  python scripts/backfill_competencia_empenho.py --executar
  ```
  - Validação pós-backfill:
  ```sql
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM empenho;
  SELECT COUNT(*) AS total, COUNT(competencia) AS com_competencia FROM liquidacao;
  ```

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


### Dependências Python — Relatório Fotográfico PDF (2026-07-22)

- [ ] **Instalar fpdf2 e Pillow no venv de produção**
  - Contexto: o módulo Identidade Visual agora gera o Relatório Fotográfico como
    PDF para download direto (rota `/identidade-visual/relatorio-fotografico/pdf`).
    Bibliotecas puras Python, sem dependência de sistema.
  - Comando (executar no servidor após o git pull):
  ```bash
  cd /home/sead/sgc_novo && source .venv/bin/activate && pip install fpdf2>=2.8.0 Pillow>=10.0.0
  ```
  - Observação: nenhuma alteração de banco necessária.

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
