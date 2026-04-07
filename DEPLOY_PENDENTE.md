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
