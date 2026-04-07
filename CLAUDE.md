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
- Hipóteses podem exigir pesquisa adicional e múltiplas etapas para testar.

### 1.4 Adaptabilidade
- Após cada ação, avaliar: o resultado exige alteração no plano?
- Se hipóteses iniciais forem refutadas, gerar novas com base nas informações coletadas.
- Não repetir a mesma ação falhada — mudar estratégia ou argumentos.

### 1.5 Fontes de informação (usar todas)
- Ferramentas disponíveis e suas capacidades.
- Políticas, regras, checklists e restrições (este arquivo, ENGINEERING_STANDARDS.md).
- Observações anteriores e histórico da conversa.
- Memory do projeto (`.claude/projects/.../memory/`).
- Perguntar ao usuário quando a informação não está disponível de outra forma.

### 1.6 Precisão e fundamentação
- Verificar afirmações citando informações exatas.
- Não presumir — confirmar lendo o código real.

### 1.7 Completude
- Todos os requisitos, restrições e preferências devem ser incorporados.
- Não concluir prematuramente: pode haver múltiplas opções relevantes.
- Revisar fontes de informação para confirmar quais são relevantes ao estado atual.

### 1.8 Persistência inteligente
- Não desistir por tempo gasto ou frustração do usuário.
- Em erros transitórios, tentar novamente (até limite razoável).
- Em outros erros, mudar estratégia — nunca repetir a mesma chamada falhada.
- Uma vez que uma ação foi tomada, não é possível voltar atrás — planejar antes.

---

## 2. AMBIENTE DE DESENVOLVIMENTO

- **TODAS as alterações são feitas no ambiente LOCAL.**
- NUNCA executar comandos em produção (deploy, migrations, scripts de banco) sem autorização explícita.
- NUNCA fazer `git push` sem autorização explícita.
- NUNCA executar scripts que alterem banco de dados de produção.
- Quando o usuário pedir para "implementar" ou "fazer" algo, assume-se LOCAL.
- Deploy e produção são etapas separadas que o usuário controla manualmente.

---

## 3. DISCIPLINA DE BUGS E VERIFICAÇÃO

### 3.1 Antes de implementar
- SEMPRE ler os arquivos completos que serão modificados para entender o contexto.
- Verificar imports existentes — não duplicar, não quebrar.
- Verificar se a função/variável/classe que será usada realmente existe no código.
- Conferir nomes exatos de campos nos modelos (ex: `data` vs `data_solicitacao`).

### 3.2 Durante a implementação
- Após cada alteração, verificar mentalmente se introduziu:
  - Import circular ou ausente.
  - Variável referenciada antes de ser definida.
  - Tipo incompatível (IntEnum vs int, string vs number).
  - Query sem `order_by()` quando precisa do "mais recente".
  - `commit()` duplicado em operações que deveriam ser atômicas.
  - Template referenciando variável que a rota não passa.
- Se o código envolve condicionais, testar mentalmente todos os branches (happy path + edge cases).

### 3.3 Após implementar
- Revisar o diff completo antes de considerar finalizado.
- Verificar se a alteração não quebrou nada adjacente (outros endpoints usando as mesmas funções, templates que herdam do mesmo base).
- Se criou nova rota: verificar que o blueprint está registrado e o URL prefix está correto.
- Se alterou modelo: verificar se a tabela/coluna existe no banco (ou se precisa de migration).

---

## 4. PADRÕES DE ENGENHARIA (REFERÊNCIA: ENGINEERING_STANDARDS.md)

O arquivo `ENGINEERING_STANDARDS.md` na raiz do projeto contém regras detalhadas. Resumo das regras críticas:

### 4.1 Banco de dados
- Operações dependentes = um único `commit()` no final (atomicidade).
- Toda `.first()` para "mais recente" DEVE ter `order_by()`.
- Colunas de filtro/JOIN/ORDER BY devem ter `index=True`.
- Usar `EXISTS` para verificação de existência, não `.first() is not None`.

### 4.2 Performance
- Eliminar N+1: batch loading com `.in_()`, nunca query-por-item em listagens.
- Queries escopadas: nunca `.all()` sem filtro em tabelas grandes.
- Lazy computation: em páginas com abas, só processar dados da aba ativa.
- Pre-load no dashboard para properties que fazem queries.

### 4.3 Constantes
- Importar de `app/constants.py` — nunca duplicar mapas/constantes localmente.
- Usar Enums (`EtapaID`, `DiariasEtapaID`) — nunca IDs numéricos literais.

### 4.4 APIs/AJAX
- Endpoints devem receber IDs explícitos.
- Validar todos os inputs antes de processar.

### 4.5 Logging
- `current_app.logger`, nunca `print()`.
- Prefixos de contexto: `[MODULO] mensagem`.

### 4.6 Thread safety
- Threads com `db.session` devem fazer `db.session.remove()` no `finally`.

---

## 5. CONSISTÊNCIA E IDENTIDADE DO SISTEMA

### 5.1 Arquitetura de módulos (padrão obrigatório)
```
app/modulo/routes/{dashboard,crud,api,admin}.py   — Rotas (finas)
app/services/modulo_service.py                     — Lógica de negócio
app/models/entidade.py                             — Modelos SQLAlchemy
app/templates/modulo/{base_modulo,navbar_modulo,dashboard,detalhes}.html
app/static/css/components/modulo.css               — CSS modular
```

### 5.2 Separação de responsabilidades
| Camada | Faz | Acessa |
|--------|-----|--------|
| Routes | HTTP, request/response, flash | Services |
| Services | Lógica de negócio, validações | Models, db.session |
| Models | Definição de tabelas, relationships | Nada (passivo) |

### 5.3 Frontend — identidade visual
- Bootstrap 5.3 como base — não misturar com outros frameworks CSS.
- Cada módulo tem sua cor de navbar (definida no base template do módulo).
- Cards, tabelas e formulários devem seguir o mesmo padrão visual dos módulos existentes.
- Ao criar novo componente visual, verificar como foi feito nos módulos existentes e replicar o padrão.
- CSS modular em `app/static/css/components/` importado via `main.css`.

### 5.4 Nomenclatura
- Rotas: `modulo.acao` (ex: `diarias.administracao_detalhe`).
- Templates: `modulo/nome_template.html`.
- Models: PascalCase singular (ex: `DiariasItinerario`).
- Tabelas DB: snake_case plural com prefixo do módulo (ex: `diarias_itinerarios`).
- Services: `ModuloService` como classe estática ou funções no service.
- Constants: UPPER_SNAKE_CASE em `app/constants.py`.

---

## 6. CHECKLIST PRÉ-ENTREGA

Antes de considerar qualquer tarefa finalizada:

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

## 7. WORKFLOW COM O USUÁRIO

1. Usuário solicita alteração.
2. Ler e entender os arquivos envolvidos (OBRIGATÓRIO antes de qualquer edição).
3. Planejar a abordagem (seção 1 deste arquivo).
4. Explicar brevemente o que será feito.
5. Implementar de forma incremental e focada.
6. Verificar bugs e consistência (seção 3).
7. **Avaliar se a alteração gera pendência de produção** (seção 8).
8. Usuário testa localmente.
9. Deploy é decisão exclusiva do usuário.

---

## 8. RASTREAMENTO DE PENDÊNCIAS DE PRODUÇÃO

O arquivo `DEPLOY_PENDENTE.md` na raiz do projeto é o **registro oficial** de tudo que foi feito localmente e ainda precisa ser executado em produção.

### 8.1 Quando OBRIGATORIAMENTE adicionar ao DEPLOY_PENDENTE.md

Após cada implementação, verificar se a alteração se enquadra em algum dos casos abaixo. Se sim, propor imediatamente a adição do item ao arquivo:

| Tipo de alteração | Exemplo | Ação em produção necessária |
|---|---|---|
| Nova tabela no modelo | `class DiariasXyz(db.Model)` | `CREATE TABLE` SQL |
| Nova coluna em modelo existente | `coluna = db.Column(...)` | `ALTER TABLE ADD COLUMN` SQL |
| Novo índice | `index=True` em coluna nova | `CREATE INDEX` SQL |
| Script de importação de dados | `scripts/importar_*.py` | Executar script em produção |
| Nova dependência Python | `pip install xyz` | `pip install xyz` + atualizar requirements |
| Nova dependência de sistema | Tesseract, wkhtmltopdf, etc. | Instalação no servidor |
| Novo arquivo de configuração | `.env`, config, certificado | Copiar/criar em produção |
| Migration de dados | reorganização de tabelas | Executar script de migração |

### 8.2 Como propor a adição

Ao final de cada implementação que gere pendência, apresentar ao usuário:

> "Esta alteração precisa de ação em produção. Adicionar ao `DEPLOY_PENDENTE.md`?"

E já exibir o bloco formatado pronto para copiar/confirmar, no formato:

```markdown
- [ ] **Descrição curta**
  - Contexto: o que foi feito localmente
  - Script/SQL: `comando ou sql aqui`
```

### 8.3 O que NÃO vai no DEPLOY_PENDENTE.md

- Alterações apenas em templates HTML (sem schema de banco).
- Alterações em arquivos Python que não mudam estrutura de dados.
- Correções de lógica/bug sem impacto em banco ou infraestrutura.
- Qualquer coisa que seja só "copiar código" — o deploy de código em si não precisa ser listado.
