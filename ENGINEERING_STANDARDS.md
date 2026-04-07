# Padrões de Engenharia — SGC (Sistema de Gestão de Contratos)

> Instruções obrigatórias para construção de módulos, features e correções.
> Este documento é cumulativo: cada revisão de módulo adiciona novas regras.
> **Use como prompt** ao iniciar qualquer tarefa de desenvolvimento neste projeto.

---

## 1. TRANSAÇÕES E BANCO DE DADOS

### 1.1 Atomicidade de operações dependentes
Quando duas ou mais operações devem acontecer juntas (tudo ou nada), use um único `commit()` no final. Nunca faça `commit()` dentro de uma função auxiliar que será chamada por outra que também faz `commit()`.

```python
# ERRADO: dois commits — se o segundo falhar, o primeiro já foi persistido
def registrar_historico(...):
    db.session.add(historico)
    db.session.commit()  # commit 1

def avancar_etapa(...):
    solicitacao.etapa_atual_id = nova_etapa_id
    registrar_historico(...)  # já commitou internamente
    db.session.commit()  # commit 2 — se falhar, histórico ficou órfão

# CERTO: parâmetro auto_commit permite controle do chamador
def registrar_historico(..., auto_commit=True):
    db.session.add(historico)
    if auto_commit:
        db.session.commit()

def avancar_etapa(...):
    solicitacao.etapa_atual_id = nova_etapa_id
    registrar_historico(..., auto_commit=False)
    db.session.commit()  # um único commit atômico
```

### 1.2 Queries determinísticas com `.first()`
Toda query que usa `.first()` para obter "o mais recente" DEVE ter `order_by()`. Sem ordenação, o banco retorna registro arbitrário.

```python
# ERRADO: qual empenho será retornado? Depende do humor do MySQL
sol = SolicitacaoEmpenho.query.filter_by(id_solicitacao=self.id).first()

# CERTO: sempre o mais recente
sol = SolicitacaoEmpenho.query.filter_by(
    id_solicitacao=self.id
).order_by(SolicitacaoEmpenho.data.desc()).first()
```

### 1.3 Índices em colunas de filtro
Toda coluna usada em `WHERE`, `JOIN`, `ORDER BY` ou `GROUP BY` com frequência deve ter `index=True` na definição do modelo.

```python
# Colunas que SEMPRE precisam de índice:
etapa_atual_id = db.Column(db.Integer, ..., index=True)      # filtro de status
data_solicitacao = db.Column(db.DateTime, ..., index=True)    # ORDER BY
protocolo_gerado_sei = db.Column(db.String(50), index=True)   # busca por protocolo
status_geral = db.Column(db.String(100), ..., index=True)     # filtro de estado
id_solicitacao = db.Column(db.BigInteger, ..., index=True)    # FK usada em JOINs
```

### 1.4 EXISTS ao invés de `.first()` para verificação de existência
Quando só precisa saber se algo existe (não precisa do objeto), use `EXISTS` subquery.

```python
# ERRADO: carrega objeto inteiro só para verificar se é None
return cls.model.query.filter_by(**filters).first() is not None

# CERTO: retorna boolean direto no banco
return db.session.query(
    cls.model.query.filter_by(**filters).exists()
).scalar()
```

---

## 2. PERFORMANCE DE QUERIES

### 2.1 Eliminar N+1 com batch loading
Quando uma rota carrega N registros e para cada um precisa de dados relacionados, NUNCA faça uma query por registro. Carregue tudo de uma vez com `.in_()`.

```python
# ERRADO: 500 processos = 500 queries de histórico
for sol in todos_processos:
    hist = HistoricoMovimentacao.query.filter_by(id_solicitacao=sol.id).all()

# CERTO: 1 query batch + agrupamento em memória
sol_ids = [s.id for s in todos_processos]
todos_historicos = HistoricoMovimentacao.query.filter(
    HistoricoMovimentacao.id_solicitacao.in_(sol_ids)
).order_by(
    HistoricoMovimentacao.id_solicitacao,
    HistoricoMovimentacao.data_movimentacao.asc()
).all()

from collections import defaultdict
hist_por_sol = defaultdict(list)
for h in todos_historicos:
    hist_por_sol[h.id_solicitacao].append(h)

# Uso: hist_por_sol.get(sol.id, []) — O(1) por acesso
```

### 2.2 Queries escopadas — nunca carregar tabela inteira
Filtrar apenas os registros relevantes ao contexto. Nunca fazer `.all()` sem filtro em tabelas grandes.

```python
# ERRADO: carrega TODOS os empenhos do sistema
todos_empenhos = db.session.query(SolicitacaoEmpenho).all()

# CERTO: carrega apenas os empenhos das solicitações da página
sol_ids = [s.id for s in solicitacoes]
empenhos = db.session.query(SolicitacaoEmpenho).filter(
    SolicitacaoEmpenho.id_solicitacao.in_(sol_ids)
).all()
```

### 2.3 Lazy computation — só computar o que será renderizado
Em páginas com abas/tabs, só processar os dados da aba ativa. Não gastar CPU/queries com dados invisíveis.

```python
# ERRADO: computa ambas as abas sempre
resumo = _gerar_visao_geral(...)
metricas = _gerar_metricas(...)  # pesada: N queries de histórico

# CERTO: só computa a aba que o usuário está vendo
if aba_ativa == 'geral':
    resumo = _gerar_visao_geral(...)
elif aba_ativa == 'metricas':
    metricas = _gerar_metricas(...)
```

### 2.4 Pre-load no dashboard para properties N+1
Quando o template acessa properties que fazem queries (ex: `solicitacao.saldo_atual_contrato`), pré-carregue os dados na rota e passe como dicts separados.

```python
# Na rota: pré-carrega em batch
saldos_preloaded = {}
for s in solicitacoes:
    saldos_preloaded[s.id] = saldos_map.get((s.codigo_contrato, s.competencia))

# No template: acessa o dict ao invés da property
{{ saldos_preloaded[s.id] }}  <!-- ao invés de {{ s.saldo_atual_contrato }} -->
```

---

## 3. CONSTANTES E FONTE ÚNICA DE VERDADE

### 3.1 Nunca duplicar mapas/constantes
Se um valor existe em `constants.py`, importe-o. Nunca crie cópias locais que podem divergir.

```python
# ERRADO: cópia local que vai ficar desatualizada
MAPA_ORDEM_LOCAL = {1: 1, 2: 2, 8: 3, ...}

# CERTO: importar a fonte única
from app.constants import MAPA_ORDEM_ETAPAS
MAPA_ORDEM_LOCAL = MAPA_ORDEM_ETAPAS
```

### 3.2 IDs mágicos — usar Enums
Referências a IDs de etapas, status, tipos devem usar as constantes de `constants.py`, nunca números literais.

```python
# ERRADO
if sol.etapa_atual_id == 6:

# CERTO
from app.constants import EtapaID
if sol.etapa_atual_id == EtapaID.PAGO:
```

---

## 4. ENDPOINTS AJAX E APIs

### 4.1 Identificação explícita — sempre passar IDs
Endpoints AJAX que operam sobre um registro devem receber o ID explicitamente. Nunca depender de "buscar o mais recente" para identificar qual registro.

```python
# ERRADO: se houver 2 pendentes, pega a errada
solicitacao = Solicitacao.query.filter_by(
    id_usuario_solicitante=current_user.id,
    status_geral='AGUARDANDO_ASSINATURA'
).order_by(Solicitacao.id.desc()).first()

# CERTO: aceita ID explícito, com fallback para retrocompatibilidade
solicitacao_id = dados.get('solicitacao_id')
if solicitacao_id:
    solicitacao = Solicitacao.query.filter_by(
        id=solicitacao_id,
        id_usuario_solicitante=current_user.id,
        status_geral='AGUARDANDO_ASSINATURA'
    ).first()
else:
    # fallback retrocompatível
    solicitacao = Solicitacao.query.filter_by(...).order_by(...).first()
```

### 4.2 Validação de entrada
Todo endpoint deve validar inputs antes de processar. Nunca confiar que o frontend enviou dados corretos.

```python
# Conversão segura de valor monetário brasileiro
if isinstance(valor, str):
    valor = valor.replace('.', '').replace(',', '.')
valor_float = float(valor)
```

---

## 5. LOGGING E OBSERVABILIDADE

### 5.1 Sempre `logger`, nunca `print()`
Em produção com gunicorn/uwsgi, `print()` pode ser perdido. Use sempre o logger do Flask.

```python
# ERRADO
print(f"Erro: {e}")

# CERTO
current_app.logger.error(f"Erro processamento: {e}")
app_obj.logger.warning(f"Aviso: {msg}")
```

### 5.2 Prefixos de contexto nos logs
Usar prefixos que identifiquem o módulo/operação para facilitar filtragem.

```python
current_app.logger.info(f"[EMPENHO] valor={valor_float}, contrato={codigo}")
current_app.logger.error(f"[SEI-SYNC] Erro protocolo {protocolo}: {e}")
```

---

## 6. THREAD SAFETY

### 6.1 Limpeza de sessão em threads
Ao usar `ThreadPoolExecutor` com Flask, cada thread DEVE limpar sua sessão no `finally` para evitar vazamento entre execuções na pool.

```python
def funcao_na_thread(app_obj, ...):
    with app_obj.app_context():
      try:
        # lógica de negócio
        db.session.commit()
        return resultado
      except Exception as e:
        db.session.rollback()
        return None
      finally:
        db.session.remove()  # OBRIGATÓRIO — limpa sessão da thread
```

### 6.2 Limitar workers de acordo com o banco
Não usar mais threads do que o `max_connections` do MySQL permite. Valores seguros: 5-10 para operações de escrita, 10-20 para leitura.

---

## 7. ARQUITETURA DE MÓDULOS

### 7.1 Estrutura padrão de um módulo

```
app/
  modulo/
    __init__.py          # Blueprint
    routes/
      __init__.py        # Registra sub-rotas
      dashboard.py       # Listagem com filtros + paginação
      crud.py            # Criar, visualizar, editar
      api.py             # Endpoints AJAX/JSON
      reports.py         # Relatórios (se aplicável)
  models/
    entidade.py          # Modelo SQLAlchemy
  services/
    entidade_service.py  # Lógica de negócio (sem dependência de Flask request)
  repositories/
    entidade_repository.py  # Queries (herda BaseRepository)
  templates/
    modulo/
      base_modulo.html   # Base com navbar do módulo
      navbar_modulo.html # Navbar específica
      dashboard.html     # Template principal
```

### 7.2 Separação de responsabilidades

| Camada | Responsabilidade | Pode acessar |
|--------|-----------------|--------------|
| Routes | HTTP, request/response, session, flash | Services, Repositories |
| Services | Lógica de negócio, validações, notificações | Repositories, Models |
| Repositories | Queries SQL, paginação, filtros | Models, db.session |
| Models | Definição de tabelas, relationships, properties simples | Nada (passivo) |

### 7.3 Properties em modelos — cuidado com queries
Properties que fazem queries (`self.query(...)`) são convenientes mas causam N+1. Regra:
- OK para acesso individual (página de detalhes)
- PROIBIDO em listagens/dashboards — usar pre-load na rota

---

## 8. FRONTEND (Templates Jinja2)

### 8.1 Filtros com overflow
Dropdowns de filtro dentro de cards com `overflow: hidden` ficam cortados. Solução:

```css
.card-filtros {
    overflow: visible;
    z-index: 10;
}
```

### 8.2 Pre-loaded data no template
Dados pré-carregados na rota devem ser acessados via dicts, não properties do modelo:

```html
<!-- ERRADO: dispara query N+1 -->
{{ solicitacao.saldo_atual_contrato }}

<!-- CERTO: acessa dict pré-carregado -->
{{ saldos_preloaded[solicitacao.id] }}
```

### 8.3 Variáveis nullable em templates com abas
Jinja2 renderiza TODO o HTML, inclusive abas ocultas por CSS/JS. Se uma variável pode ser `None` (ex: lazy loading por aba), proteger com `{% if %}` ou filtro `if`.

```html
<!-- ERRADO: se pagination é None na aba de métricas, explode -->
Total: {{ pagination.total }} processos
{% if pagination.pages > 1 %}

<!-- CERTO: proteger contra None -->
Total: {{ pagination.total if pagination else 0 }} processos
{% if pagination and pagination.pages > 1 %}
```

---

## 9. CHECKLIST PRÉ-COMMIT

Antes de commitar qualquer feature/correção, verificar:

- [ ] Toda `.first()` que precisa do "mais recente" tem `order_by()`?
- [ ] Operações dependentes compartilham um único `commit()`?
- [ ] Listagens usam batch loading (`.in_()`) ao invés de query-por-item?
- [ ] Constantes/mapas importam de `constants.py` (sem cópias locais)?
- [ ] Endpoints AJAX recebem IDs explícitos (não dependem de "mais recente")?
- [ ] Threads com `db.session` fazem `db.session.remove()` no `finally`?
- [ ] Colunas de filtro/JOIN/ORDER BY têm `index=True` no modelo?
- [ ] Logs usam `logger` com prefixo de contexto (não `print()`)?
- [ ] Páginas com abas/tabs só computam dados da aba ativa?
- [ ] Queries de auditoria/relatório filtram por IDs relevantes (não `.all()` global)?

---

## 10. HISTÓRICO DE REVISÕES

| Data | Módulo Revisado | Regras Adicionadas |
|------|----------------|-------------------|
| 2026-03-18 | Solicitação de Pagamentos | Todas as regras 1-9 (revisão inicial) |
| 2026-03-19 | Diárias | Aplicação das regras 1-9 existentes |

> Próximos módulos a revisar: Financeiro, Prestações de Contratos, Dashboards, Usuários
