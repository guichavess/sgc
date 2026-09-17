/* =====================================================================
   SGCFluxo — fluxos guiados em passos (Nova solicitação, Vincular processo)
   Vanilla, sem dependências. Marcação: macros passo / busca_contrato / resumo
   de templates/components/sgc_ui.html · estilos: css/components/sgc-ui.css
   Movimento: classes .is-entrando animadas no CSS com os tokens de motion.css
   (prefers-reduced-motion desliga no próprio CSS).

   API (window.SGCFluxo):
     escapeHtml(valor)
     setPasso(li, estado, resumoHtml?)   pendente | andamento | sucesso | erro;
                                         sucesso + resumoHtml recolhe o passo numa linha
     fluxo(opções)                       orquestra passos, resumo lateral, "Falta" e envio
     buscaContratos(raiz, opções)        busca + cartões de contrato selecionáveis
     cartaoContrato(c) · resumoContrato(c)
     resumoPares([{ rotulo, valor, mono?, largo?, detalhe? }])   ficha do passo recolhido
     separaDocumento(nomeContratado)     "05340639000130 - NOME" → { nome, documento: 'CNPJ 05.340.639/0001-30' }
     dropdown(select, opções)            select nativo realçado (listbox, busca opcional)
     mascaraCompetencia(input) · competenciaValida(texto)
     mascaraMoeda(input) · formatarMoeda(texto)
     erroCampo(campo, mensagem) · limparErro(campo)
     alerta(el, { focar?, focoAoFechar? })   alerta da página (macro alerta): traz para a vista e liga o fechar
   ===================================================================== */
window.SGCFluxo = (function () {
  'use strict';

  var ROTULOS = { pendente: 'Aguardando', andamento: 'Em andamento', sucesso: 'Concluído', erro: 'Com erro' };
  var FOCAVEIS = 'input:not([type="hidden"]):not([disabled]), select:not([disabled]):not([tabindex="-1"]), textarea:not([disabled]), button:not([disabled])';
  var RADIO = 'input[name="selecao_contrato_visual"]';
  var SEP = '<span class="sgc-sep" aria-hidden="true">·</span>';

  function escapeHtml(valor) {
    if (valor === null || valor === undefined) return '';
    return String(valor).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  /* reinicia a animação de entrada definida no CSS */
  function surgir(el) {
    el.classList.remove('is-entrando');
    void el.offsetWidth;
    el.classList.add('is-entrando');
  }

  /* ─── Erros inline: <div id="{campo.id}-erro" class="sgc-campo-erro" hidden> ─── */
  function erroCampo(campo, mensagem) {
    var el = document.getElementById(campo.id + '-erro');
    campo.setAttribute('aria-invalid', 'true');
    if (el) {
      el.textContent = mensagem;
      el.hidden = false;
    }
  }

  function limparErro(campo) {
    var el = document.getElementById(campo.id + '-erro');
    campo.removeAttribute('aria-invalid');
    if (el) {
      el.textContent = '';
      el.hidden = true;
    }
  }

  /* ─── Passo ─── */
  function setPasso(li, estado, resumoHtml) {
    var corpo = li.querySelector('.sgc-passo-corpo');
    var resumo = li.querySelector('.sgc-passo-resumo');
    var selo = li.querySelector('.sync-fase-estado');
    var recolher = estado === 'sucesso' && resumoHtml !== undefined;

    li.setAttribute('data-estado', estado);
    if (selo) selo.textContent = ROTULOS[estado] || estado;

    if (estado === 'pendente') {
      corpo.setAttribute('inert', '');
      corpo.setAttribute('aria-disabled', 'true');
    } else {
      corpo.removeAttribute('inert');
      corpo.removeAttribute('aria-disabled');
    }

    if (resumo) {
      resumo.querySelector('.sgc-passo-resumo-texto').innerHTML = recolher ? resumoHtml : '';
      if (resumo.hidden === recolher) {
        resumo.hidden = !recolher;
        if (recolher) surgir(resumo);
      }
    }
    if (corpo.hidden !== recolher) {
      corpo.hidden = recolher;
      if (!recolher) surgir(corpo);
    }
  }

  /* ─── Fluxo ───
     opções = {
       form, botao, resumo (aside.sgc-resumo),
       textoPronto ('Tudo pronto.'), textoEnviando ('Enviando…'),
       passos: [{
         el: li.sgc-passo,
         campos: [{ chave (data-resumo), rotulo (linha "Falta"), valor() → texto ou '' , mono?,
                    html() → HTML já escapado do <dd> (opcional; só chamado com valor) }],
         resumo() → HTML já escapado da linha recolhida,
         auto: conclui sozinho quando todos os campos ficam válidos (padrão true)
       }]
     }
     Um passo conta como pronto quando não está pendente/erro e todos os campos têm valor. */
  function fluxo(o) {
    var passos = o.passos;
    var painel = o.resumo;
    var falta = painel && painel.querySelector('[data-resumo-falta]');
    var botao = o.botao;
    var rotuloBotao = botao && botao.querySelector('.sgc-mbtn__label');
    var textoBotao = rotuloBotao ? rotuloBotao.textContent : '';
    var enviando = false;

    function estado(i) { return passos[i].el.getAttribute('data-estado'); }
    function valido(i) { return passos[i].campos.every(function (c) { return !!c.valor(); }); }

    function juntar(lista) {
      if (lista.length < 2) return lista.join('');
      return lista.slice(0, -1).join(', ') + ' e ' + lista[lista.length - 1];
    }

    function atualizar() {
      var faltando = [];
      passos.forEach(function (p, i) {
        var aberto = estado(i) !== 'pendente' && estado(i) !== 'erro';
        p.campos.forEach(function (c) {
          var valor = aberto ? c.valor() : '';
          var dd = painel && painel.querySelector('[data-resumo="' + c.chave + '"]');
          if (dd) {
            if (valor && c.html) dd.innerHTML = c.html();
            else dd.textContent = valor || '—';
            dd.classList.toggle('is-vazio', !valor);
            dd.classList.toggle('sol-mono', !!(valor && c.mono));
          }
          if (!valor) faltando.push(c.rotulo);
        });
      });
      var pronto = faltando.length === 0;
      if (botao && !enviando) botao.disabled = !pronto;
      if (falta) {
        falta.textContent = pronto ? (o.textoPronto || 'Tudo pronto.') : 'Falta: ' + juntar(faltando);
        falta.classList.toggle('is-pronto', pronto);
      }
      return pronto;
    }

    function focar(i) {
      var corpo = passos[i].el.querySelector('.sgc-passo-corpo');
      var alvo = corpo.querySelector(RADIO + ':checked') || corpo.querySelector(FOCAVEIS);
      if (alvo) alvo.focus();
    }

    function concluir(i) {
      if (!valido(i)) {
        atualizar();
        return false;
      }
      setPasso(passos[i].el, 'sucesso', passos[i].resumo ? passos[i].resumo() : undefined);
      var proximo = -1;
      for (var j = i + 1; j < passos.length; j++) {
        if (estado(j) !== 'sucesso') { proximo = j; break; }
      }
      if (proximo < 0) {
        atualizar();
        if (botao && !botao.disabled) botao.focus();
        return true;
      }
      setPasso(passos[proximo].el, 'andamento');
      // dados mantidos depois de um "Trocar": o passo seguinte já válido fecha em cascata
      if (passos[proximo].auto !== false && valido(proximo)) return concluir(proximo);
      atualizar();
      focar(proximo);
      return true;
    }

    /* "Trocar": reabre o passo e devolve os seguintes a pendente (saem do resumo) */
    function abrir(i) {
      setPasso(passos[i].el, 'andamento');
      for (var j = i + 1; j < passos.length; j++) setPasso(passos[j].el, 'pendente');
      atualizar();
      focar(i);
    }

    passos.forEach(function (p, i) {
      var trocar = p.el.querySelector('.sgc-passo-trocar');
      if (trocar) trocar.addEventListener('click', function () { abrir(i); });
      p.el.addEventListener('input', atualizar);
      p.el.addEventListener('change', atualizar);
      if (p.auto !== false) {
        var tentar = function () { if (estado(i) === 'andamento' && valido(i)) concluir(i); };
        p.el.addEventListener('input', tentar);
        p.el.addEventListener('change', tentar);
      }
    });

    function liberarBotao() {
      enviando = false;
      if (rotuloBotao) rotuloBotao.textContent = textoBotao;
      if (botao) botao.removeAttribute('aria-busy');
      atualizar();
    }

    if (o.form) {
      o.form.addEventListener('submit', function (e) {
        if (enviando || !atualizar()) {
          e.preventDefault();
          return;
        }
        enviando = true;
        if (botao) {
          botao.disabled = true;
          botao.setAttribute('aria-busy', 'true');
        }
        if (rotuloBotao) rotuloBotao.textContent = o.textoEnviando || 'Enviando…';
      });
      // voltar pelo histórico (bfcache) não pode deixar o botão preso em "Criando…"
      window.addEventListener('pageshow', function (e) { if (e.persisted) liberarBotao(); });
    }

    atualizar();
    return { atualizar: atualizar, concluir: concluir, abrir: abrir, valido: valido };
  }

  /* ─── Contratos ─── */
  function cartaoContrato(c) {
    var codigo = escapeHtml(c.codigo);
    var idRadio = 'radio_' + codigo;
    return '<label class="sgc-contrato" for="' + idRadio + '">' +
      '<input class="sgc-contrato-radio" type="radio" name="selecao_contrato_visual" id="' + idRadio + '" value="' + codigo + '">' +
      '<span class="sgc-contrato-info">' +
        '<span class="sgc-contrato-linha"><span class="sol-mono">' + codigo + '</span>' +
          (c.numeroOriginal ? SEP + '<span class="sgc-contrato-numero">' + escapeHtml(c.numeroOriginal) + '</span>' : '') +
        '</span>' +
        '<span class="sgc-contrato-nome">' + (escapeHtml(c.nomeContratado) || 'Contratado não informado') + '</span>' +
        (c.objeto ? '<span class="sgc-contrato-objeto">' + escapeHtml(c.objeto) + '</span>' : '') +
      '</span>' +
      (c.numProcesso ? '<span class="sgc-contrato-proc">Proc. <span class="sol-mono">' + escapeHtml(c.numProcesso) + '</span></span>' : '') +
    '</label>';
  }

  /* Ficha do passo concluído: pares com rótulo em cima e dado embaixo, lado a lado;
     largo ocupa a linha inteira (nome longo quebra em vez de ser cortado) e detalhe vai numa linha
     discreta sob o dado. Pares sem valor saem. */
  function resumoPares(pares) {
    return pares.filter(function (p) { return p.valor; }).map(function (p) {
      return '<span class="sgc-par' + (p.largo ? ' sgc-par--largo' : '') + '">' +
        '<span class="sgc-par-rotulo">' + escapeHtml(p.rotulo) + '</span>' +
        '<span class="sgc-par-valor' + (p.mono ? ' sol-mono' : '') + '">' + escapeHtml(p.valor) + '</span>' +
        (p.detalhe ? '<span class="sgc-par-detalhe">' + escapeHtml(p.detalhe) + '</span>' : '') +
      '</span>';
    }).join('');
  }

  /* nomeContratado vem como "CNPJ/CPF - NOME": separa o documento (formatado) do nome.
     Sem documento reconhecível, devolve o texto inteiro como nome. */
  function separaDocumento(texto) {
    var m = /^\s*([\d.\/-]{11,18})\s*-\s*(.+)$/.exec(texto || '');
    var digitos = m ? m[1].replace(/\D/g, '') : '';
    if (digitos.length === 14) {
      return { nome: m[2].trim(), documento: 'CNPJ ' + digitos.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, '$1.$2.$3/$4-$5') };
    }
    if (digitos.length === 11) {
      return { nome: m[2].trim(), documento: 'CPF ' + digitos.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4') };
    }
    return { nome: String(texto || '').trim(), documento: '' };
  }

  function resumoContrato(c) {
    if (!c) return '';
    var contratado = separaDocumento(c.nomeContratado);
    return resumoPares([
      { rotulo: 'Código', valor: c.codigo, mono: true },
      { rotulo: 'Nº', valor: c.numeroOriginal },
      { rotulo: 'Processo', valor: c.numProcesso, mono: true },
      { rotulo: 'Contratado', valor: contratado.nome || 'Não informado', detalhe: contratado.documento, largo: true }
    ]);
  }

  /* opções = { aoSelecionar(contrato|null), aoConfirmar(contrato), limite (20) }
     Setas navegam entre os cartões (seleciona sem avançar); clique, Enter ou Espaço confirmam. */
  function buscaContratos(raiz, o) {
    o = o || {};
    var url = raiz.getAttribute('data-url-busca');
    var termo = raiz.querySelector('[data-busca-termo]');
    var botao = raiz.querySelector('[data-busca-botao]');
    var rotulo = botao.querySelector('.sgc-mbtn__label');
    var textoBotao = rotulo.textContent;
    var resultados = raiz.querySelector('[data-busca-resultados]');
    var contador = raiz.querySelector('[data-busca-contador]');
    var lista = raiz.querySelector('[data-busca-lista]');
    var vazio = raiz.querySelector('[data-busca-vazio]');
    var vazioTermo = raiz.querySelector('[data-busca-vazio-termo]');
    var limite = o.limite || 20;
    var porCodigo = {};
    var sequencia = 0;
    var ponteiro = false;

    function carregando(sim) {
      botao.disabled = sim;
      rotulo.textContent = sim ? 'Buscando…' : textoBotao;
      lista.setAttribute('aria-busy', sim ? 'true' : 'false');
    }

    function marcar(radio) {
      lista.querySelectorAll('.sgc-contrato').forEach(function (cartao) {
        cartao.classList.toggle('is-selecionado', !!radio && radio.checked && cartao.contains(radio));
      });
      if (o.aoSelecionar) o.aoSelecionar(radio && radio.checked ? porCodigo[radio.value] : null);
    }

    function confirmar(radio) {
      radio.checked = true;
      marcar(radio);
      if (o.aoConfirmar) o.aoConfirmar(porCodigo[radio.value]);
    }

    function render(q, contratos) {
      var n = contratos.length;
      porCodigo = {};
      contratos.forEach(function (c) { porCodigo[c.codigo] = c; });
      lista.innerHTML = contratos.map(cartaoContrato).join('');
      lista.hidden = n === 0;
      vazio.hidden = n > 0;
      vazioTermo.textContent = q;
      contador.hidden = n === 0;
      contador.textContent = n === 1 ? '1 contrato encontrado'
        : n + ' contratos encontrados' + (n >= limite ? ' · refine a busca para ver outros' : '');
      resultados.hidden = false;
      surgir(resultados);
      if (o.aoSelecionar) o.aoSelecionar(null);
    }

    function buscar() {
      var q = termo.value.trim();
      if (q.length < 3) {
        erroCampo(termo, 'Digite pelo menos 3 caracteres.');
        termo.focus();
        return;
      }
      limparErro(termo);
      var minha = ++sequencia;
      carregando(true);
      fetch(url + (url.indexOf('?') >= 0 ? '&' : '?') + 'q=' + encodeURIComponent(q), {
        credentials: 'same-origin',
        headers: { Accept: 'application/json' }
      })
        .then(function (r) {
          if (!r.ok) throw new Error('status ' + r.status);
          return r.json();
        })
        .then(function (dados) {
          if (minha === sequencia) render(q, (dados && dados.results) || []);
        })
        .catch(function () {
          if (minha === sequencia) erroCampo(termo, 'Não foi possível buscar os contratos agora. Tente de novo.');
        })
        .then(function () {
          if (minha === sequencia) carregando(false);
        });
    }

    botao.addEventListener('click', buscar);
    termo.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        e.preventDefault(); // Enter busca; nunca envia o formulário
        buscar();
      }
    });
    termo.addEventListener('input', function () {
      if (termo.value.trim().length >= 3) limparErro(termo);
    });

    lista.addEventListener('pointerdown', function () { ponteiro = true; });
    lista.addEventListener('keydown', function (e) {
      ponteiro = false;
      if (!e.target.matches(RADIO)) return;
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        confirmar(e.target);
      }
    });
    lista.addEventListener('click', function (e) {
      if (!e.target.matches(RADIO)) return;
      // o clique sintético das setas não confirma: só ponteiro (mouse/toque)
      if (ponteiro) {
        ponteiro = false;
        confirmar(e.target);
      }
    });
    lista.addEventListener('change', function (e) {
      if (e.target.matches(RADIO)) marcar(e.target);
    });

    return { buscar: buscar };
  }

  /* ─── Dropdown ───
     Realça um <select> nativo, que continua no form com id/name e segue sendo a fonte do valor:
     gatilho no visual sgc-input + painel com listbox (padrão combobox da WAI-ARIA).
     opções = { busca (false), placeholder, placeholderBusca ('Buscar…'), vazio ('Nada encontrado') }
     Teclado: ↓/↑/Enter/Espaço abrem · ↓/↑ (Home/End sem busca) navegam · Enter escolhe · Esc fecha. */
  var sequenciaDropdown = 0;

  function semAcento(texto) {
    return String(texto || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
  }

  function dropdown(select, o) {
    o = o || {};
    var id = select.id || 'sgc-dropdown-' + (++sequenciaDropdown);
    var rotulo = select.id && document.querySelector('label[for="' + select.id + '"]');
    var raiz = document.createElement('div');
    var opcoes = Array.prototype.filter.call(select.options, function (op) { return op.value !== ''; });
    var opcaoVazia = select.querySelector('option[value=""]');
    var placeholder = o.placeholder || (opcaoVazia ? opcaoVazia.text : 'Selecione…');
    var textoBusca = o.placeholderBusca || 'Buscar…';

    if (rotulo && !rotulo.id) rotulo.id = id + '-rotulo';
    raiz.className = 'sgc-dropdown';
    raiz.innerHTML =
      '<button type="button" class="sgc-input sgc-dropdown-gatilho" id="' + id + '-gatilho" aria-haspopup="listbox" aria-expanded="false" aria-controls="' + id + '-lista"' +
        (rotulo ? ' aria-labelledby="' + rotulo.id + ' ' + id + '-valor"' : '') + '>' +
        '<span class="sgc-dropdown-valor" id="' + id + '-valor"></span>' +
        '<i class="bi bi-chevron-down sgc-dropdown-seta" aria-hidden="true"></i>' +
      '</button>' +
      '<div class="sgc-dropdown-painel" hidden>' +
        (o.busca
          ? '<div class="sgc-dropdown-busca"><i class="bi bi-search" aria-hidden="true"></i>' +
            '<input type="text" class="sgc-dropdown-busca-input" role="combobox" aria-expanded="true" aria-autocomplete="list" aria-controls="' + id + '-lista" autocomplete="off" placeholder="' + escapeHtml(textoBusca) + '" aria-label="' + escapeHtml(textoBusca) + '"></div>'
          : '') +
        '<ul class="sgc-dropdown-lista" id="' + id + '-lista" role="listbox" tabindex="-1"' + (rotulo ? ' aria-labelledby="' + rotulo.id + '"' : '') + '>' +
          opcoes.map(function (op, i) {
            return '<li class="sgc-dropdown-opcao" role="option" id="' + id + '-opcao-' + i + '" data-valor="' + escapeHtml(op.value) + '" aria-selected="false">' +
              '<span>' + escapeHtml(op.text.trim()) + '</span><i class="bi bi-check2" aria-hidden="true"></i></li>';
          }).join('') +
        '</ul>' +
        '<p class="sgc-dropdown-vazio" hidden>' + escapeHtml(o.vazio || 'Nada encontrado') + '</p>' +
      '</div>';

    select.parentNode.insertBefore(raiz, select);
    raiz.appendChild(select);
    select.classList.add('sgc-dropdown-nativo');
    select.setAttribute('tabindex', '-1');
    select.setAttribute('aria-hidden', 'true');

    var gatilho = raiz.querySelector('.sgc-dropdown-gatilho');
    var valor = raiz.querySelector('.sgc-dropdown-valor');
    var painel = raiz.querySelector('.sgc-dropdown-painel');
    var busca = raiz.querySelector('.sgc-dropdown-busca-input');
    var lista = raiz.querySelector('.sgc-dropdown-lista');
    var vazio = raiz.querySelector('.sgc-dropdown-vazio');
    var itens = Array.prototype.slice.call(lista.children);
    var foco = busca || lista;
    var ativo = null;

    if (rotulo) rotulo.htmlFor = gatilho.id;
    if (select.getAttribute('aria-describedby')) gatilho.setAttribute('aria-describedby', select.getAttribute('aria-describedby'));
    gatilho.disabled = select.disabled || opcoes.length === 0;

    function sincronizar() {
      var escolhido = select.value ? select.options[select.selectedIndex] : null;
      valor.textContent = escolhido ? escolhido.text.trim() : placeholder;
      gatilho.classList.toggle('is-placeholder', !escolhido);
      itens.forEach(function (li) {
        li.setAttribute('aria-selected', String(!!escolhido && li.getAttribute('data-valor') === select.value));
      });
    }

    function visiveis() { return itens.filter(function (li) { return !li.hidden; }); }

    function ativar(li) {
      if (ativo) ativo.classList.remove('is-ativo');
      ativo = li || null;
      if (ativo) {
        ativo.classList.add('is-ativo');
        foco.setAttribute('aria-activedescendant', ativo.id);
        // rola só a lista (scrollIntoView arrastaria a página junto)
        var topo = ativo.offsetTop - lista.offsetTop;
        var base = topo + ativo.offsetHeight;
        if (topo < lista.scrollTop) lista.scrollTop = topo;
        else if (base > lista.scrollTop + lista.clientHeight) lista.scrollTop = base - lista.clientHeight;
      } else {
        foco.removeAttribute('aria-activedescendant');
      }
    }

    function filtrar() {
      var q = busca ? semAcento(busca.value.trim()) : '';
      itens.forEach(function (li) { li.hidden = !!q && semAcento(li.textContent).indexOf(q) < 0; });
      var mostrados = visiveis();
      vazio.hidden = mostrados.length > 0;
      var selecionado = mostrados.filter(function (li) { return li.getAttribute('aria-selected') === 'true'; })[0];
      ativar(q ? mostrados[0] : (selecionado || mostrados[0]));
    }

    function aberto() { return !painel.hidden; }

    function foraDoCampo(e) {
      if (!raiz.contains(e.target)) fechar(false);
    }

    function abrir() {
      if (aberto() || gatilho.disabled) return;
      painel.hidden = false;
      raiz.classList.remove('is-acima');
      var caixa = gatilho.getBoundingClientRect();
      var abaixo = window.innerHeight - caixa.bottom;
      if (abaixo < painel.offsetHeight + 8 && caixa.top > abaixo) raiz.classList.add('is-acima');
      raiz.classList.add('is-aberto');
      gatilho.setAttribute('aria-expanded', 'true');
      surgir(painel);
      if (busca) busca.value = '';
      filtrar();
      foco.focus();
      document.addEventListener('pointerdown', foraDoCampo, true);
    }

    function fechar(devolverFoco) {
      if (!aberto()) return;
      painel.hidden = true;
      raiz.classList.remove('is-aberto');
      gatilho.setAttribute('aria-expanded', 'false');
      ativar(null);
      document.removeEventListener('pointerdown', foraDoCampo, true);
      if (devolverFoco) gatilho.focus();
    }

    function escolher(li) {
      var mudou = select.value !== li.getAttribute('data-valor');
      select.value = li.getAttribute('data-valor');
      sincronizar();
      fechar(true);
      // depois de devolver o foco: o fluxo pode concluir o passo e levar o foco adiante
      if (mudou) select.dispatchEvent(new Event('change', { bubbles: true }));
    }

    function mover(passo) {
      var mostrados = visiveis();
      if (!mostrados.length) return;
      var i = mostrados.indexOf(ativo);
      ativar(mostrados[Math.min(mostrados.length - 1, Math.max(0, i < 0 ? 0 : i + passo))]);
    }

    gatilho.addEventListener('click', function () {
      if (aberto()) fechar(true);
      else abrir();
    });
    gatilho.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        abrir();
      }
    });

    foco.addEventListener('keydown', function (e) {
      switch (e.key) {
        case 'ArrowDown': e.preventDefault(); mover(1); break;
        case 'ArrowUp': e.preventDefault(); mover(-1); break;
        case 'Home':
        case 'End':
          if (busca) return; // com busca, Home/End movem o cursor do texto
          e.preventDefault();
          ativar(e.key === 'Home' ? visiveis()[0] : visiveis().slice(-1)[0]);
          break;
        case 'Enter':
          e.preventDefault(); // nunca envia o formulário
          if (ativo) escolher(ativo);
          break;
        case ' ':
          if (busca) return;
          e.preventDefault();
          if (ativo) escolher(ativo);
          break;
        case 'Escape':
          e.preventDefault();
          e.stopPropagation();
          fechar(true);
          break;
        case 'Tab':
          fechar(false);
          break;
      }
    });

    if (busca) {
      busca.addEventListener('input', function (e) {
        e.stopPropagation(); // digitar na busca não é preencher o passo
        filtrar();
      });
    }
    lista.addEventListener('pointermove', function (e) {
      var li = e.target.closest('.sgc-dropdown-opcao');
      if (li && li !== ativo) ativar(li);
    });
    lista.addEventListener('click', function (e) {
      var li = e.target.closest('.sgc-dropdown-opcao');
      if (li) escolher(li);
    });
    select.addEventListener('change', sincronizar);
    window.addEventListener('pageshow', function (e) { if (e.persisted) sincronizar(); });

    sincronizar();
    return { abrir: abrir, fechar: fechar, sincronizar: sincronizar };
  }

  /* ─── Máscaras ─── */
  function competenciaValida(texto) {
    return /^(0[1-9]|1[0-2])\/\d{4}$/.test(texto || '');
  }

  /* MM/AAAA: só dígitos, barra automática, mês 01–12 com erro inline */
  function mascaraCompetencia(input) {
    function validar(final) {
      var v = input.value;
      var mes = parseInt(v.slice(0, 2), 10);
      if (!v) return limparErro(input);
      if (v.length >= 2 && (mes < 1 || mes > 12)) return erroCampo(input, 'Mês inválido. Use de 01 a 12.');
      if (final && !competenciaValida(v)) return erroCampo(input, 'Use o formato MM/AAAA.');
      limparErro(input);
    }

    input.addEventListener('input', function () {
      var d = input.value.replace(/\D/g, '').slice(0, 6);
      var v = d.length > 2 ? d.slice(0, 2) + '/' + d.slice(2) : d;
      if (input.value !== v) input.value = v;
      validar(v.length === 7);
    });
    input.addEventListener('blur', function () { validar(true); });
  }

  /* 18432055 → 184.320,55 (digita da direita para a esquerda) */
  function formatarMoeda(texto) {
    var d = String(texto || '').replace(/\D/g, '').replace(/^0+(?=\d)/, '');
    if (!d) return '';
    while (d.length < 3) d = '0' + d;
    return d.slice(0, -2).replace(/\B(?=(\d{3})+(?!\d))/g, '.') + ',' + d.slice(-2);
  }

  /* ─── Alerta da página ───
     Erro/aviso vem de um envio que recarregou a tela: rola até o alerta e foca nele (role=alert
     é lido pelo leitor de tela), sem depender de onde a página parou. O fechar remove o alerta. */
  function alerta(el, o) {
    if (!el) return;
    o = o || {};
    var fechar = el.querySelector('.sgc-alerta-fechar');
    if (fechar) {
      fechar.addEventListener('click', function () {
        el.parentNode.removeChild(el);
        if (o.focoAoFechar) o.focoAoFechar.focus();
      });
    }
    if (o.focar) {
      // scroll-margin-top (CSS) desconta a barra fixa do topo; só rola se o alerta não estiver à vista
      var caixa = el.getBoundingClientRect();
      var margem = parseFloat(window.getComputedStyle(el).scrollMarginTop) || 0;
      if (caixa.top < margem || caixa.bottom > window.innerHeight) {
        var reduzir = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        el.scrollIntoView({ block: 'start', behavior: reduzir ? 'auto' : 'smooth' });
      }
      el.focus({ preventScroll: true });
    }
  }

  function mascaraMoeda(input) {
    input.addEventListener('input', function () { input.value = formatarMoeda(input.value); });
    if (input.value) input.value = formatarMoeda(input.value);
  }

  return {
    escapeHtml: escapeHtml,
    setPasso: setPasso,
    fluxo: fluxo,
    buscaContratos: buscaContratos,
    cartaoContrato: cartaoContrato,
    resumoContrato: resumoContrato,
    separaDocumento: separaDocumento,
    resumoPares: resumoPares,
    dropdown: dropdown,
    mascaraCompetencia: mascaraCompetencia,
    competenciaValida: competenciaValida,
    mascaraMoeda: mascaraMoeda,
    formatarMoeda: formatarMoeda,
    erroCampo: erroCampo,
    limparErro: limparErro,
    alerta: alerta
  };
})();
