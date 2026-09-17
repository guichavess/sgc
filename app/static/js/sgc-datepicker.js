/**
 * SGC Date Picker — botão outline + calendário em popover, sem dependências.
 * Adaptado do "Calendar22" (shadcn/originui sobre react-aria-components) para JS puro.
 *
 * Markup (renderizado no servidor):
 *   <div class="sgc-datepicker" data-sgc-datepicker [data-min-input="campo"] [data-max-input="campo"]>
 *     <label class="sgc-datepicker__rotulo" for="ID">De</label>
 *     <div class="sgc-datepicker__campo">
 *       <input type="hidden" name="campo" value="AAAA-MM-DD">
 *       <button type="button" id="ID" class="sgc-datepicker__trigger" aria-haspopup="dialog" aria-expanded="false">
 *         <span class="sgc-datepicker__valor">DD/MM/AAAA</span> <i class="bi bi-chevron-down sgc-datepicker__seta"></i>
 *       </button>
 *     </div>
 *   </div>
 *
 * O valor enviado no formulário continua AAAA-MM-DD (mesmo contrato do <input type="date">).
 * data-min-input / data-max-input: nome de outro campo do formulário que limita as datas.
 */
(function () {
    'use strict';

    var LOCALE = 'pt-BR';
    var PLACEHOLDER = 'Selecionar data';
    var DIAS_SEMANA = [
        ['D', 'domingo'], ['S', 'segunda-feira'], ['T', 'terça-feira'], ['Q', 'quarta-feira'],
        ['Q', 'quinta-feira'], ['S', 'sexta-feira'], ['S', 'sábado']
    ];
    var fmtMes = new Intl.DateTimeFormat(LOCALE, { month: 'long', year: 'numeric' });
    var fmtDiaCompleto = new Intl.DateTimeFormat(LOCALE, { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });

    var aberto = null; // instância com o popover aberto

    function doisDigitos(n) { return (n < 10 ? '0' : '') + n; }
    function paraISO(d) { return d.getFullYear() + '-' + doisDigitos(d.getMonth() + 1) + '-' + doisDigitos(d.getDate()); }
    function formatar(d) { return doisDigitos(d.getDate()) + '/' + doisDigitos(d.getMonth() + 1) + '/' + d.getFullYear(); }
    function deISO(texto) {
        var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(texto || '');
        if (!m) return null;
        var d = new Date(+m[1], +m[2] - 1, +m[3]);
        return d.getMonth() === +m[2] - 1 ? d : null;
    }
    function hoje() { var d = new Date(); return new Date(d.getFullYear(), d.getMonth(), d.getDate()); }
    function mesmoDia(a, b) { return !!a && !!b && a.getTime() === b.getTime(); }
    function somarDias(d, n) { return new Date(d.getFullYear(), d.getMonth(), d.getDate() + n); }
    function somarMeses(d, n) {
        var alvo = new Date(d.getFullYear(), d.getMonth() + n, 1);
        var ultimo = new Date(alvo.getFullYear(), alvo.getMonth() + 1, 0).getDate();
        return new Date(alvo.getFullYear(), alvo.getMonth(), Math.min(d.getDate(), ultimo));
    }
    function capitalizar(t) { return t.charAt(0).toUpperCase() + t.slice(1); }

    function criar(raiz) {
        if (raiz._sgcDatePicker) return raiz._sgcDatePicker;

        var input = raiz.querySelector('input[type="hidden"]');
        var trigger = raiz.querySelector('.sgc-datepicker__trigger');
        var valorEl = trigger.querySelector('.sgc-datepicker__valor');
        var campo = raiz.querySelector('.sgc-datepicker__campo') || raiz;
        var form = raiz.closest('form');
        var popover, titulo, corpo, visivel, foco, timerFechar;

        var inst = { raiz: raiz, abrir: abrir, fechar: fechar, selecionar: selecionar };
        raiz._sgcDatePicker = inst;

        function valorDoCampo(nome) {
            var el = nome && form ? form.elements[nome] : null;
            return el ? deISO(el.value) : null;
        }
        function desabilitado(d) {
            var min = valorDoCampo(raiz.dataset.minInput);
            var max = valorDoCampo(raiz.dataset.maxInput);
            return (!!min && d < min) || (!!max && d > max);
        }

        function montar() {
            popover = document.createElement('div');
            popover.className = 'sgc-datepicker__popover';
            popover.hidden = true;
            popover.setAttribute('role', 'dialog');
            popover.setAttribute('aria-label', trigger.getAttribute('title') || 'Escolher data');
            popover.innerHTML =
                '<div class="sgc-datepicker__cabecalho">' +
                    '<button type="button" class="sgc-datepicker__nav" data-acao="anterior" aria-label="Mês anterior"><i class="bi bi-chevron-left" aria-hidden="true"></i></button>' +
                    '<div class="sgc-datepicker__titulo" aria-live="polite"></div>' +
                    '<button type="button" class="sgc-datepicker__nav" data-acao="proximo" aria-label="Próximo mês"><i class="bi bi-chevron-right" aria-hidden="true"></i></button>' +
                '</div>' +
                '<table class="sgc-datepicker__grade" role="grid">' +
                    '<thead><tr>' + DIAS_SEMANA.map(function (d) {
                        return '<th scope="col" class="sgc-datepicker__semana" abbr="' + d[1] + '">' + d[0] + '</th>';
                    }).join('') + '</tr></thead>' +
                    '<tbody></tbody>' +
                '</table>' +
                '<div class="sgc-datepicker__rodape">' +
                    '<button type="button" class="sgc-datepicker__acao" data-acao="limpar">Limpar</button>' +
                    '<button type="button" class="sgc-datepicker__acao" data-acao="hoje">Hoje</button>' +
                '</div>';
            campo.appendChild(popover);
            titulo = popover.querySelector('.sgc-datepicker__titulo');
            corpo = popover.querySelector('tbody');

            popover.addEventListener('click', function (e) {
                var acao = e.target.closest('[data-acao]');
                if (acao) {
                    var tipo = acao.dataset.acao;
                    if (tipo === 'anterior' || tipo === 'proximo') {
                        foco = somarMeses(foco, tipo === 'anterior' ? -1 : 1);
                        visivel = new Date(foco.getFullYear(), foco.getMonth(), 1);
                        renderizar(false);
                    } else if (tipo === 'limpar') {
                        selecionar(null);
                    } else if (tipo === 'hoje') {
                        var h = hoje();
                        if (desabilitado(h)) { foco = h; visivel = new Date(h.getFullYear(), h.getMonth(), 1); renderizar(true); }
                        else selecionar(h);
                    }
                    return;
                }
                var dia = e.target.closest('.sgc-datepicker__dia');
                if (dia && dia.getAttribute('aria-disabled') !== 'true') selecionar(deISO(dia.dataset.iso));
            });

            popover.addEventListener('keydown', function (e) {
                var dia = e.target.closest('.sgc-datepicker__dia');
                if (!dia) return;
                var atual = deISO(dia.dataset.iso);
                var novo = null;
                switch (e.key) {
                    case 'ArrowLeft': novo = somarDias(atual, -1); break;
                    case 'ArrowRight': novo = somarDias(atual, 1); break;
                    case 'ArrowUp': novo = somarDias(atual, -7); break;
                    case 'ArrowDown': novo = somarDias(atual, 7); break;
                    case 'Home': novo = somarDias(atual, -atual.getDay()); break;
                    case 'End': novo = somarDias(atual, 6 - atual.getDay()); break;
                    case 'PageUp': novo = somarMeses(atual, e.shiftKey ? -12 : -1); break;
                    case 'PageDown': novo = somarMeses(atual, e.shiftKey ? 12 : 1); break;
                    default: return;
                }
                e.preventDefault();
                foco = novo;
                visivel = new Date(novo.getFullYear(), novo.getMonth(), 1);
                renderizar(true);
            });
        }

        function renderizar(focar) {
            var ano = visivel.getFullYear();
            var mes = visivel.getMonth();
            var deslocamento = new Date(ano, mes, 1).getDay();
            var diasNoMes = new Date(ano, mes + 1, 0).getDate();
            var selecionado = deISO(input.value);
            var dataHoje = hoje();
            var html = '';

            titulo.textContent = capitalizar(fmtMes.format(visivel));

            for (var celula = 0, dia = 1 - deslocamento; celula < Math.ceil((deslocamento + diasNoMes) / 7) * 7; celula++, dia++) {
                if (celula % 7 === 0) html += '<tr>';
                if (dia < 1 || dia > diasNoMes) {
                    html += '<td role="gridcell"></td>';
                } else {
                    var d = new Date(ano, mes, dia);
                    var sel = mesmoDia(d, selecionado);
                    var off = desabilitado(d);
                    var classes = 'sgc-datepicker__dia' + (sel ? ' is-selecionado' : '') +
                        (mesmoDia(d, dataHoje) ? ' is-hoje' : '') + (off ? ' is-desabilitado' : '');
                    html += '<td role="gridcell" aria-selected="' + sel + '">' +
                        '<button type="button" class="' + classes + '" data-iso="' + paraISO(d) + '"' +
                        ' tabindex="' + (mesmoDia(d, foco) ? '0' : '-1') + '"' +
                        ' aria-label="' + fmtDiaCompleto.format(d) + '"' +
                        (mesmoDia(d, dataHoje) ? ' aria-current="date"' : '') +
                        (off ? ' aria-disabled="true"' : '') + '>' + dia + '</button></td>';
                }
                if (celula % 7 === 6) html += '</tr>';
            }
            corpo.innerHTML = html;

            if (focar) {
                var alvo = corpo.querySelector('[tabindex="0"]');
                if (alvo) alvo.focus();
            }
        }

        function posicionar() {
            campo.classList.remove('is-alinhado-fim');
            var r = popover.getBoundingClientRect();
            if (r.right > document.documentElement.clientWidth - 8) campo.classList.add('is-alinhado-fim');
        }

        function abrir() {
            if (aberto && aberto !== inst) aberto.fechar(false);
            if (!popover) montar();
            clearTimeout(timerFechar);

            var sel = deISO(input.value);
            foco = sel || valorDoCampo(raiz.dataset.minInput) || valorDoCampo(raiz.dataset.maxInput) || hoje();
            visivel = new Date(foco.getFullYear(), foco.getMonth(), 1);

            popover.hidden = false;
            renderizar(true);
            posicionar();
            void popover.offsetWidth; // garante a transição de entrada
            popover.classList.add('is-visivel');
            raiz.classList.add('is-aberto');
            trigger.setAttribute('aria-expanded', 'true');
            aberto = inst;
        }

        function fechar(devolverFoco) {
            if (!popover || popover.hidden) return;
            popover.classList.remove('is-visivel');
            raiz.classList.remove('is-aberto');
            trigger.setAttribute('aria-expanded', 'false');
            timerFechar = setTimeout(function () { popover.hidden = true; }, 160);
            if (aberto === inst) aberto = null;
            if (devolverFoco) trigger.focus();
        }

        function selecionar(d) {
            input.value = d ? paraISO(d) : '';
            valorEl.textContent = d ? formatar(d) : PLACEHOLDER;
            raiz.classList.toggle('is-preenchido', !!d);
            input.dispatchEvent(new Event('change', { bubbles: true }));
            fechar(true);
        }

        trigger.addEventListener('click', function () {
            if (raiz.classList.contains('is-aberto')) fechar(false); else abrir();
        });
        trigger.addEventListener('keydown', function (e) {
            if (e.key === 'ArrowDown' && !raiz.classList.contains('is-aberto')) { e.preventDefault(); abrir(); }
        });

        return inst;
    }

    // Fecha ao clicar/focar fora e com Esc (listeners únicos no documento)
    document.addEventListener('pointerdown', function (e) {
        if (aberto && !aberto.raiz.contains(e.target)) aberto.fechar(false);
    });
    document.addEventListener('focusin', function (e) {
        if (aberto && !aberto.raiz.contains(e.target)) aberto.fechar(false);
    });
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && aberto) { e.preventDefault(); aberto.fechar(true); }
    });

    function iniciar(escopo) {
        (escopo || document).querySelectorAll('[data-sgc-datepicker]').forEach(criar);
    }

    window.SGCDatePicker = { iniciar: iniciar };

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { iniciar(); });
    else iniciar();
})();
