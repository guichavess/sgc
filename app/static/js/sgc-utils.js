/**
 * SGC — Utilitários compartilhados para AJAX, botões e feedback.
 *
 * Depende de Bootstrap 5.3 (Toast, classe .is-invalid).
 * Incluir em base_layout.html ou nos base templates dos módulos.
 *
 * Uso:
 *   SGC.lockBtn(btn)                   — desabilita + spinner
 *   SGC.unlockBtn(btn)                 — restaura estado original
 *   SGC.showToast(msg, 'success')      — toast no canto inferior direito
 *   SGC.markInvalid(field, msg)        — aplica .is-invalid + feedback
 *   SGC.clearInvalid(form)             — limpa todas as marcações
 *   SGC.validateRequired(form)         — valida campos [required] visualmente
 *   SGC.fetchJSON(url, opts)           — fetch com tratamento de erro padrão
 */
window.SGC = (function() {
    'use strict';

    /* ================================================================
     * BOTÕES — lock / unlock
     * ================================================================ */
    var _btnState = new WeakMap();

    function lockBtn(btn) {
        if (!btn || btn.disabled) return;
        _btnState.set(btn, btn.innerHTML);
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Aguarde\u2026';
    }

    function unlockBtn(btn) {
        if (!btn) return;
        var original = _btnState.get(btn);
        btn.disabled = false;
        if (original !== undefined) {
            btn.innerHTML = original;
            _btnState.delete(btn);
        }
    }

    /* ================================================================
     * TOAST — feedback não-bloqueante
     * ================================================================ */
    var _toastContainer = null;

    function _getToastContainer() {
        if (_toastContainer) return _toastContainer;
        // Tenta containers existentes (notification_toasts.html)
        _toastContainer = document.getElementById('toast-container-alerta')
                       || document.getElementById('toastContainer');
        if (!_toastContainer) {
            _toastContainer = document.createElement('div');
            _toastContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
            _toastContainer.style.zIndex = '1090';
            document.body.appendChild(_toastContainer);
        }
        return _toastContainer;
    }

    var _iconMap = {
        success: 'bi-check-circle-fill',
        danger:  'bi-exclamation-triangle-fill',
        warning: 'bi-exclamation-circle-fill',
        info:    'bi-info-circle-fill'
    };
    var _bgMap = {
        success: '#198754',
        danger:  '#dc3545',
        warning: '#fd7e14',
        info:    '#343990'
    };

    function showToast(msg, tipo, duracao) {
        tipo = tipo || 'info';
        duracao = duracao || 5000;

        var icon = _iconMap[tipo] || _iconMap.info;
        var bg   = _bgMap[tipo]   || _bgMap.info;

        var el = document.createElement('div');
        el.className = 'toast align-items-center text-white border-0 shadow-lg';
        el.setAttribute('role', 'alert');
        el.style.cssText = 'background-color: ' + bg + '; min-width: 300px;';
        el.innerHTML =
            '<div class="d-flex">' +
                '<div class="toast-body d-flex align-items-center gap-2">' +
                    '<i class="bi ' + icon + ' fs-5"></i>' +
                    '<span>' + msg + '</span>' +
                '</div>' +
                '<button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>' +
            '</div>';
        _getToastContainer().appendChild(el);
        var toast = new bootstrap.Toast(el, { delay: duracao });
        toast.show();
        el.addEventListener('hidden.bs.toast', function() { el.remove(); });
    }

    /* ================================================================
     * VALIDAÇÃO VISUAL — .is-invalid
     * ================================================================ */
    function markInvalid(field, msg) {
        if (!field) return;
        field.classList.add('is-invalid');
        // Cria ou atualiza o .invalid-feedback irmão
        var fb = field.parentElement.querySelector('.invalid-feedback');
        if (!fb) {
            fb = document.createElement('div');
            fb.className = 'invalid-feedback';
            field.parentElement.appendChild(fb);
        }
        fb.textContent = msg || 'Campo obrigatório.';
        // Limpa ao digitar/selecionar
        field.addEventListener('input', function handler() {
            field.classList.remove('is-invalid');
            field.removeEventListener('input', handler);
        }, { once: true });
        field.addEventListener('change', function handler() {
            field.classList.remove('is-invalid');
            field.removeEventListener('change', handler);
        }, { once: true });
    }

    function clearInvalid(form) {
        if (!form) return;
        form.querySelectorAll('.is-invalid').forEach(function(el) {
            el.classList.remove('is-invalid');
        });
    }

    /**
     * Valida todos os campos [required] visíveis de um form.
     * Retorna true se todos são válidos, false se algum falhou.
     * Aplica .is-invalid + scroll ao primeiro campo com erro.
     */
    function validateRequired(form) {
        clearInvalid(form);
        var ok = true;
        var primeiro = null;
        form.querySelectorAll('[required]').forEach(function(el) {
            // Pula campos ocultos
            if (el.offsetParent === null) return;
            var val = (el.value || '').trim();
            if (!val) {
                markInvalid(el);
                ok = false;
                if (!primeiro) primeiro = el;
            }
        });
        if (primeiro) {
            primeiro.scrollIntoView({ behavior: 'smooth', block: 'center' });
            primeiro.focus();
        }
        return ok;
    }

    /* ================================================================
     * FETCH — wrapper com tratamento de erro
     * ================================================================ */

    /**
     * Wrapper de fetch que retorna o JSON parsed.
     * Em caso de erro HTTP ou de rede, lança Error com mensagem amigável.
     *
     * @param {string} url
     * @param {object} opts - opções do fetch (method, body, headers, etc.)
     * @returns {Promise<object>} - resposta JSON
     */
    async function fetchJSON(url, opts) {
        opts = opts || {};
        if (!opts.headers) opts.headers = {};
        opts.headers['X-Requested-With'] = 'XMLHttpRequest';

        var resp;
        try {
            resp = await fetch(url, opts);
        } catch (e) {
            throw new Error('Erro de conexão. Verifique sua internet e tente novamente.');
        }

        var data;
        try {
            data = await resp.json();
        } catch (e) {
            if (!resp.ok) throw new Error('Erro do servidor (HTTP ' + resp.status + ').');
            throw new Error('Resposta inesperada do servidor.');
        }

        // Anexa status HTTP para uso externo
        data._httpStatus = resp.status;
        return data;
    }

    /* ================================================================
     * DUPLO-CLIQUE — proteção de formulário
     * ================================================================ */

    /**
     * Protege um form contra duplo submit.
     * Retorna uma função `release()` para desbloquear caso haja erro.
     *
     * Uso:
     *   var release = SGC.guardSubmit(form, btn);
     *   if (!release) return;  // já está submetendo
     *   try { ... } catch { release(); }
     */
    var _submitting = new WeakSet();

    function guardSubmit(form, btn) {
        if (_submitting.has(form)) return null;
        _submitting.add(form);
        if (btn) lockBtn(btn);

        return function release() {
            _submitting.delete(form);
            if (btn) unlockBtn(btn);
        };
    }

    /* ================================================================
     * MÁSCARA MONETÁRIA — formato brasileiro (1.234,56)
     * ================================================================ */

    /**
     * Aplica máscara monetária brasileira a um input.
     * Aceita digitação livre e formata em tempo real.
     * O valor raw (float) fica em input.dataset.rawValue.
     *
     * Uso:
     *   SGC.maskMoney(document.getElementById('campo'));
     *   SGC.maskMoney('#campo');                          // aceita seletor
     *   SGC.maskMoneyAll('.mask-money');                   // aplica a todos
     *
     * Para ler o valor numérico:  SGC.parseMoney(input.value) → float
     */
    function _formatBRL(centavos) {
        if (centavos === null || centavos === undefined) return '';
        var neg = centavos < 0;
        centavos = Math.abs(centavos);
        var inteiro = Math.floor(centavos / 100);
        var dec = centavos % 100;
        var strInt = String(inteiro).replace(/\B(?=(\d{3})+(?!\d))/g, '.');
        return (neg ? '-' : '') + strInt + ',' + (dec < 10 ? '0' : '') + dec;
    }

    function maskMoney(input) {
        if (typeof input === 'string') {
            input = document.querySelector(input);
        }
        if (!input) return;

        // Se já tiver valor ao iniciar, formata
        if (input.value) {
            var initial = parseMoney(input.value);
            if (initial) {
                var centavos = Math.round(initial * 100);
                input.value = _formatBRL(centavos);
                input.dataset.rawValue = initial;
            }
        }

        input.addEventListener('input', function() {
            // Extrai apenas dígitos
            var digits = input.value.replace(/\D/g, '');
            if (!digits) {
                input.value = '';
                input.dataset.rawValue = '';
                return;
            }
            var centavos = parseInt(digits, 10);
            input.value = _formatBRL(centavos);
            input.dataset.rawValue = (centavos / 100).toFixed(2);
        });
    }

    /**
     * Aplica maskMoney a todos os elementos que casam com o seletor.
     */
    function maskMoneyAll(selector) {
        document.querySelectorAll(selector).forEach(function(el) {
            maskMoney(el);
        });
    }

    /**
     * Converte string monetária brasileira para float.
     * "1.234,56" → 1234.56
     * Aceita também formato com ponto decimal (1234.56).
     */
    function parseMoney(str) {
        if (!str) return 0;
        str = String(str).trim();
        // Se tem vírgula, é formato BR
        if (str.indexOf(',') !== -1) {
            str = str.replace(/\./g, '').replace(',', '.');
        }
        var val = parseFloat(str);
        return isNaN(val) ? 0 : val;
    }

    /* ================================================================
     * API PÚBLICA
     * ================================================================ */
    return {
        lockBtn: lockBtn,
        unlockBtn: unlockBtn,
        showToast: showToast,
        markInvalid: markInvalid,
        clearInvalid: clearInvalid,
        validateRequired: validateRequired,
        fetchJSON: fetchJSON,
        guardSubmit: guardSubmit,
        maskMoney: maskMoney,
        maskMoneyAll: maskMoneyAll,
        parseMoney: parseMoney,
    };
})();
