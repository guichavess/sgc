/* =====================================================================
   Hub de módulos (/hub) — cards expansíveis, busca e entrada com GSAP
   Requer: vendor/gsap (gsap, Flip, CustomEase) + js/sgc-motion.js
   Cards renderizados no servidor (templates/hub.html); aqui só comportamento.
   ===================================================================== */
(function () {
  'use strict';
  const root = document.documentElement;
  const hubEl = document.getElementById('hub');
  if (!hubEl || !window.gsap || !window.SGCMotion) { root.classList.remove('hub-preload'); return; }

  const M = window.SGCMotion, D = M.D;
  const LAST_KEY = 'sgc_hub_ultimo_modulo', INTRO_KEY = 'sgc_hub_intro_vista';

  const $ = id => document.getElementById(id);
  const grid = $('moduleGrid'), adminList = $('adminList'), adminBlock = $('adminBlock');
  const input = $('hubSearch'), status = $('hubSearchStatus'), aside = $('searchAside');
  const hoverDevice = window.matchMedia('(hover: hover) and (min-width: 768px)');
  const norm = s => s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
  const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const store = (area, k, v) => { try { const s = window[area + 'Storage']; return v === undefined ? s.getItem(k) : s.setItem(k, v); } catch (e) { return null; } };
  const allItems = () => [...(grid ? grid.children : []), ...(adminList ? adminList.children : [])];
  const visibleItems = () => allItems().filter(li => !li.hidden && li.dataset.match !== '0');

  function highlight(text, q) {
    if (!q) return esc(text);
    const i = norm(text).indexOf(q);
    if (i < 0) return esc(text);
    return esc(text.slice(0, i)) + '<mark>' + esc(text.slice(i, i + q.length)) + '</mark>' + esc(text.slice(i + q.length));
  }

  /* Índice de busca (resumo + recursos) e selo de último acesso */
  const lastId = store('local', LAST_KEY);
  allItems().forEach(li => {
    const textos = [li.querySelector('.detail-summary'), ...li.querySelectorAll('.detail-features li')];
    li.dataset.search = norm(textos.filter(Boolean).map(el => el.textContent).join(' '));
    if (li.dataset.id === lastId) li.querySelector('.card-side').insertAdjacentHTML('afterbegin', '<span class="badge-last">Último acesso</span>');
  });

  /* =====================================================================
     Card expansível
     Uma timeline PAUSADA por card: play() ao entrar, reverse() ao sair com timeScale 1,45
     (saída ≈ 70% da entrada). Interromper no meio é natural: a timeline só muda de direção.
       Ação primária:   altura do card cresce (300ms, ease de entrada) + sombra de elevação
       Secundária:      barra de cor alarga, ícone assenta, seta entra
       Follow-through:  filete desenha → resumo → funcionalidades em cascata (40ms) → rodapé
     A altura cresce no fluxo da grade, então os cards de baixo são empurrados.
     ===================================================================== */
  const EXPAND_INTENT = .12;           // intenção de hover: evita expandir ao só "passar" o mouse
  const cards = new WeakMap();         // card -> { tl }
  let active = null, intent = null, hovered = null, opening = null;

  function buildExpand(card) {
    const q = s => card.querySelector(s), qa = s => [...card.querySelectorAll(s)];
    const li = card.parentElement;
    const fullH = card.scrollHeight;   // altura natural com o detalhe

    const parts = [card, li, q('.card-elev'), q('.card-accent'), q('.card-tile'), q('.card-arrow'), q('.card-more i'), q('.detail-rule'),
      q('.detail-summary'), q('.detail-foot'), q('.card-detail'), ...qa('.detail-features li')].filter(Boolean);

    const tl = M.timeline({
      paused: true,
      // lazy:false — sem isso, um render adiado para o próximo tick pode reescrever a altura DEPOIS do clearProps
      defaults: { lazy: false },
      onReverseComplete() {
        card.classList.remove('is-expanded');
        // devolve ao CSS só o que o GSAP escreveu ('all' apagaria o --c inline com a cor do módulo)
        gsap.set(parts, { clearProps: 'height,top,bottom,zIndex,transform,opacity,visibility' });
        cards.delete(card);                        // próxima expansão remede (largura/rolagem podem ter mudado)
      },
    });

    tl.set(li, { zIndex: 10 }, 0);

    if (M.reduced()) {
      tl.set(card, { height: fullH }, 0)
        .fromTo(q('.card-detail'), { autoAlpha: 0 }, { autoAlpha: 1, duration: D.quick }, 0);
      return tl;
    }

    tl.to(card, { height: fullH, duration: D.standard, ease: 'sgc-enter' }, 0)
      .to(q('.card-elev'), { autoAlpha: 1, duration: D.quick }, 0)
      .to(q('.card-accent'), { scaleX: 1, duration: D.quick }, 0)
      .to(q('.card-tile'), { scale: 1.06, rotation: -4, duration: D.quick }, .02)
      .to(q('.card-arrow'), { autoAlpha: 1, x: 0, duration: D.quick }, .04)
      .to(q('.card-more i'), { rotation: 180, duration: D.quick }, 0)
      .fromTo(q('.detail-rule'), { scaleX: 0 }, { scaleX: 1, duration: D.slow }, .06)
      .fromTo(q('.detail-summary'), { autoAlpha: 0, y: 8 }, { autoAlpha: 1, y: 0, duration: D.quick, ease: 'sgc-enter' }, .1)
      .fromTo(qa('.detail-features li'), { autoAlpha: 0, y: 10 }, { autoAlpha: 1, y: 0, duration: D.quick, ease: 'sgc-enter', stagger: .04 }, .14)
      .fromTo(q('.detail-foot'), { autoAlpha: 0 }, { autoAlpha: 1, duration: D.quick }, .3);
    return tl;
  }

  function expand(card) {
    if (opening || !card || !card.querySelector('.card-detail')) return;
    if (active && active !== card) collapse(active);
    active = card;
    let st = cards.get(card);
    if (!st) { st = { tl: buildExpand(card) }; cards.set(card, st); }
    card.classList.add('is-expanded');
    card.querySelector('.card-more')?.setAttribute('aria-expanded', 'true');
    st.tl.timeScale(st.tl.timeScale() > 1.3 ? st.tl.timeScale() / 1.45 : st.tl.timeScale()).play();
    focusDim(card);
  }

  function collapse(card) {
    const st = cards.get(card);
    card.querySelector('.card-more')?.setAttribute('aria-expanded', 'false');
    if (active === card) { active = null; focusDim(null); }
    if (!st) return;
    st.tl.timeScale(st.tl.timeScale() * 1.45).reverse();
  }

  function collapseAll(instant) {
    intent?.kill(); intent = null;
    allItems().forEach(li => {
      const card = li.firstElementChild, st = cards.get(card);
      if (!st) return;
      card.querySelector('.card-more')?.setAttribute('aria-expanded', 'false');
      if (instant) { st.tl.progress(0).pause(); st.tl.eventCallback('onReverseComplete')(); } else collapse(card);
    });
    active = null;
    if (instant) gsap.set(allItems(), { clearProps: 'opacity' }); else focusDim(null);
  }

  /* Um herói por vez: os demais recuam levemente, a partir do card ativo (stagger por distância na grade) */
  function focusDim(card) {
    const items = allItems().filter(li => !li.hidden);
    if (!card) { gsap.to(items, { opacity: 1, duration: D.quick, overwrite: 'auto' }); return; }
    const idx = items.indexOf(card.parentElement);
    if (idx < 0) return;
    gsap.to(items, {
      opacity: (i, el) => (el === card.parentElement ? 1 : .6),
      duration: D.quick, overwrite: 'auto',
      stagger: { each: .025, from: idx, grid: 'auto' },
    });
  }

  function requestExpand(card) {
    intent?.kill();
    if (opening) return;
    if (active === card) { expand(card); return; }
    // Já há um card aberto: troca quase imediata (o usuário está "navegando" pelos cards)
    intent = gsap.delayedCall(active ? .05 : EXPAND_INTENT, () => expand(card));
  }

  /* ---------- Coreografia de entrada (timeline única, com labels) ---------- */
  function entrance(full) {
    const tl = M.timeline({ defaults: { ease: 'sgc-enter' } });
    const lis = grid ? [...grid.children] : [];
    const admins = adminList ? [...adminList.children] : [];
    root.classList.remove('hub-preload');

    if (full) {
      tl.from([$('hubEyebrow'), $('hubTitle'), $('searchWrap')].filter(Boolean), { autoAlpha: 0, y: M.dist(8), duration: D.quick, stagger: .06, ease: 'sgc-standard', clearProps: 'all' }, 0)
        .from($('hubSignature'), { scaleX: M.reduced() ? 1 : 0, autoAlpha: M.reduced() ? 0 : 1, duration: D.slow, ease: 'sgc-standard', clearProps: 'all' }, .14);
    }

    tl.addLabel('cards', full ? .08 : 0);
    const cardStagger = full ? { each: .07, grid: 'auto', from: 'start' } : .02;
    if (lis.length) {
      tl.from(lis, { autoAlpha: 0, y: M.dist(full ? 24 : 8), scale: M.sc(full ? .97 : 1), duration: full ? D.standard : D.quick, stagger: cardStagger, clearProps: 'all' }, 'cards');
      if (full && !M.reduced()) {
        tl.from(lis.map(li => li.querySelector('.card-accent')), { scaleY: 0, duration: D.standard, ease: 'sgc-standard', stagger: cardStagger, clearProps: 'transform' }, 'cards+=.08')
          .from(lis.map(li => li.querySelector('.card-tile')), { autoAlpha: 0, y: 6, scale: .85, duration: D.quick, stagger: cardStagger, clearProps: 'all' }, 'cards+=.12');
      }
    }
    if (admins.length) {
      tl.from($('adminTitle'), { autoAlpha: 0, duration: D.quick, clearProps: 'all' }, lis.length ? '>-.15' : 'cards')
        .from(admins, { autoAlpha: 0, y: M.dist(full ? 16 : 6), duration: D.quick, stagger: full ? .05 : .02, clearProps: 'all' }, '<.04');
    }
    const badge = hubEl.querySelector('.badge-last');
    if (badge) tl.from(badge, { autoAlpha: 0, scale: M.sc(.6), duration: D.standard, ease: 'back.out(1.4)', clearProps: 'all' }, full ? '>-.05' : '>');
    return tl;
  }

  /* ---------- Busca: saída → Flip (quem fica desliza) → entrada em cascata ---------- */
  let filterToken = 0;
  function filter(raw, animated = true) {
    const token = ++filterToken;
    const q = norm(raw.trim());
    const items = allItems();
    collapseAll(true);
    gsap.killTweensOf(items);
    gsap.set(items, { clearProps: 'all' });

    let n = 0;
    const leaving = [], entering = [];
    items.forEach(li => {
      const t = li.querySelector('.card-link'), d = li.querySelector('.card-desc');
      const hit = !q || norm(t.dataset.text).includes(q) || norm(d.dataset.text).includes(q) || (li.dataset.search || '').includes(q);
      t.innerHTML = highlight(t.dataset.text, q);
      d.innerHTML = highlight(d.dataset.text, q);
      li.dataset.match = hit ? '1' : '0';
      if (hit) n++;
      if (!hit && !li.hidden) leaving.push(li);
      if (hit && li.hidden) entering.push(li);
    });

    aside.innerHTML = raw ? '<button class="search-clear" type="button" aria-label="Limpar busca"><i class="bi bi-x-lg"></i></button>' : '<kbd>/</kbd>';
    status.textContent = q ? (n === 1 ? '1 módulo encontrado' : n + ' módulos encontrados') : '';

    const commit = () => {
      leaving.forEach(li => { li.hidden = true; gsap.set(li, { clearProps: 'all' }); });
      entering.forEach(li => { li.hidden = false; });
      if (adminBlock) adminBlock.style.display = q && ![...adminList.children].some(li => !li.hidden) ? 'none' : '';
    };

    if (!animated) { commit(); renderEmpty(raw, q, n, false); return; }

    const run = () => {
      if (token !== filterToken) return;
      M.flip(items.filter(li => !leaving.includes(li)), commit, {
        onEnter: () => gsap.fromTo(entering, { autoAlpha: 0, scale: M.sc(.96) }, { autoAlpha: 1, scale: 1, duration: D.quick, ease: 'sgc-enter', stagger: .03, delay: .06, clearProps: 'all' }),
      });
      renderEmpty(raw, q, n, true);
    };
    if (leaving.length) gsap.to(leaving, { autoAlpha: 0, scale: M.sc(.94), duration: .15, ease: 'sgc-exit', onComplete: run });
    else run();
  }

  function renderEmpty(raw, q, n, animated) {
    const box = $('emptySearch');
    if (!box) return;
    if (!(q && !n)) { box.innerHTML = ''; return; }
    if (box.firstElementChild) { box.querySelector('h2').textContent = `Nenhum módulo para “${raw.trim()}”`; return; }
    box.innerHTML = `<div class="empty"><i class="bi bi-search" aria-hidden="true"></i><h2>Nenhum módulo para “${esc(raw.trim())}”</h2>
      <p>Tente o nome do módulo ou um recurso, como “empenho”.</p><button class="hub-btn-outline" type="button" id="emptyClear">Limpar busca</button></div>`;
    if (!animated) return;
    const tl = M.timeline().from(box.firstElementChild, { autoAlpha: 0, y: M.dist(8), duration: D.quick, ease: 'sgc-enter', delay: .06, clearProps: 'all' });
    if (!M.reduced()) tl.to(box.querySelector('i'), { keyframes: { x: [0, -5, 4, -2, 0] }, duration: .35, ease: 'sgc-inOut', clearProps: 'transform' }, '<.1');
  }

  /* ---------- Abrir módulo: antecipação → ação → reação, enquanto o navegador segue o link ---------- */
  function open(card) {
    if (!card || opening) return;
    intent?.kill();
    const li = card.parentElement;
    store('local', LAST_KEY, li.dataset.id);
    const desc = card.querySelector('.card-desc');
    const statusEl = card.querySelector('.card-status');
    const others = allItems().filter(x => x !== li && !x.hidden);
    card.classList.add('is-opening');
    card.setAttribute('aria-busy', 'true');

    const tl = M.timeline({ defaults: { ease: 'sgc-standard' } });
    tl.to(card, { scale: M.sc(.98), duration: D.press, yoyo: true, repeat: 1 }, 0)
      .fromTo(card.querySelector('.card-progress'), { scaleX: 0 }, { scaleX: .92, duration: 1.2, ease: 'sgc-inOut' }, 0)
      .to(desc, { autoAlpha: 0, y: M.dist(-6), duration: .12, ease: 'sgc-exit' }, 0)
      // troca por crossfade entre dois elementos (sem mexer em texto): o reverse() desfaz sozinho
      .fromTo(statusEl, { autoAlpha: 0, y: M.dist(6) }, { autoAlpha: 1, y: 0, duration: D.quick, ease: 'sgc-enter', immediateRender: false }, .1)
      .to(others, { opacity: .4, scale: M.sc(.985), duration: D.quick, stagger: { each: .03, from: Math.max(0, allItems().indexOf(li)), grid: 'auto' } }, .06);
    // Segurança: se a navegação não acontecer (ex.: download, erro de rede), a tela volta ao normal
    opening = { card, desc, tl, timer: gsap.delayedCall(8, restoreOpening) };
  }

  function restoreOpening() {
    if (!opening) return;
    const { card, desc, tl } = opening;
    opening = null;
    tl.eventCallback('onReverseComplete', () => {
      gsap.set([card, desc, card.querySelector('.card-status'), card.querySelector('.card-progress')], { clearProps: 'transform,opacity,visibility' });
      card.classList.remove('is-opening'); card.removeAttribute('aria-busy');
      if (hovered !== card) collapse(card);
      if (active) focusDim(active);
    });
    tl.timeScale(tl.timeScale() * 2).reverse();
  }

  /* Voltar pelo histórico (bfcache): a página reaparece como foi deixada — desfaz na hora */
  function resetOpening() {
    if (!opening) return;
    const { card, desc, tl, timer } = opening;
    opening = null;
    timer.kill(); tl.progress(0).kill();
    gsap.set([card, desc, card.querySelector('.card-status'), card.querySelector('.card-progress'), ...allItems()], { clearProps: 'transform,opacity,visibility' });
    card.classList.remove('is-opening'); card.removeAttribute('aria-busy');
  }

  /* ---------- Spotlight: gsap.quickTo (um tween reaproveitado, sem criar tween por evento) ---------- */
  const spots = new WeakMap();
  hubEl.addEventListener('pointermove', e => {
    if (M.reduced() || e.pointerType === 'touch') return;
    const card = e.target.closest && e.target.closest('.module-card');
    if (!card) return;
    let s = spots.get(card);
    if (!s) {
      gsap.set(card, { '--mx': e.offsetX || 200, '--my': 48 });
      s = { x: gsap.quickTo(card, '--mx', { duration: .35, ease: 'power3' }), y: gsap.quickTo(card, '--my', { duration: .35, ease: 'power3' }) };
      spots.set(card, s);
    }
    const r = card.getBoundingClientRect();
    s.x(e.clientX - r.left); s.y(e.clientY - r.top);
  });

  /* ---------- Hover / foco / toque ---------- */
  hubEl.addEventListener('pointerover', e => {
    if (e.pointerType === 'touch' || !hoverDevice.matches) return;
    const card = e.target.closest('.module-card');
    if (!card || card === hovered) return;
    hovered = card;
    requestExpand(card);
  });
  hubEl.addEventListener('pointerout', e => {
    if (e.pointerType === 'touch' || !hoverDevice.matches) return;
    const card = e.target.closest('.module-card');
    if (!card || card.contains(e.relatedTarget)) return;
    if (hovered === card) hovered = null;
    intent?.kill();
    if (opening && opening.card === card) return;
    // saiu da grade (ou foi para o vão entre cards): recolhe
    if (!e.relatedTarget || !e.relatedTarget.closest('.module-card')) collapse(card);
  });
  hubEl.addEventListener('focusin', e => {
    const link = e.target.closest('.card-link');
    if (link && link.matches(':focus-visible')) { intent?.kill(); expand(link.closest('.module-card')); }
  });
  hubEl.addEventListener('focusout', e => {
    const card = e.target.closest('.module-card');
    if (card && !card.contains(e.relatedTarget) && hovered !== card) collapse(card);
  });

  hubEl.addEventListener('click', e => {
    const more = e.target.closest('.card-more');
    if (more) {
      const card = more.closest('.module-card');
      card.classList.contains('is-expanded') && active === card ? collapse(card) : expand(card);
      return;
    }
    const link = e.target.closest('.card-link');
    if (link) {
      if (opening) { e.preventDefault(); return; }          // evita duplo clique
      const card = link.closest('.module-card');
      // ctrl/shift/meta/botão do meio abrem em outra aba: só registra o último acesso
      if (e.button !== 0 || e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) store('local', LAST_KEY, card.parentElement.dataset.id);
      else open(card);
      return;
    }
    if (e.target.closest('.search-clear, #emptyClear')) { input.value = ''; filter(''); input.focus(); }
  });

  input.addEventListener('input', () => filter(input.value));

  document.addEventListener('keydown', e => {
    const typing = document.activeElement === input;
    // não interfere em outros campos, modais e menus da página
    if (!typing && e.target.closest && e.target.closest('input, textarea, select, [contenteditable="true"], .modal, .dropdown-menu')) return;
    if (document.querySelector('.modal.show')) return;
    if (e.key === '/' && !typing) { e.preventDefault(); input.focus(); input.select(); return; }
    if (e.key === 'Escape') {
      if (typing) { input.value = ''; filter(''); input.blur(); } else if (active) collapse(active);
      return;
    }
    if (typing && (e.key === 'Enter' || e.key === 'ArrowDown')) {
      const first = visibleItems()[0];
      if (first) { e.preventDefault(); const l = first.querySelector('.card-link'); e.key === 'Enter' ? l.click() : l.focus(); }
      return;
    }
    if (!typing && grid && /^[1-9]$/.test(e.key) && !e.ctrlKey && !e.metaKey && !e.altKey) {
      const li = grid.children[+e.key - 1];
      if (li && !li.hidden) { const l = li.querySelector('.card-link'); l.focus(); l.click(); }
      return;
    }
    const cur = document.activeElement.closest && document.activeElement.closest('.module-item');
    if (!cur || !['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(e.key)) return;
    const list = visibleItems(), idx = list.indexOf(cur);
    const top0 = list[0].getBoundingClientRect().top;
    const cols = Math.max(1, list.filter(li => Math.abs(li.getBoundingClientRect().top - top0) < 4).length);
    const next = list[idx + { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -cols, ArrowDown: cols }[e.key]];
    e.preventDefault();
    if (next) next.querySelector('.card-link').focus(); else if (e.key === 'ArrowUp') input.focus();
  });

  hoverDevice.addEventListener('change', () => collapseAll(true));
  let resizeT;
  window.addEventListener('resize', () => { clearTimeout(resizeT); resizeT = setTimeout(() => { if (active) collapseAll(true); }, 150); });
  window.addEventListener('pageshow', e => {
    if (!e.persisted) return;
    resetOpening();
    collapseAll(true);
    if (input.value) filter(input.value, false);
  });

  const firstVisit = store('session', INTRO_KEY) !== '1';
  store('session', INTRO_KEY, '1');
  entrance(firstVisit);
})();
