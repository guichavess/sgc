/* =====================================================================
   SGCMotion — camada fina sobre o GSAP com as regras de movimento do SGC
   Requer: vendor/gsap/gsap.min.js, Flip.min.js, CustomEase.min.js (sem CDN)

   motion-design-skill: personalidade Corporate, curva assinatura (.2,0,0,1),
     entrada ease-out / saída ease-in (~70%) / na tela in-out, cascata < 500ms,
     mobile 0,8×, reduced motion = sem deslocamento, só opacidade, metade do tempo
   gsap-skills: registerPlugin antes do uso, timelines em vez de delay,
     transforms/autoAlpha, clearProps para devolver o controle ao CSS
   Tokens CSS equivalentes: css/base/motion.css
   ===================================================================== */
window.SGCMotion = (function () {
  'use strict';
  if (!window.gsap) return null;

  gsap.registerPlugin(Flip, CustomEase);
  CustomEase.create('sgc-standard', '.2,0,0,1');
  CustomEase.create('sgc-enter', '.05,.7,.1,1');
  CustomEase.create('sgc-exit', '.3,0,1,1');
  CustomEase.create('sgc-inOut', '.4,0,.2,1');
  // lazy:false: tweens renderizam na hora. Evita que um render adiado reescreva estilos já limpos por clearProps.
  gsap.defaults({ ease: 'sgc-standard', duration: .3, lazy: false });

  const D = { press: .06, micro: .09, quick: .2, standard: .3, slow: .45 };
  const mqReduce = window.matchMedia('(prefers-reduced-motion: reduce)');
  const reduced = () => mqReduce.matches;
  const mobile = () => window.innerWidth < 576;
  const dist = v => (reduced() ? 0 : v);            // deslocamento some com reduced motion
  const sc = v => (reduced() ? 1 : v);              // escala idem

  /* timeline com a escala de tempo do contexto (mobile 0,8×, reduzido 0,5×) */
  function timeline(vars = {}) {
    const tl = gsap.timeline(vars);
    tl.timeScale(reduced() ? 2 : mobile() ? 1.25 : 1);
    return tl;
  }

  /* FLIP de layout: captura → muda o DOM → anima do estado antigo para o novo */
  function flip(targets, mutate, vars = {}) {
    const state = Flip.getState(targets);
    mutate();
    if (reduced()) { vars.onEnter && vars.onEnter([]); return null; }
    return Flip.from(state, { duration: .25, ease: 'sgc-inOut', ...vars });
  }

  function setSpeed(s) {
    gsap.globalTimeline.timeScale(s);
    document.documentElement.style.setProperty('--motion-speed', s);
  }

  return { D, reduced, mobile, dist, sc, timeline, flip, setSpeed };
})();
