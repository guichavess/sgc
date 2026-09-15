# Bibliotecas de terceiros (vendor)

Todas as bibliotecas de front-end são servidas localmente. A intranet pode bloquear
CDNs, e sem estes arquivos as telas ficam sem layout, ícones, máscaras, selects e
gráficos. O teste `tests/test_sem_cdn.py` impede que novas referências a CDN entrem.

Para atualizar: `npm pack <pacote>@<versão>`, extrair e copiar só os arquivos listados
(não usar o `package.json` do Vite). Manter a versão exata e atualizar esta tabela.

| Pasta | Pacote npm | Versão | Arquivos | Licença |
|---|---|---|---|---|
| `bootstrap/` | bootstrap | 5.3.0 | `css/bootstrap.min.css`, `js/bootstrap.bundle.min.js` (+ `.map`) | MIT |
| `bootstrap-icons/` | bootstrap-icons | 1.10.0 | `bootstrap-icons.css`, `fonts/*.woff2`, `fonts/*.woff` | MIT |
| `inter/` | @fontsource/inter | 5.3.0 | `inter-latin-{300,400,600,700}-normal.woff2` (usados em `css/base/fonts.css`) | OFL-1.1 |
| `select2/` | select2 | 4.1.0-rc.0 | `css/select2.min.css`, `js/select2.min.js`, `js/i18n/pt-BR.js` | MIT |
| `select2/` | select2-bootstrap-5-theme | 1.3.0 | `css/select2-bootstrap-5-theme.min.css` | MIT |
| `tom-select/` | tom-select | 2.3.1 | `css/tom-select.bootstrap5.min.css`, `js/tom-select.complete.min.js` (+ `.map`) | Apache-2.0 |
| `flatpickr/` | flatpickr | 4.6.13 | `flatpickr.min.css`, `flatpickr.min.js`, `themes/airbnb.css`, `l10n/pt.js` | MIT |
| `chart.js/` | chart.js | 4.4.7 | `chart.umd.min.js` (= `dist/chart.umd.js`, já minificado) + `chart.umd.js.map` | MIT |
| `apexcharts/` | apexcharts | 7.3.0 | `apexcharts.min.js` | MIT |
| `sweetalert2/` | sweetalert2 | 11.26.25 | `sweetalert2.all.min.js` | MIT |
| `html2pdf/` | html2pdf.js | 0.10.1 | `html2pdf.bundle.min.js` (+ `.map`, `.LICENSE.txt`) | MIT |
| `jquery-mask/` | jquery-mask-plugin | 1.14.16 | `jquery.mask.min.js` | MIT |
| `jquery/` | jquery | 3.7.1 | `jquery-3.7.1.min.js` | MIT |
| `datatables/` | datatables.net (+ bs5) | 1.13.8 | `js/jquery.dataTables.min.js`, `js/dataTables.bootstrap5.min.js`, `css/dataTables.bootstrap5.min.css`, `i18n/pt-BR.json` | MIT |
| `gsap/` | gsap | 3.15.0 | `gsap.min.js`, `Flip.min.js`, `CustomEase.min.js` | GSAP Standard License |
