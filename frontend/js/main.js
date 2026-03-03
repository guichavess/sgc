/**
 * SGC - Sistema de Gestão de Pagamentos
 * Entry point principal do JavaScript
 */

// Utilitários
export { api, ApiClient } from './utils/api.js';
export { formatMoney, formatDate, formatDateTime, parseMoney } from './utils/formatters.js';

// Componentes
export { ModalManager } from './components/Modal.js';
export { MoneyInput } from './components/MoneyInput.js';

/**
 * Inicialização global
 * Roda quando o DOM estiver pronto
 */
document.addEventListener('DOMContentLoaded', () => {
    console.log('SGC Frontend carregado');

    // Aplica máscaras monetárias automaticamente
    const moneyInputs = document.querySelectorAll('.money-mask');
    if (moneyInputs.length > 0) {
        import('./components/MoneyInput.js').then(({ MoneyInput }) => {
            MoneyInput.applyToAll('.money-mask');
        });
    }

    // Inicializa tooltips do Bootstrap se existirem
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    if (tooltipTriggerList.length > 0) {
        tooltipTriggerList.forEach(el => new bootstrap.Tooltip(el));
    }

    // Inicializa popovers do Bootstrap se existirem
    const popoverTriggerList = document.querySelectorAll('[data-bs-toggle="popover"]');
    if (popoverTriggerList.length > 0) {
        popoverTriggerList.forEach(el => new bootstrap.Popover(el));
    }
});
