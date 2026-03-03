/**
 * Funções de Formatação
 */

/**
 * Formata valor como moeda brasileira (R$)
 * @param {number} value - Valor numérico
 * @returns {string} Valor formatado
 */
export function formatMoney(value) {
    if (value === null || value === undefined) return 'R$ 0,00';

    return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: 'BRL',
    }).format(value);
}

/**
 * Converte string de moeda para número
 * @param {string} value - Valor em formato de moeda
 * @returns {number} Valor numérico
 */
export function parseMoney(value) {
    if (!value) return 0;

    // Remove R$, pontos de milhar e converte vírgula para ponto
    const cleaned = value
        .replace('R$', '')
        .replace(/\./g, '')
        .replace(',', '.')
        .trim();

    return parseFloat(cleaned) || 0;
}

/**
 * Formata data para exibição (DD/MM/YYYY)
 * @param {Date|string} date - Data
 * @returns {string} Data formatada
 */
export function formatDate(date) {
    if (!date) return '--';

    const d = typeof date === 'string' ? new Date(date) : date;

    return new Intl.DateTimeFormat('pt-BR').format(d);
}

/**
 * Formata data e hora (DD/MM/YYYY HH:mm)
 * @param {Date|string} date - Data
 * @returns {string} Data e hora formatada
 */
export function formatDateTime(date) {
    if (!date) return '--';

    const d = typeof date === 'string' ? new Date(date) : date;

    return new Intl.DateTimeFormat('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
    }).format(d);
}

/**
 * Formata competência (MM/YYYY ou Mês/YYYY)
 * @param {string} value - Competência
 * @returns {string} Competência formatada
 */
export function formatCompetencia(value) {
    if (!value) return '--';
    return value;
}

/**
 * Formata número com separadores de milhar
 * @param {number} value - Valor numérico
 * @returns {string} Valor formatado
 */
export function formatNumber(value) {
    if (value === null || value === undefined) return '0';

    return new Intl.NumberFormat('pt-BR').format(value);
}

/**
 * Trunca texto com reticências
 * @param {string} text - Texto
 * @param {number} maxLength - Tamanho máximo
 * @returns {string} Texto truncado
 */
export function truncateText(text, maxLength = 50) {
    if (!text || text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

export default {
    formatMoney,
    parseMoney,
    formatDate,
    formatDateTime,
    formatCompetencia,
    formatNumber,
    truncateText,
};
