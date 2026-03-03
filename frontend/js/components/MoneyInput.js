/**
 * Componente: Input de Valor Monetário
 * Aplica máscara de moeda brasileira em inputs
 */

export class MoneyInput {
    constructor(element) {
        this.element = typeof element === 'string'
            ? document.querySelector(element)
            : element;

        if (!this.element) {
            console.warn('MoneyInput: Elemento não encontrado');
            return;
        }

        this.init();
    }

    init() {
        this.element.addEventListener('input', (e) => this.handleInput(e));
        this.element.addEventListener('blur', (e) => this.handleBlur(e));
        this.element.addEventListener('focus', (e) => this.handleFocus(e));

        // Formata valor inicial se existir
        if (this.element.value) {
            this.element.value = this.format(this.element.value);
        }
    }

    handleInput(e) {
        const cursorPosition = e.target.selectionStart;
        const oldLength = e.target.value.length;

        e.target.value = this.format(e.target.value);

        // Ajusta posição do cursor
        const newLength = e.target.value.length;
        const diff = newLength - oldLength;
        e.target.setSelectionRange(cursorPosition + diff, cursorPosition + diff);
    }

    handleBlur(e) {
        // Garante formatação completa ao sair do campo
        if (e.target.value) {
            e.target.value = this.format(e.target.value);
        }
    }

    handleFocus(e) {
        // Seleciona todo o texto ao focar
        setTimeout(() => e.target.select(), 0);
    }

    /**
     * Formata valor como moeda brasileira
     * @param {string} value - Valor a formatar
     * @returns {string} Valor formatado
     */
    format(value) {
        // Remove tudo exceto números
        let numericValue = value.replace(/\D/g, '');

        if (!numericValue) return '';

        // Converte para centavos
        let cents = parseInt(numericValue, 10);

        // Limita a um valor máximo razoável
        if (cents > 99999999999) {
            cents = 99999999999;
        }

        // Formata como moeda
        const formatted = (cents / 100).toLocaleString('pt-BR', {
            style: 'currency',
            currency: 'BRL',
        });

        return formatted;
    }

    /**
     * Retorna o valor numérico (em reais)
     * @returns {number}
     */
    getValue() {
        const value = this.element.value;
        if (!value) return 0;

        const numericValue = value
            .replace('R$', '')
            .replace(/\./g, '')
            .replace(',', '.')
            .trim();

        return parseFloat(numericValue) || 0;
    }

    /**
     * Define o valor programaticamente
     * @param {number} value - Valor em reais
     */
    setValue(value) {
        if (typeof value === 'number') {
            this.element.value = this.format(String(Math.round(value * 100)));
        }
    }

    /**
     * Aplica máscara a todos os inputs com a classe especificada
     * @param {string} selector - Seletor CSS
     * @returns {MoneyInput[]} Array de instâncias
     */
    static applyToAll(selector = '.money-mask') {
        const elements = document.querySelectorAll(selector);
        return Array.from(elements).map(el => new MoneyInput(el));
    }
}

export default MoneyInput;
