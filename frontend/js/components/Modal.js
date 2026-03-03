/**
 * Componente: Gerenciador de Modais
 */

export class ModalManager {
    constructor(modalId) {
        this.modalElement = document.getElementById(modalId);
        if (!this.modalElement) {
            console.warn(`Modal com ID "${modalId}" não encontrado`);
            return;
        }

        // Inicializa o modal do Bootstrap
        this.bsModal = new bootstrap.Modal(this.modalElement);

        // Referências aos elementos internos
        this.progressBar = this.modalElement.querySelector('.progress-bar');
        this.statusMessage = this.modalElement.querySelector('.status-message');
        this.logContainer = this.modalElement.querySelector('.log-container');
    }

    /**
     * Mostra o modal
     */
    show() {
        if (this.bsModal) {
            this.bsModal.show();
        }
    }

    /**
     * Esconde o modal
     */
    hide() {
        if (this.bsModal) {
            this.bsModal.hide();
        }
    }

    /**
     * Atualiza a barra de progresso
     * @param {number} percent - Porcentagem (0-100)
     * @param {string} message - Mensagem opcional
     */
    updateProgress(percent, message = '') {
        if (this.progressBar) {
            this.progressBar.style.width = `${percent}%`;
            this.progressBar.textContent = `${percent}%`;
            this.progressBar.setAttribute('aria-valuenow', percent);
        }

        if (message && this.statusMessage) {
            this.statusMessage.textContent = message;
        }
    }

    /**
     * Adiciona uma linha ao log
     * @param {string} text - Texto do log
     * @param {string} type - Tipo (info, success, error, warning)
     */
    addLog(text, type = 'info') {
        if (!this.logContainer) return;

        const iconMap = {
            info: 'bi-info-circle text-info',
            success: 'bi-check-circle text-success',
            error: 'bi-x-circle text-danger',
            warning: 'bi-exclamation-circle text-warning',
        };

        const icon = iconMap[type] || iconMap.info;
        const timestamp = new Date().toLocaleTimeString('pt-BR');

        const logLine = document.createElement('div');
        logLine.className = 'log-line small';
        logLine.innerHTML = `
            <span class="text-muted">[${timestamp}]</span>
            <i class="bi ${icon} mx-1"></i>
            <span>${text}</span>
        `;

        this.logContainer.appendChild(logLine);
        this.logContainer.scrollTop = this.logContainer.scrollHeight;
    }

    /**
     * Limpa o log
     */
    clearLog() {
        if (this.logContainer) {
            this.logContainer.innerHTML = '';
        }
    }

    /**
     * Reseta o estado do modal
     */
    reset() {
        this.updateProgress(0, '');
        this.clearLog();
    }

    /**
     * Define o modal como carregando
     * @param {boolean} loading
     */
    setLoading(loading) {
        const spinner = this.modalElement.querySelector('.spinner-border');
        if (spinner) {
            spinner.style.display = loading ? 'inline-block' : 'none';
        }
    }
}

export default ModalManager;
