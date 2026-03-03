/**
 * Página: Dashboard
 * Funcionalidades: Sincronização, filtros, máscaras monetárias
 */

import { ModalManager } from '../components/Modal.js';
import { MoneyInput } from '../components/MoneyInput.js';
import { formatMoney } from '../utils/formatters.js';

class DashboardPage {
    constructor() {
        this.syncModal = null;
        this.progressModal = null;
        this.moneyInputs = [];

        this.init();
    }

    init() {
        // Inicializa modais se existirem
        const syncModalEl = document.getElementById('modalSyncSei');
        if (syncModalEl) {
            this.syncModal = new ModalManager('modalSyncSei');
        }

        const progressModalEl = document.getElementById('modalProgresso');
        if (progressModalEl) {
            this.progressModal = new ModalManager('modalProgresso');
        }

        // Inicializa máscaras monetárias
        this.moneyInputs = MoneyInput.applyToAll('.money-mask');

        // Configura filtros
        this.setupFilters();

        // Configura botão de sincronização
        this.setupSyncButton();

        // Configura linhas clicáveis
        this.setupClickableRows();
    }

    /**
     * Configura os filtros do dashboard
     */
    setupFilters() {
        // Filtro por competência
        const competenciaSelect = document.getElementById('filtroCompetencia');
        if (competenciaSelect) {
            competenciaSelect.addEventListener('change', () => this.applyFilters());
        }

        // Filtro por etapa
        const etapaSelect = document.getElementById('filtroEtapa');
        if (etapaSelect) {
            etapaSelect.addEventListener('change', () => this.applyFilters());
        }

        // Botão de limpar filtros
        const clearBtn = document.getElementById('btnLimparFiltros');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clearFilters());
        }
    }

    /**
     * Aplica os filtros selecionados
     */
    applyFilters() {
        const form = document.getElementById('formFiltros');
        if (form) {
            form.submit();
        }
    }

    /**
     * Limpa todos os filtros
     */
    clearFilters() {
        const competenciaSelect = document.getElementById('filtroCompetencia');
        const etapaSelect = document.getElementById('filtroEtapa');

        if (competenciaSelect) competenciaSelect.value = '';
        if (etapaSelect) etapaSelect.value = '';

        this.applyFilters();
    }

    /**
     * Configura o botão de sincronização SEI
     */
    setupSyncButton() {
        const syncBtn = document.getElementById('btnSyncSei');
        if (!syncBtn || !this.syncModal) return;

        syncBtn.addEventListener('click', () => this.startSync());
    }

    /**
     * Inicia o processo de sincronização
     */
    async startSync() {
        this.syncModal.reset();
        this.syncModal.show();
        this.syncModal.updateProgress(0, 'Iniciando sincronização...');

        try {
            // Fase 1: Sincronizar documentos SEI
            await this.syncPhase('/solicitacoes/api/sincronizar-documentos', 'Sincronizando documentos SEI...', 33);

            // Fase 2: Atualizar etapas
            await this.syncPhase('/solicitacoes/api/atualizar-etapas-sei', 'Atualizando etapas...', 66);

            // Fase 3: Atualizar saldos
            await this.syncPhase('/solicitacoes/api/atualizar-saldos', 'Atualizando saldos...', 100);

            this.syncModal.updateProgress(100, 'Sincronização concluída!');
            this.syncModal.addLog('Processo finalizado com sucesso', 'success');

            // Recarrega a página após 2 segundos
            setTimeout(() => {
                window.location.reload();
            }, 2000);

        } catch (error) {
            this.syncModal.addLog(`Erro: ${error.message}`, 'error');
            this.syncModal.updateProgress(0, 'Erro na sincronização');
        }
    }

    /**
     * Executa uma fase da sincronização
     */
    async syncPhase(url, message, targetProgress) {
        this.syncModal.addLog(message, 'info');

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`Erro na fase: ${response.status}`);
        }

        // Se for streaming (EventSource), processa linha por linha
        if (response.headers.get('content-type')?.includes('text/event-stream')) {
            await this.processEventStream(response, targetProgress);
        } else {
            const data = await response.json();
            this.syncModal.updateProgress(targetProgress, message);

            if (data.sucesso === false) {
                throw new Error(data.msg || 'Erro desconhecido');
            }
        }
    }

    /**
     * Processa stream de eventos
     */
    async processEventStream(response, targetProgress) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            const text = decoder.decode(value);
            const lines = text.split('\n');

            for (const line of lines) {
                if (line.startsWith('data:')) {
                    try {
                        const data = JSON.parse(line.substring(5));
                        if (data.log) {
                            this.syncModal.addLog(data.log, data.tipo || 'info');
                        }
                        if (data.progresso) {
                            this.syncModal.updateProgress(data.progresso, data.msg || '');
                        }
                    } catch (e) {
                        // Ignora linhas que não são JSON válido
                    }
                }
            }
        }

        this.syncModal.updateProgress(targetProgress);
    }

    /**
     * Configura linhas clicáveis da tabela
     */
    setupClickableRows() {
        const rows = document.querySelectorAll('.clickable-row');
        rows.forEach(row => {
            row.addEventListener('click', () => {
                const href = row.dataset.href;
                if (href) {
                    window.location.href = href;
                }
            });
        });
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.dashboardPage = new DashboardPage();
});

export default DashboardPage;
