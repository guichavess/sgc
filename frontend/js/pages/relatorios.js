/**
 * Página: Relatórios
 * Funcionalidades: Tabs, geração de PDF, impressão
 */

class RelatoriosPage {
    constructor() {
        this.activeTab = 'geral';
        this.init();
    }

    init() {
        this.setupTabs();
        this.setupPDFButton();
        this.setupPrintButton();
        this.restoreActiveTab();
    }

    /**
     * Configura navegação entre tabs
     */
    setupTabs() {
        const tabBtns = document.querySelectorAll('[data-bs-toggle="tab"]');

        tabBtns.forEach(btn => {
            btn.addEventListener('shown.bs.tab', (e) => {
                this.activeTab = e.target.getAttribute('data-bs-target')?.replace('#', '') || 'geral';
                this.saveActiveTab();
            });
        });
    }

    /**
     * Salva a tab ativa na URL
     */
    saveActiveTab() {
        const url = new URL(window.location);
        url.searchParams.set('aba_ativa', this.activeTab);
        window.history.replaceState({}, '', url);
    }

    /**
     * Restaura a tab ativa da URL
     */
    restoreActiveTab() {
        const urlParams = new URLSearchParams(window.location.search);
        const savedTab = urlParams.get('aba_ativa');

        if (savedTab) {
            const tabBtn = document.querySelector(`[data-bs-target="#${savedTab}"]`);
            if (tabBtn) {
                const tab = new bootstrap.Tab(tabBtn);
                tab.show();
                this.activeTab = savedTab;
            }
        }
    }

    /**
     * Configura botão de gerar PDF
     */
    setupPDFButton() {
        const pdfBtn = document.getElementById('btnGerarPDF');
        if (!pdfBtn) return;

        pdfBtn.addEventListener('click', async () => {
            // Verifica se html2pdf está disponível
            if (typeof html2pdf === 'undefined') {
                alert('Biblioteca de PDF não carregada');
                return;
            }

            pdfBtn.disabled = true;
            pdfBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Gerando...';

            try {
                await this.generatePDF();
            } catch (error) {
                console.error('Erro ao gerar PDF:', error);
                alert('Erro ao gerar PDF');
            } finally {
                pdfBtn.disabled = false;
                pdfBtn.innerHTML = '<i class="bi bi-file-pdf"></i> Gerar PDF';
            }
        });
    }

    /**
     * Gera o PDF da tab ativa
     */
    async generatePDF() {
        const activePane = document.querySelector('.tab-pane.active');
        if (!activePane) {
            alert('Nenhuma aba ativa encontrada');
            return;
        }

        const tabNames = {
            geral: 'Relatório Geral',
            metricas: 'Métricas e Estatísticas',
            etapas: 'Relatório por Etapas',
        };

        const title = tabNames[this.activeTab] || 'Relatório';
        const filename = `SGC_${title.replace(/\s/g, '_')}_${this.getDateString()}.pdf`;

        const options = {
            margin: 10,
            filename: filename,
            image: { type: 'jpeg', quality: 0.98 },
            html2canvas: { scale: 2, useCORS: true },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'landscape' },
        };

        // Prepara elemento para impressão
        const clone = activePane.cloneNode(true);
        clone.style.width = '100%';
        clone.style.padding = '20px';

        // Remove elementos não imprimíveis
        clone.querySelectorAll('.no-print, button').forEach(el => el.remove());

        await html2pdf().set(options).from(clone).save();
    }

    /**
     * Configura botão de impressão
     */
    setupPrintButton() {
        const printBtn = document.getElementById('btnImprimir');
        if (!printBtn) return;

        printBtn.addEventListener('click', () => {
            // Abre página de impressão em nova aba
            const params = new URLSearchParams(window.location.search);
            params.set('aba_ativa', this.activeTab);

            window.open(`/solicitacoes/relatorios/imprimir?${params.toString()}`, '_blank');
        });
    }

    /**
     * Retorna data formatada para nome de arquivo
     */
    getDateString() {
        const now = new Date();
        return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.relatoriosPage = new RelatoriosPage();
});

export default RelatoriosPage;
