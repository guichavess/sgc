/**
 * Página: Detalhes da Solicitação
 * Funcionalidades: Timeline interativa, ações
 */

class DetalhesPage {
    constructor() {
        this.init();
    }

    init() {
        this.setupTimeline();
        this.setupActions();
    }

    /**
     * Configura interações da timeline
     */
    setupTimeline() {
        const timelineItems = document.querySelectorAll('.timeline-item');

        timelineItems.forEach(item => {
            const circle = item.querySelector('.step-circle');
            const info = item.querySelector('.step-info');

            if (circle && info) {
                // Efeito hover
                item.addEventListener('mouseenter', () => {
                    if (!circle.classList.contains('current')) {
                        circle.style.transform = 'scale(1.1)';
                    }
                });

                item.addEventListener('mouseleave', () => {
                    if (!circle.classList.contains('current')) {
                        circle.style.transform = '';
                    }
                });
            }
        });
    }

    /**
     * Configura botões de ação
     */
    setupActions() {
        // Botão de aprovar documentação
        const btnAprovar = document.getElementById('btnAprovarDoc');
        if (btnAprovar) {
            btnAprovar.addEventListener('click', () => this.confirmarAcao('aprovar'));
        }

        // Botão de solicitar revisão
        const btnRevisao = document.getElementById('btnSolicitarRevisao');
        if (btnRevisao) {
            btnRevisao.addEventListener('click', () => this.confirmarAcao('revisao'));
        }

        // Botão de avançar etapa
        const btnAvancar = document.getElementById('btnAvancarEtapa');
        if (btnAvancar) {
            btnAvancar.addEventListener('click', () => this.confirmarAcao('avancar'));
        }
    }

    /**
     * Confirma uma ação antes de executar
     */
    confirmarAcao(tipo) {
        const mensagens = {
            aprovar: 'Confirma a aprovação da documentação?',
            revisao: 'Deseja solicitar revisão da documentação?',
            avancar: 'Confirma o avanço para a próxima etapa?',
        };

        if (confirm(mensagens[tipo] || 'Confirma esta ação?')) {
            this.executarAcao(tipo);
        }
    }

    /**
     * Executa a ação
     */
    async executarAcao(tipo) {
        const form = document.getElementById(`form${tipo.charAt(0).toUpperCase() + tipo.slice(1)}`);
        if (form) {
            form.submit();
        }
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.detalhesPage = new DetalhesPage();
});

export default DetalhesPage;
