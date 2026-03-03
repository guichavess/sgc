/**
 * Página: Pendências de NE
 * Funcionalidades: Filtros de dropdown, inserção de NE
 */

class PendenciasPage {
    constructor() {
        this.init();
    }

    init() {
        this.setupFilters();
        this.setupNEInputs();
    }

    /**
     * Configura filtros de dropdown
     */
    setupFilters() {
        const searchInput = document.getElementById('searchContratado');
        const dropdownItems = document.querySelectorAll('.dropdown-item-contratado');
        const dropdownLabel = document.getElementById('dropdownContratadoLabel');

        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                const query = e.target.value.toLowerCase();

                dropdownItems.forEach(item => {
                    const text = item.textContent.toLowerCase();
                    item.style.display = text.includes(query) ? '' : 'none';
                });
            });
        }

        // Clique nos itens do dropdown
        dropdownItems.forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                const value = item.dataset.value;
                const text = item.textContent;

                if (dropdownLabel) {
                    dropdownLabel.textContent = text || 'Todos os Contratados';
                }

                // Aplica o filtro
                const urlParams = new URLSearchParams(window.location.search);
                if (value) {
                    urlParams.set('contratado', value);
                } else {
                    urlParams.delete('contratado');
                }

                window.location.search = urlParams.toString();
            });
        });

        // Botão limpar
        const clearBtn = document.getElementById('btnLimparFiltro');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                const urlParams = new URLSearchParams(window.location.search);
                urlParams.delete('contratado');
                window.location.search = urlParams.toString();
            });
        }
    }

    /**
     * Configura inputs de NE
     */
    setupNEInputs() {
        const neInputs = document.querySelectorAll('.input-ne');

        neInputs.forEach(input => {
            // Máscara para NE (apenas números)
            input.addEventListener('input', (e) => {
                e.target.value = e.target.value.replace(/\D/g, '');
            });

            // Validação mínima
            input.addEventListener('blur', (e) => {
                const value = e.target.value;
                if (value && value.length < 4) {
                    e.target.classList.add('is-invalid');
                } else {
                    e.target.classList.remove('is-invalid');
                }
            });
        });

        // Botões de salvar NE
        const saveBtns = document.querySelectorAll('.btn-salvar-ne');
        saveBtns.forEach(btn => {
            btn.addEventListener('click', async (e) => {
                const row = e.target.closest('tr');
                const input = row?.querySelector('.input-ne');
                const solicitacaoId = row?.dataset.solicitacaoId;

                if (!input || !solicitacaoId) return;

                const ne = input.value.trim();
                if (!ne || ne.length < 4) {
                    alert('NE deve ter pelo menos 4 dígitos');
                    return;
                }

                await this.salvarNE(solicitacaoId, ne, btn);
            });
        });
    }

    /**
     * Salva a NE
     */
    async salvarNE(solicitacaoId, ne, btn) {
        const originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

        try {
            const response = await fetch('/solicitacoes/api/salvar-ne', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    solicitacao_id: solicitacaoId,
                    ne: ne,
                }),
            });

            const data = await response.json();

            if (data.sucesso) {
                btn.innerHTML = '<i class="bi bi-check"></i>';
                btn.classList.remove('btn-primary');
                btn.classList.add('btn-success');

                // Remove a linha após 1 segundo
                setTimeout(() => {
                    btn.closest('tr')?.remove();
                }, 1000);
            } else {
                alert(data.msg || 'Erro ao salvar');
                btn.disabled = false;
                btn.innerHTML = originalText;
            }
        } catch (error) {
            alert('Erro de conexão');
            btn.disabled = false;
            btn.innerHTML = originalText;
        }
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.pendenciasPage = new PendenciasPage();
});

export default PendenciasPage;
