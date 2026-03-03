/**
 * Página: Nova Solicitação
 * Funcionalidades: Busca de contrato, máscara de competência, assinatura
 */

import { api } from '../utils/api.js';
import { formatMoney } from '../utils/formatters.js';
import { ModalManager } from '../components/Modal.js';

class NovaSolicitacaoPage {
    constructor() {
        this.contratoSelecionado = null;
        this.signatureModal = null;

        this.init();
    }

    init() {
        // Inicializa modal de assinatura
        const modalEl = document.getElementById('modalAssinatura');
        if (modalEl) {
            this.signatureModal = new ModalManager('modalAssinatura');
        }

        // Configura busca de contrato
        this.setupContratoBusca();

        // Configura máscara de competência
        this.setupCompetenciaMask();

        // Configura formulário
        this.setupForm();
    }

    /**
     * Configura a busca de contrato
     */
    setupContratoBusca() {
        const searchInput = document.getElementById('buscaContrato');
        const resultsContainer = document.getElementById('resultadosContrato');
        const contratoInput = document.getElementById('codigoContrato');
        const infoContainer = document.getElementById('infoContrato');

        if (!searchInput || !resultsContainer) return;

        let debounceTimer;

        searchInput.addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();

            if (query.length < 3) {
                resultsContainer.innerHTML = '';
                resultsContainer.style.display = 'none';
                return;
            }

            debounceTimer = setTimeout(async () => {
                try {
                    const response = await fetch(`/solicitacoes/api/buscar-contrato?q=${encodeURIComponent(query)}`);
                    const data = await response.json();

                    this.renderContratoResults(data.contratos || [], resultsContainer);
                } catch (error) {
                    console.error('Erro ao buscar contratos:', error);
                }
            }, 300);
        });

        // Fecha resultados ao clicar fora
        document.addEventListener('click', (e) => {
            if (!e.target.closest('#buscaContrato') && !e.target.closest('#resultadosContrato')) {
                resultsContainer.style.display = 'none';
            }
        });
    }

    /**
     * Renderiza os resultados da busca de contrato
     */
    renderContratoResults(contratos, container) {
        if (contratos.length === 0) {
            container.innerHTML = '<div class="p-2 text-muted">Nenhum contrato encontrado</div>';
            container.style.display = 'block';
            return;
        }

        container.innerHTML = contratos.map(c => `
            <div class="contrato-item p-2 border-bottom"
                 data-codigo="${c.codigo}"
                 data-nome="${c.nomeContratado}"
                 data-objeto="${c.objeto || ''}">
                <strong>${c.codigo}</strong> - ${c.nomeContratadoResumido || c.nomeContratado}
                <br>
                <small class="text-muted">${this.truncate(c.objeto, 100)}</small>
            </div>
        `).join('');

        container.style.display = 'block';

        // Adiciona evento de clique nos itens
        container.querySelectorAll('.contrato-item').forEach(item => {
            item.addEventListener('click', () => this.selectContrato(item));
            item.style.cursor = 'pointer';
        });
    }

    /**
     * Seleciona um contrato
     */
    selectContrato(item) {
        const codigo = item.dataset.codigo;
        const nome = item.dataset.nome;
        const objeto = item.dataset.objeto;

        this.contratoSelecionado = { codigo, nome, objeto };

        // Atualiza inputs
        const contratoInput = document.getElementById('codigoContrato');
        const buscaInput = document.getElementById('buscaContrato');
        const infoContainer = document.getElementById('infoContrato');

        if (contratoInput) contratoInput.value = codigo;
        if (buscaInput) buscaInput.value = `${codigo} - ${nome}`;

        if (infoContainer) {
            infoContainer.innerHTML = `
                <div class="alert alert-info">
                    <strong>Contrato selecionado:</strong> ${codigo}<br>
                    <strong>Contratado:</strong> ${nome}<br>
                    <small>${objeto}</small>
                </div>
            `;
        }

        // Esconde resultados
        document.getElementById('resultadosContrato').style.display = 'none';
    }

    /**
     * Configura máscara de competência (MM/YYYY)
     */
    setupCompetenciaMask() {
        const competenciaInput = document.getElementById('competencia');
        if (!competenciaInput) return;

        competenciaInput.addEventListener('input', (e) => {
            let value = e.target.value.replace(/\D/g, '');

            if (value.length > 6) {
                value = value.substring(0, 6);
            }

            if (value.length >= 2) {
                value = value.substring(0, 2) + '/' + value.substring(2);
            }

            e.target.value = value;
        });
    }

    /**
     * Configura o formulário
     */
    setupForm() {
        const form = document.getElementById('formNovaSolicitacao');
        if (!form) return;

        form.addEventListener('submit', async (e) => {
            e.preventDefault();

            if (!this.validateForm()) {
                return;
            }

            // Mostra modal de assinatura
            if (this.signatureModal) {
                this.signatureModal.show();
            } else {
                // Se não tem modal, submete diretamente
                form.submit();
            }
        });

        // Botão de confirmar assinatura
        const confirmBtn = document.getElementById('btnConfirmarAssinatura');
        if (confirmBtn) {
            confirmBtn.addEventListener('click', () => this.submitWithSignature());
        }
    }

    /**
     * Valida o formulário
     */
    validateForm() {
        const codigoContrato = document.getElementById('codigoContrato')?.value;
        const competencia = document.getElementById('competencia')?.value;

        if (!codigoContrato) {
            alert('Selecione um contrato');
            return false;
        }

        if (!competencia || competencia.length < 7) {
            alert('Informe a competência no formato MM/AAAA');
            return false;
        }

        return true;
    }

    /**
     * Submete com assinatura
     */
    async submitWithSignature() {
        const senha = document.getElementById('senhaAssinatura')?.value;
        const errorEl = document.getElementById('erroAssinatura');

        if (!senha) {
            if (errorEl) errorEl.textContent = 'Digite sua senha';
            return;
        }

        const confirmBtn = document.getElementById('btnConfirmarAssinatura');
        if (confirmBtn) {
            confirmBtn.disabled = true;
            confirmBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Processando...';
        }

        try {
            const form = document.getElementById('formNovaSolicitacao');
            const formData = new FormData(form);
            formData.append('senha_assinatura', senha);

            const response = await fetch(form.action, {
                method: 'POST',
                body: formData,
            });

            const data = await response.json();

            if (data.sucesso) {
                window.location.href = data.redirect || '/solicitacoes/dashboard';
            } else {
                if (errorEl) errorEl.textContent = data.msg || 'Erro ao processar';
                if (confirmBtn) {
                    confirmBtn.disabled = false;
                    confirmBtn.innerHTML = 'Confirmar';
                }
            }
        } catch (error) {
            if (errorEl) errorEl.textContent = 'Erro de conexão';
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.innerHTML = 'Confirmar';
            }
        }
    }

    /**
     * Trunca texto
     */
    truncate(text, maxLength) {
        if (!text || text.length <= maxLength) return text || '';
        return text.substring(0, maxLength) + '...';
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.novaSolicitacaoPage = new NovaSolicitacaoPage();
});

export default NovaSolicitacaoPage;
