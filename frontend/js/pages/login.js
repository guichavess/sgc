/**
 * Página: Login
 * Funcionalidades: Validação de formulário, feedback visual
 */

class LoginPage {
    constructor() {
        this.init();
    }

    init() {
        this.setupForm();
        this.setupPasswordToggle();
    }

    /**
     * Configura validação do formulário
     */
    setupForm() {
        const form = document.getElementById('formLogin');
        if (!form) return;

        form.addEventListener('submit', (e) => {
            const usuario = document.getElementById('usuario')?.value.trim();
            const senha = document.getElementById('senha')?.value;

            if (!usuario || !senha) {
                e.preventDefault();
                this.showError('Preencha todos os campos');
                return;
            }

            // Mostra loading no botão
            const btn = form.querySelector('button[type="submit"]');
            if (btn) {
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Entrando...';
            }
        });
    }

    /**
     * Configura toggle de visibilidade da senha
     */
    setupPasswordToggle() {
        const toggle = document.getElementById('toggleSenha');
        const senhaInput = document.getElementById('senha');

        if (!toggle || !senhaInput) return;

        toggle.addEventListener('click', () => {
            const type = senhaInput.type === 'password' ? 'text' : 'password';
            senhaInput.type = type;

            // Atualiza ícone
            const icon = toggle.querySelector('i');
            if (icon) {
                icon.className = type === 'password' ? 'bi bi-eye' : 'bi bi-eye-slash';
            }
        });
    }

    /**
     * Mostra mensagem de erro
     */
    showError(message) {
        const alertContainer = document.getElementById('alertContainer');
        if (!alertContainer) return;

        alertContainer.innerHTML = `
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
                <i class="bi bi-exclamation-circle me-2"></i>
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
    }
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', () => {
    window.loginPage = new LoginPage();
});

export default LoginPage;
