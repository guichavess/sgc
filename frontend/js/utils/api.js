/**
 * Cliente API - Gerencia chamadas HTTP
 */

export class ApiClient {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl;
    }

    /**
     * Requisição GET
     * @param {string} url - Endpoint
     * @param {object} params - Query parameters
     * @returns {Promise}
     */
    async get(url, params = {}) {
        const query = new URLSearchParams(params).toString();
        const fullUrl = query ? `${this.baseUrl}${url}?${query}` : `${this.baseUrl}${url}`;

        const response = await fetch(fullUrl, {
            method: 'GET',
            headers: this.getHeaders(),
        });

        return this.handleResponse(response);
    }

    /**
     * Requisição POST
     * @param {string} url - Endpoint
     * @param {object} data - Dados a enviar
     * @returns {Promise}
     */
    async post(url, data = {}) {
        const response = await fetch(`${this.baseUrl}${url}`, {
            method: 'POST',
            headers: this.getHeaders(),
            body: JSON.stringify(data),
        });

        return this.handleResponse(response);
    }

    /**
     * Requisição POST com FormData
     * @param {string} url - Endpoint
     * @param {FormData} formData - FormData
     * @returns {Promise}
     */
    async postForm(url, formData) {
        const response = await fetch(`${this.baseUrl}${url}`, {
            method: 'POST',
            body: formData,
        });

        return this.handleResponse(response);
    }

    /**
     * Retorna headers padrão
     */
    getHeaders() {
        return {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        };
    }

    /**
     * Processa a resposta
     * @param {Response} response
     */
    async handleResponse(response) {
        if (!response.ok) {
            const error = await response.text();
            throw new Error(`HTTP ${response.status}: ${error}`);
        }

        const contentType = response.headers.get('content-type');
        if (contentType && contentType.includes('application/json')) {
            return response.json();
        }

        return response.text();
    }
}

// Instância padrão exportada
export const api = new ApiClient();

export default api;
