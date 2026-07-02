"""Integração com o Gateway DETRAN-SEAD (consulta de veículos por placa).

Usado pelo módulo Identidade Visual para preencher automaticamente os dados de
um veículo (tipo, marca/modelo e cor) a partir da placa. A chave da API
(`X-Api-Key`) NUNCA vai para o front-end — toda consulta passa por aqui, no
servidor, e só os campos públicos são devolvidos (o endpoint retorna também
dados pessoais do proprietário, que são descartados).
"""
import re
import requests
import urllib3
from flask import current_app

# O gateway usa certificado interno — mesma postura das demais integrações (SEI/SIAFE).
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT = 30
MAX_RETRIES = 3


class DetranError(Exception):
    """Erro de negócio na consulta de placa (placa inválida, não configurado, etc.)."""


def normalizar_placa(placa):
    """Remove tudo que não for letra/número e devolve em maiúsculas.

    Aceita tanto placa antiga (AAA1234) quanto Mercosul (AAA1A23) — ambas com
    7 caracteres alfanuméricos.
    """
    return re.sub(r'[^A-Za-z0-9]', '', placa or '').upper()


def _descricao(node):
    """Extrai a melhor descrição de um nó {id, descricao, descricaoCrv}."""
    if not isinstance(node, dict):
        return None
    valor = (node.get('descricao') or node.get('descricaoCrv') or '').strip()
    return valor or None


def _cor(node):
    """A cor costuma vir só em `descricaoCrv` (descricao=None) — prioriza-a."""
    if not isinstance(node, dict):
        return None
    valor = (node.get('descricaoCrv') or node.get('descricao') or '').strip()
    return valor or None


def consultar_veiculo(placa):
    """Consulta a placa no gateway DETRAN-SEAD.

    Retorna um dict `{placa, tipo_veiculo, marca_modelo, cor}` apenas com os
    campos públicos. Levanta `DetranError` para erros de negócio (placa inválida,
    integração não configurada, veículo não encontrado) e `requests`/`Exception`
    para falhas de rede/servidor (o chamador decide o status HTTP).
    """
    placa_norm = normalizar_placa(placa)
    if len(placa_norm) != 7:
        raise DetranError('Placa inválida — informe 7 caracteres (ex.: RSK6J04)')

    base = (current_app.config.get('DETRAN_API_URL') or '').rstrip('/')
    key = current_app.config.get('DETRAN_API_KEY') or ''
    if not base or not key:
        raise DetranError('Integração DETRAN não configurada')

    url = f'{base}/sead/renavam/consulta-veiculo-local'
    headers = {'Accept': 'application/json;charset=utf-8', 'X-Api-Key': key}

    ultimo_erro = None
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params={'placa': placa_norm},
                                verify=False, timeout=TIMEOUT)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            ultimo_erro = e
            current_app.logger.warning(
                f'[IDENTIDADE_VISUAL] Consulta placa {placa_norm} falhou '
                f'(tentativa {tentativa}/{MAX_RETRIES}): {e}')
    else:
        raise ultimo_erro if ultimo_erro else RuntimeError('Falha na consulta DETRAN')

    payload = resp.json() or {}
    data = payload.get('data') or {}
    if not data:
        raise DetranError('Veículo não encontrado para essa placa')

    return {
        'placa': placa_norm,
        'tipo_veiculo': _descricao(data.get('tipoVeiculo')),
        'marca_modelo': _descricao(data.get('marcaModelo')),
        'cor': _cor(data.get('cor')),
    }
