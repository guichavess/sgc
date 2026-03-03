"""
Service para comunicação com a API do Gestor SEAD (pessoaSGA).
Busca dados de servidores por CPF na base do SGA.
"""
import requests
import urllib3
from flask import current_app

# Desabilita warnings de SSL (certificado autoassinado, mesmo padrão SIAFE)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SGAService:
    """Serviço de integração com a API pessoaSGA do Gestor SEAD."""

    @staticmethod
    def buscar_pessoa_por_cpf(cpf):
        """
        Busca dados de um servidor pelo CPF na API pessoaSGA.

        Args:
            cpf (str): CPF do servidor (11 dígitos, sem formatação).

        Returns:
            dict: Dados do servidor com campos:
                - matricula, cpf, nome, cargo, setor, orgao,
                - superintendencia, banco_agencia, banco_conta,
                - vinculo, cod_sefaz_orgao
            None: Se o servidor não for encontrado ou em caso de erro.
        """
        # Limpa CPF (remove pontos, traços, espaços)
        cpf_limpo = ''.join(c for c in cpf if c.isdigit())

        if len(cpf_limpo) != 11:
            return None

        url = current_app.config.get('SGA_API_URL', 'https://gestor.sead.pi.gov.br/api/pessoaSGA')
        hashkey = current_app.config.get('SGA_API_HASHKEY', '')

        if not hashkey:
            current_app.logger.error('[SGA] Hashkey não configurada.')
            return None

        headers = {'hashkey': hashkey}
        payload = {'cpf': cpf_limpo}

        try:
            response = requests.post(
                url,
                headers=headers,
                data=payload,
                verify=False,
                timeout=15,
            )
            response.raise_for_status()

            data = response.json()

            # API retorna [] quando não encontra
            if not data or (isinstance(data, list) and len(data) == 0):
                return None

            # API retorna objeto direto quando encontra
            if isinstance(data, dict):
                return {
                    'matricula': data.get('matricula', ''),
                    'cpf': data.get('cpf', cpf_limpo),
                    'nome': data.get('nome', ''),
                    'cargo': data.get('cargo', ''),
                    'setor': data.get('setor', ''),
                    'orgao': data.get('orgao', ''),
                    'superintendencia': data.get('superintendencia', ''),
                    'banco_agencia': data.get('banco_agencia', ''),
                    'banco_conta': data.get('banco_conta', ''),
                    'vinculo': data.get('vinculo') or '',
                    'cod_sefaz_orgao': data.get('cod_sefaz_orgao'),
                }

            # Se retornou lista com dados (caso diferente do esperado)
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                return {
                    'matricula': item.get('matricula', ''),
                    'cpf': item.get('cpf', cpf_limpo),
                    'nome': item.get('nome', ''),
                    'cargo': item.get('cargo', ''),
                    'setor': item.get('setor', ''),
                    'orgao': item.get('orgao', ''),
                    'superintendencia': item.get('superintendencia', ''),
                    'banco_agencia': item.get('banco_agencia', ''),
                    'banco_conta': item.get('banco_conta', ''),
                    'vinculo': item.get('vinculo') or '',
                    'cod_sefaz_orgao': item.get('cod_sefaz_orgao'),
                }

            return None

        except requests.exceptions.Timeout:
            current_app.logger.error('[SGA] Timeout ao consultar API pessoaSGA.')
            return None
        except requests.exceptions.ConnectionError:
            current_app.logger.error('[SGA] Erro de conexão com API pessoaSGA.')
            return None
        except requests.exceptions.RequestException as e:
            current_app.logger.error(f'[SGA] Erro na requisição: {e}')
            return None
        except (ValueError, KeyError) as e:
            current_app.logger.error(f'[SGA] Erro ao processar resposta: {e}')
            return None
