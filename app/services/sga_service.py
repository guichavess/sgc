"""
Service para comunicação com a API do Gestor SEAD (pessoaSGA).
Busca dados de servidores por CPF na base do SGA.
"""
import requests
import urllib3
from flask import current_app

from app.utils.cpf import formatar_cpf

# Desabilita warnings de SSL (certificado autoassinado, mesmo padrão SIAFE)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SGAService:
    """Serviço de integração com a API pessoaSGA do Gestor SEAD."""

    @staticmethod
    def buscar_pessoa_por_cpf(cpf):
        """
        Busca dados de um servidor pelo CPF na API pessoaSGA.

        Args:
            cpf (str): CPF do servidor, com ou sem formatação.

        Returns:
            dict: Dados do servidor com campos:
                - matricula, cpf, nome, cargo, setor, orgao,
                - superintendencia, banco_agencia, banco_conta,
                - vinculo, cod_sefaz_orgao
            None: Se o servidor não for encontrado ou em caso de erro.
        """
        # A API pessoaSGA só responde com o CPF FORMATADO ('999.999.999-99').
        # Enviando apenas dígitos ela devolve HTTP 200 com corpo '[]', o que é
        # indistinguível de "não encontrado" — o erro passa silencioso.
        cpf_formatado = formatar_cpf(cpf)
        if not cpf_formatado:
            return None

        url = current_app.config.get('SGA_API_URL', 'https://gestor.sead.pi.gov.br/api/pessoaSGA')
        hashkey = current_app.config.get('SGA_API_HASHKEY', '')

        if not hashkey:
            current_app.logger.error('[SGA] Hashkey não configurada.')
            return None

        headers = {'hashkey': hashkey}
        payload = {'cpf': cpf_formatado}

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
                current_app.logger.info(
                    f'[SGA] Nenhuma pessoa retornada para o CPF {cpf_formatado}.'
                )
                return None

            # API retorna objeto direto quando encontra
            if isinstance(data, dict):
                return {
                    'matricula': data.get('matricula') or '',
                    'cpf': data.get('cpf') or cpf_formatado,
                    'nome': data.get('nome') or '',
                    'cargo': data.get('cargo') or '',
                    'setor': data.get('setor') or '',
                    'orgao': data.get('orgao') or '',
                    'superintendencia': data.get('superintendencia') or '',
                    'banco_agencia': data.get('banco_agencia') or '',
                    'banco_conta': data.get('banco_conta') or '',
                    'vinculo': data.get('vinculo') or '',
                    'cod_sefaz_orgao': data.get('cod_sefaz_orgao'),
                }

            # Se retornou lista com dados (caso diferente do esperado)
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                return {
                    'matricula': item.get('matricula') or '',
                    'cpf': item.get('cpf') or cpf_formatado,
                    'nome': item.get('nome') or '',
                    'cargo': item.get('cargo') or '',
                    'setor': item.get('setor') or '',
                    'orgao': item.get('orgao') or '',
                    'superintendencia': item.get('superintendencia') or '',
                    'banco_agencia': item.get('banco_agencia') or '',
                    'banco_conta': item.get('banco_conta') or '',
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
