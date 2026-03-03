import requests
import json
import re
import os 

class SeiService:
    def __init__(self):
        
        self.BASE_URL = os.getenv("SEI_API_URL", "https://api.sei.pi.gov.br") 
        self.token = None

    # Arquivo: app/services/sei_service.py

    # ADICIONE "quantidade=10" AQUI NA DEFINIÇÃO
    def listar_documentos_procedimento(self, id_unidade, id_procedimento=None, protocolo_procedimento=None, quantidade=10):
        """
        Lista documentos de um processo.
        """
        if not self.token: return {"erro": "Token não fornecido"}
        
        if not protocolo_procedimento:
             return {"erro": "Protocolo não informado para busca."}

        endpoint = f"{self.BASE_URL}/v1/unidades/{id_unidade}/procedimentos/documentos"
        
        protocolo_limpo = "".join(filter(str.isdigit, protocolo_procedimento))
            
        params = {
            'protocolo_procedimento': protocolo_limpo, 
            'pagina': 1, 
            'quantidade': quantidade, # <--- E USE A VARIÁVEL AQUI
            'sinal_retornar_conteudo': 'N'
        }

        headers = {'token': self.token, 'Content-Type': 'application/json'}
        
        try:
            # Timeout aumentado para segurança
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200: 
                dados = response.json()
                
                vazio = False
                if isinstance(dados, list) and len(dados) == 0: vazio = True
                if isinstance(dados, dict) and not dados.get('Documentos'): vazio = True
                
                if vazio:
                    # Retry com o protocolo original se o limpo falhar
                    params['protocolo_procedimento'] = protocolo_procedimento
                    retry = requests.get(endpoint, headers=headers, params=params, timeout=30)
                    return retry.json()
                
                return dados
                
            else: 
                return {"erro": f"HTTP {response.status_code}", "detalhes": response.text}
                
        except Exception as e: 
            return {"erro": f"Erro de conexão: {str(e)}"}