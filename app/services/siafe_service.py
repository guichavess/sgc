import requests
import json
import urllib3
import os  # <--- Adicione esta importação

# Suprime avisos de "InsecureRequestWarning"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configurações via .env ---
SIAFE_AUTH_URL = 'https://tesouro.sefaz.pi.gov.br/siafe-api/auth'
SIAFE_USER = os.getenv("SIAFE_USER")  # <--- Alterado
SIAFE_PASS = os.getenv("SIAFE_PASS")  # <--- Alterado
CODIGO_UG = "210101" # Se quiser, pode colocar isso no .env também (ex: SIAFE_COD_UG)

def get_siafe_token():
    """
    Realiza a autenticação na API do SIAFE e retorna o token Bearer.
    """
    
    if not SIAFE_USER or not SIAFE_PASS:
        print("❌ [SIAFE Service] Erro: Credenciais não encontradas no .env")
        return None
        
    print(f"🔄 [SIAFE Service] Tentando autenticar usuário {SIAFE_USER}...")
    
    credenciais = {
        "usuario": SIAFE_USER,
        "senha": SIAFE_PASS
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Python/SGC-Sistema" # Ajuda a não ser bloqueado por firewall
    }

    try:
        # verify=False pula a checagem do certificado SSL (Crucial para intranet/governo)
        response = requests.post(
            SIAFE_AUTH_URL, 
            json=credenciais, 
            headers=headers,
            timeout=30,
            verify=False 
        )

        if response.status_code == 200:
            dados = response.json()
            token = dados.get('token')
            if token:
                print("✅ [SIAFE Service] Token obtido com sucesso.")
                return token
            else:
                print("⚠️ [SIAFE Service] Autenticação 200 OK, mas sem token na resposta.")
                return None
        else:
            print(f"❌ [SIAFE Service] Erro Auth: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"❌ [SIAFE Service] Erro de Conexão na Autenticação: {e}")
        return None

def validar_ne_siafe(ne_digitada, contrato_sistema):
    """
    Valida a NE na API do SIAFE e verifica se o contrato vinculado corresponde ao do sistema.
    """
    print(f"🚀 [SIAFE Service] Iniciando validação da NE: {ne_digitada}")

    # 1. Validação básica de formato
    if not ne_digitada or len(ne_digitada) < 4:
        return {'sucesso': False, 'mensagem': 'NE inválida: Formato incorreto.', 'categoria': 'warning'}

    try:
        # 2. Obter Token
        token = get_siafe_token()
        if not token:
            return {'sucesso': False, 'mensagem': 'Erro de conexão com SIAFE (Falha no Login). Verifique o terminal do servidor.', 'categoria': 'danger'}

        # 3. Preparar Requisição
        exercicio = ne_digitada[:4]
        url_consulta = f"https://tesouro.sefaz.pi.gov.br/siafe-api/nota-empenho/{exercicio}"
        
        payload = { "codigo": ne_digitada, "codigoUG": CODIGO_UG }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # 4. Consultar API
        print(f"🔍 [SIAFE Service] Consultando detalhes da NE...")
        response = requests.post(url_consulta, json=payload, headers=headers, timeout=30, verify=False)

        if response.status_code == 200:
            dados_api = response.json()
            
            contrato_api = str(dados_api.get('codContrato', '',)).strip()
            contrato_sistema = str(contrato_sistema).strip()
            
            # --- NOVO: Captura o Nome do Credor ---
            nome_credor = dados_api.get('nomeCredor', 'Credor não informado')
            
            print(f"📊 [SIAFE Service] Comparação: SIAFE '{contrato_api}' vs Sistema '{contrato_sistema}'")

            if not contrato_api:
                return {'sucesso': False, 'mensagem': f'Atenção: SIAFE retornou a NE {ne_digitada}, mas sem contrato vinculado.', 'categoria': 'warning'}

            if contrato_api != contrato_sistema:
                return {'sucesso': False, 'mensagem': f'DIVERGÊNCIA: A NE {ne_digitada} pertence ao contrato {contrato_api}, mas a solicitação é do contrato {contrato_sistema}.', 'categoria': 'danger'}

            # --- ATUALIZADO: Inclui o nome_credor na mensagem de sucesso ---
            return {
                'sucesso': True, 
                'mensagem': f'NE {ne_digitada} validada com sucesso! Contratante: {nome_credor}', 
                'categoria': 'success'
            }

        elif response.status_code == 404:
            print(f"⚠️ [SIAFE Service] NE não encontrada (404).")
            return {'sucesso': False, 'mensagem': f'NE {ne_digitada} não encontrada no SIAFE.', 'categoria': 'warning'}
        else:
            print(f"❌ [SIAFE Service] Erro API Consulta: {response.status_code}")
            return {'sucesso': False, 'mensagem': f'Erro na consulta SIAFE. Código: {response.status_code}.', 'categoria': 'danger'}

    except requests.exceptions.RequestException as e:
        print(f"❌ [SIAFE Service] Erro de Conexão na Consulta: {e}")
        return {'sucesso': False, 'mensagem': 'Falha de comunicação com o SIAFE. Tente novamente.', 'categoria': 'danger'}
    except Exception as e:
        print(f"❌ [SIAFE Service] Erro Interno: {e}")
        return {'sucesso': False, 'mensagem': 'Erro interno ao validar NE.', 'categoria': 'danger'}