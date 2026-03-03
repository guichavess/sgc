import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# O load_dotenv já é chamado no __init__.py do app, então as variáveis já estarão carregadas

def enviar_email_teste(assunto, corpo_html, lista_destinatarios):
    """
    Envia um email utilizando as credenciais configuradas no .env
    """
    # Configurações do Servidor SMTP (Gmail)
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    
    # Obter credenciais (Certifique-se que estão no .env raiz do projeto Flask)
    remetente = os.getenv("EMAIL_ADDRESS")
    senha = os.getenv("EMAIL_PASSWORD")

    if not remetente or not senha:
        print("❌ [Email] Erro: Variáveis EMAIL_ADDRESS ou EMAIL_PASSWORD não definidas.")
        return False

    if not lista_destinatarios:
        print("⚠️ [Email] Aviso: Nenhum destinatário fornecido.")
        return False

    # Construção da Mensagem
    msg = MIMEMultipart()
    msg['From'] = remetente
    msg['To'] = ", ".join(lista_destinatarios)
    msg['Subject'] = assunto

    msg.attach(MIMEText(corpo_html, 'html'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(remetente, senha)
            server.send_message(msg)
        print(f"✅ [Email] Enviado com sucesso para: {lista_destinatarios}")
        return True
    except Exception as e:
        print(f"❌ [Email] Falha ao enviar: {e}")
        return False