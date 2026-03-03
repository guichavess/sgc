from app import create_app, db
from sqlalchemy import text

app = create_app()

def testar_conexao():
    """Tenta conectar ao banco antes de subir o servidor"""
    try:
        with app.app_context():
            db.session.execute(text('SELECT 1'))
            print("[OK] Conexao com o Banco de Dados: OK")
    except Exception as e:
        print(f"[ERRO] Erro na conexao com o Banco: {e}")
        print("   Verifique suas credenciais no arquivo .env ou __init__.py")

if __name__ == '__main__':
    testar_conexao()
    # Debug=True permite que o site recarregue quando você altera o código
    app.run(host='0.0.0.0', port=5000, debug=True)