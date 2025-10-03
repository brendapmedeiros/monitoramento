import great_expectations as gx

print("🔍 Carregando Great Expectations com nomes personalizados...")

try:
    contexto = gx.get_context()
    print("✅ FUNCIONOU! Seus nomes personalizados estão corretos!")
    print(f"📁 Projeto em: {contexto.root_directory}")
    print(f"🏪 Stores configuradas: {list(contexto.list_stores())}")
    
except Exception as erro:
    print(f"❌ DEU ERRO: {erro}")
    print("\n💡 Hora de debugar! Lê a mensagem de erro com calma.")
    