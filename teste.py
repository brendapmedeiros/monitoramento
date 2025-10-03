import great_expectations as gx

print("Carregando Great Expectations...")

try:
    contexto = great_expectations.get_context()
    print("Tudo certo!")
    print(f"Projeto armazenado em: {contexto.root_directory}")
    print(f"Stores configuradas: {list(contexto.list_stores())}")
    
except Exception as erro:
    print(f"ERRO: {erro}")
    