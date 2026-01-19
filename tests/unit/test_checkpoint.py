import great_expectations as gx

# Carregar contexto
context = gx.get_context(project_root_dir=".")

# Listar checkpoints disponíveis
print("Checkpoints disponíveis:")
try:
    checkpoints = context.list_checkpoints()
    print(checkpoints)
except Exception as e:
    print(f"Erro: {e}")

# Tentar obter o checkpoint
try:
    checkpoint = context.get_checkpoint("sales_checkpoint")
    print("\n Checkpoint 'sales_checkpoint' encontrado!")
    print(f"Nome: {checkpoint.name}")
except Exception as e:
    print(f"\nErro ao obter checkpoint: {e}")