import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from pathlib import Path
import yaml


def inicia_great_expectations(project_root='.'):

    project_path = Path(project_root).absolute()
    ge_dir = project_path / 'great_expectations'
    
    print("Inicializando...")
    
    
    if not ge_dir.exists():
        context = gx.get_context(project_root_dir=str(project_path))
        print(f"Great Expectations inicializado em: {ge_dir}")
    else:
        context = gx.get_context(project_root_dir=str(project_path))
        print(f"Great Expectations já instalado  em: {ge_dir}")
    
    return context


def config_datasources(context, data_dir='data'):
   
    data_path = Path(data_dir).absolute()
    
    print("\n Configurando...")
    
    # Configuração para dados de vendas
    datasource_config_sales = {
        "name": "sales_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "default_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": str(data_path),
                "default_regex": {
                    "group_names": ["data_asset_name"],
                    "pattern": "(sales_.*)\\.csv"
                }
            }
        }
    }
    
    # Configuração para dados de atividade
    datasource_config_activity = {
        "name": "activity_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "default_inferred_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": str(data_path),
                "default_regex": {
                    "group_names": ["data_asset_name"],
                    "pattern": "(user_activity)\\.csv"
                }
            }
        }
    }
    
    try:
        # Adiciona datasource de vendas
        context.add_datasource(**datasource_config_sales)
        print("Datasource 'sales_datasource' configurado")
    except Exception as e:
        if "already exists" in str(e):
            print(" Datasource 'sales_datasource' já existe")
        else:
            print(f"Erro ao configurar sales_datasource: {e}")
    
    try:
        # Adiciona datasource de atividade
        context.add_datasource(**datasource_config_activity)
        print("Datasource 'activity_datasource' configurado")
    except Exception as e:
        if "already exists" in str(e):
            print("Datasource 'activity_datasource' já existe")
        else:
            print(f"Erro ao configurar activity_datasource: {e}")
    
    # Lista datasources disponíveis
    print("\n Datasources disponíveis:")
    for ds_name in context.list_datasources():
        print(f"   - {ds_name['name']}")
    
    return context


def create_expectation_suite(context, suite_name="sales_quality_suite"):
    """
    Cria a primeira Expectation Suite para validação de dados de vendas
    """
    print(f"\n Criando Expectation Suite: {suite_name}")
    
    try:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
        print(f" Suite '{suite_name}' criada com sucesso")
    except Exception as e:
        if "already exists" in str(e):
            suite = context.get_expectation_suite(expectation_suite_name=suite_name)
            print(f"  Suite '{suite_name}' já existe, usando existente")
        else:
            raise e
    
    return suite


def add_expectations_to_suite(context, suite_name="sales_quality_suite"):
  
    # adiciona expectations na suite de validação
 
    print(f"\n Adicionando expectations na suite '{suite_name}'.")
    
    # batch para criar expectations
    batch_request = BatchRequest(
        datasource_name="sales_datasource",
        data_connector_name="default_inferred_data_connector",
        data_asset_name="sales_transactions"
    )
        
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
        
    expectations_added = []
    
    # colunas esperadas
    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            'transaction_id', 'date', 'customer_id', 'product_id',
            'quantity', 'unit_price', 'region', 'payment_method',
            'total_amount', 'status', 'customer_email'
        ]
    )
    expectations_added.append("Colunas esperadas")
    
    # nenhum campo vazio na coluna transaction_id
    validator.expect_column_values_to_not_be_null(column="transaction_id")
    expectations_added.append("transaction_id não-nulo")
    
    # transaction_id de valor unico
    validator.expect_column_values_to_be_unique(column="transaction_id")
    expectations_added.append("transaction_id único")
    
    # valores de quantidade entre 1 e 100
    validator.expect_column_values_to_be_between(
        column="quantity",
        min_value=1,
        max_value=100
    )
    expectations_added.append("quantity entre 1 e 100")
    
    # unit_price positivo
    validator.expect_column_values_to_be_between(
        column="unit_price",
        min_value=0,
        max_value=10000
    )
    expectations_added.append("unit_price positivo")
    
    # status deve ter valores válidos
    validator.expect_column_values_to_be_in_set(
        column="status",
        value_set=['completed', 'pending', 'cancelled']
    )
    expectations_added.append("status em conjunto válido")
    
    # region deve conter valores dentre os listados
    validator.expect_column_values_to_be_in_set(
        column="region",
        value_set=['North', 'South', 'East', 'West']
    )
    expectations_added.append("region em conjunto válido")
    
    #customer_email precisa estar no padrão
    validator.expect_column_values_to_match_regex(
        column="customer_email",
        regex=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )
    expectations_added.append("customer_email formato válido")
    
    # precisa de pelo menos 80%  das transações completas
    validator.expect_column_values_to_be_in_set(
        column="status",
        value_set=['completed'],
        mostly=0.8
    )
    expectations_added.append("80% das transações completas")
    
    # Salva suite
    validator.save_expectation_suite(discard_failed_expectations=False)
    
    print(f" {len(expectations_added)} regras adicionadas:")
    for exp in expectations_added:
        print(f"   ✓ {exp}")
    
    return validator


def create_checkpoint(context, checkpoint_name="sales_checkpoint"):
    """
    Cria um checkpoint para executar validações
    """
    print(f"\n Criando checkpoint: {checkpoint_name}")
    
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-sales-validation",
        "validations": [
    {
        "batch_request": BatchRequest(
            datasource_name="sales_datasource",
            data_connector_name="default_inferred_data_connector",
            data_asset_name="sales_transactions"
        ),
                "expectation_suite_name": "sales_quality_suite"
            }
        ]
    }
    
    try:
        context.add_checkpoint(**checkpoint_config)
        print(f"Checkpoint '{checkpoint_name}' criado com sucesso")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Checkpoint '{checkpoint_name}' já existe")
        else:
            print(f"Erro ao criar checkpoint: {e}")


def run_checkpoint(context, checkpoint_name="sales_checkpoint"):
    """
    Executa o checkpoint e mostra resultados
    """
    print(f"\n  Executando: {checkpoint_name}")
    
    results = context.run_checkpoint(checkpoint_name=checkpoint_name)
    
    print("\n Resultados de validação:")
    print(f"   Status: {'Validado' if results.success else 'Inválido'}")
    print(f"   Validações executadas: {len(results.run_results)}")
    
    # Detalhamento de resultados
    for result_id, result in results.run_results.items():
        validation_result = result['validation_result']
        print(f"\n   Expectativas avaliadas: {validation_result.statistics['evaluated_expectations']}")
        print(f"   Expectativas bem-sucedidas: {validation_result.statistics['successful_expectations']}")
        print(f"   Taxa de sucesso: {validation_result.statistics['success_percent']:.2f}%")
    
    return results


def main():
    """
    Executa o setup
    """
    print("=" * 60)
    print("Setup Great Expectations")
    print("=" * 60)
    
    # Inicializa GE e configura datasource
    context = inicia_great_expectations()
    
    context = config_datasources(context)
    
    suite = create_expectation_suite(context)
    
    validator = add_expectations_to_suite(context)
    
    create_checkpoint(context)
    
    # Executa o checkpoint de teste
    results = run_checkpoint(context)
    
    print("\n" + "=" * 60)
    print("Setup Completo")
    print("=" * 60)
    print("\n Estrutura criada:")
    print("   - great_expectations/")
    print("   - Datasources configurados")
    print("   - Expectation Suite criada")
    print("   - Checkpoint configurado")

    print("\n   - Revisar resultados em great_expectations/uncommitted/")
  


if __name__ == '__main__':
    main()