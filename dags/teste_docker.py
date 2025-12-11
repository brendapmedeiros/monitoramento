from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'teste_docker_airflow',
    default_args=default_args,
    description='DAG de teste para Docker',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['teste', 'docker'],
)


def teste_python():
    """Testa Python e Airflow"""
    print("=" * 50)
    print("Airflow executando.")
    print("=" * 50)
    print(" Python executando.")
    print(f"Data/Hora: {datetime.now()}")
    
    import sys
    print(f"Python Ver.: {sys.version}")
    
    return "Tudo certo!"


def teste_imports():
    """Testa se os pacotes estão ok"""
    print(" Testando imports...")
    
    try:
        import pandas as pd
        print("Pandas OK")
        
        import numpy as np
        print("NumPy OK")
        
        from sklearn.ensemble import IsolationForest
        print("Scikit-learn OK")
        
        print("\nTodos os pacotes funcionando!")
        return True
        
    except Exception as e:
        print(f" Erro: {e}")
        return False


def teste_volumes():
    """Testa se os volumes estão ok"""
    print("Testando acesso...")
    
    import os
    
    dirs = ['/opt/airflow/dags', '/opt/airflow/logs', '/opt/airflow/src']
    
    for dir_path in dirs:
        if os.path.exists(dir_path):
            print(f" {dir_path} - OK")
        else:
            print(f" {dir_path} - NÃO ENCONTRADO")
    
    return "Volumes OK"


# Tarefas
task_hello = BashOperator(
    task_id='hello_docker',
    bash_command='echo " Docker on!"',
    dag=dag,
)

task_python = PythonOperator(
    task_id='teste_python',
    python_callable=teste_python,
    dag=dag,
)

task_imports = PythonOperator(
    task_id='teste_imports',
    python_callable=teste_imports,
    dag=dag,
)

task_volumes = PythonOperator(
    task_id='teste_volumes',
    python_callable=teste_volumes,
    dag=dag,
)

# Ordem de execução
task_hello > task_python > task_imports > task_volumes