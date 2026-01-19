import pytest
import pandas as pd
import yaml
from pathlib import Path


@pytest.fixture
def sample_dataframe():
    # dados limpos para testes básicos
    return pd.DataFrame({
        'id': range(1, 11),
        'value': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        'category': ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A'],
        'date': pd.date_range('2024-01-01', periods=10)
    })


@pytest.fixture
def dataframe_with_nulls():
   #valores nulos para teste de completude
    return pd.DataFrame({
        'col1': [1, 2, None, 4, 5],
        'col2': ['a', None, 'c', 'd', None],
        'col3': [10.5, 20.3, 30.1, None, 50.8]
    })


@pytest.fixture
def dataframe_with_outliers():
    # outliers óbvios para testar detecção de anomalias
    values = [10] * 20 + [100, 105] + [10] * 8
    return pd.DataFrame({
        'value': values,
        'timestamp': pd.date_range('2024-01-01', periods=30)
    })


@pytest.fixture
def temp_config_file(tmp_path):
    # cria arquivo de configuração temporário 
    config_data = {
        'slack': {
            'webhook_url': 'https://hooks.slack.com/test',
            'channel': '#test-alerts',
            'enabled': True
        },
        'quality': {
            'min_completeness': 0.95,
            'min_uniqueness': 0.9,
            'anomaly_threshold': 3.0
        },
        'monitoring': {
            'check_interval_minutes': 30,
            'data_sources': [
                {
                    'name': 'test_db',
                    'type': 'database',
                    'path': None
                }
            ]
        }
    }
    
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_data, f)
    
    return config_file


@pytest.fixture
def sample_csv_file(tmp_path, sample_dataframe):
    # cria  CSV temporário 
    csv_file = tmp_path / "test_data.csv"
    sample_dataframe.to_csv(csv_file, index=False)
    return csv_file