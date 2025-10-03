"""
Script para gerar dados fake para testes do sistema
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path


def generate_sales_data(n_records=1000, with_anomalies=True):
    """
    Gera dados de vendas fictícios com possíveis inconsistências
    """
    np.random.seed(42)
    random.seed(42)
    
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(n_records)]
    
    # Dados básicos
    data = {
        'transaction_id': [f'TXN{str(i).zfill(6)}' for i in range(1, n_records + 1)],
        'date': dates,
        'customer_id': np.random.randint(1000, 9999, n_records),
        'product_id': np.random.choice(['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005'], n_records),
        'quantity': np.random.randint(1, 50, n_records),
        'unit_price': np.random.uniform(10, 500, n_records).round(2),
        'region': np.random.choice(['North', 'South', 'East', 'West'], n_records),
        'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash'], n_records),
    }
    
    df = pd.DataFrame(data)
    
    # Calcular valor total
    df['total_amount'] = (df['quantity'] * df['unit_price']).round(2)
    
    # Adicionar status
    df['status'] = np.random.choice(['completed', 'pending', 'cancelled'], n_records, p=[0.85, 0.10, 0.05])
    
    # Adicionar email
    df['customer_email'] = [
        f'customer{i}@email.com' if random.random() > 0.02 
        else f'invalid_email_{i}' 
        for i in range(n_records)
    ]
    
    if with_anomalies:
        # Inserir inconsistências propositalmente
        
        # 1 Nulos em campos críticos 
        null_indices = np.random.choice(df.index, size=int(n_records * 0.02), replace=False)
        df.loc[null_indices, 'customer_id'] = np.nan
        
        # 2. Preços negativos 
        negative_indices = np.random.choice(df.index, size=5, replace=False)
        df.loc[negative_indices, 'unit_price'] = -df.loc[negative_indices, 'unit_price']
        
        # 3. Quantidades muito altas
        high_qty_indices = np.random.choice(df.index, size=3, replace=False)
        df.loc[high_qty_indices, 'quantity'] = np.random.randint(1000, 5000, size=3)
        
        # 4. transaction_id duplicados
        duplicate_indices = np.random.choice(df.index[10:], size=5, replace=False)
        df.loc[duplicate_indices, 'transaction_id'] = df.loc[duplicate_indices - 10, 'transaction_id'].values
        
        # 5. Datas que ainda não aconteceram
        future_indices = np.random.choice(df.index, size=3, replace=False)
        df.loc[future_indices, 'date'] = datetime.now() + timedelta(days=30)
    
    return df


def generate_user_activity_data(n_records=500):
    """
    Gera atividade de usuários
    """
    np.random.seed(42)
    
    start_time = datetime(2024, 10, 1)
    
    data = {
        'user_id': [f'USER{str(i).zfill(5)}' for i in range(1, n_records + 1)],
        'timestamp': [start_time + timedelta(hours=i*2) for i in range(n_records)],
        'action': np.random.choice(['login', 'view_page', 'purchase', 'logout', 'add_to_cart'], n_records),
        'session_duration_minutes': np.random.exponential(15, n_records).round(2),
        'pages_viewed': np.random.randint(1, 50, n_records),
        'device_type': np.random.choice(['mobile', 'desktop', 'tablet'], n_records, p=[0.5, 0.4, 0.1]),
        'browser': np.random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'], n_records),
        'country': np.random.choice(['BR', 'US', 'UK', 'CA', 'DE'], n_records),
    }
    
    df = pd.DataFrame(data)
    
    # Adicionar inconsistência
    anomaly_indices = np.random.choice(df.index, size=10, replace=False)
    df.loc[anomaly_indices, 'session_duration_minutes'] = np.random.uniform(200, 500, 10)
    
    return df


def save_datasets(output_dir='data'):
    """
    Salva todos os datasets gerados
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(" Gerando dados falsos...")
    
    # Gerar e salvar vendas
    sales_df = generate_sales_data(n_records=1000, with_anomalies=True)
    sales_path = output_path / 'sales_transactions.csv'
    sales_df.to_csv(sales_path, index=False)
    print(f" Dados de vendas salvos: {sales_path}")
    print(f"   - {len(sales_df)} registros")
    print(f"   - {len(sales_df.columns)} colunas")
    
    # Gerar e salvar atividade de usuários
    activity_df = generate_user_activity_data(n_records=500)
    activity_path = output_path / 'user_activity.csv'
    activity_df.to_csv(activity_path, index=False)
    print(f" Dados de atividade salvos: {activity_path}")
    print(f"   - {len(activity_df)} registros")
    print(f"   - {len(activity_df.columns)} colunas")
    
    # Criar versão sem erros para comparação
    clean_sales_df = generate_sales_data(n_records=1000, with_anomalies=False)
    clean_path = output_path / 'sales_transactions_clean.csv'
    clean_sales_df.to_csv(clean_path, index=False)
    print(f"Dados limpos salvos: {clean_path}")
    
    print("\nResumo dos dados gerados:")
    print(f"   Total de registros: {len(sales_df) + len(activity_df)}")
    print(f"   Arquivos criados: 3")
    print(f"   Diretório: {output_path.absolute()}")
    
    return sales_df, activity_df, clean_sales_df


if __name__ == '__main__':
    save_datasets()