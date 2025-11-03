"""
Teste para o sistema de métricas
"""

import pandas as pd
import numpy as np
from src.core.data_quality import DataQualityMetrics
import sys
from pathlib import Path

def create_sample_datasets():
    
    # Alta qualidade
    high_quality = pd.DataFrame({
        'customer_id': range(1, 101),
        'name': [f'Customer_{i}' for i in range(1, 101)],
        'email': [f'customer{i}@email.com' for i in range(1, 101)],
        'age': np.random.randint(18, 80, 100),
        'purchase_amount': np.random.uniform(10, 1000, 100).round(2)
    })
    
    # Qualidade média 
    medium_quality = high_quality.copy()
    medium_quality.loc[0:10, 'email'] = None
    medium_quality.loc[5:15, 'age'] = None
    medium_quality = pd.concat([medium_quality, medium_quality.iloc[0:5]], ignore_index=True)
    
    # Baixa qualidade
    low_quality = high_quality.copy()
    low_quality.loc[0:30, 'email'] = None
    low_quality.loc[10:40, 'name'] = None
    low_quality.loc[20:50, 'age'] = None
    low_quality = pd.concat([low_quality, low_quality.iloc[0:20]], ignore_index=True)
    low_quality.loc[5:10, 'age'] = -1
    low_quality.loc[15:20, 'purchase_amount'] = -100
    
    return {
        'high_quality': high_quality,
        'medium_quality': medium_quality,
        'low_quality': low_quality
    }

def test_single_dataset(df, dataset_name):
    """Testa métricas em um único dataset"""
    print(f"\n{'='*60}")
    print(f"Testando: {dataset_name}")
    print(f"{'='*60}\n")
    
    # Inicializa o sistema
    dq = DataQualityMetrics(dataset_name=dataset_name)
    
    # Define regras de validação customizadas
    validation_rules = {
        'valid_email': lambda df: df['email'].str.contains('@', na=False),
        'positive_age': lambda df: df['age'] > 0,
        'positive_amount': lambda df: df['purchase_amount'] > 0,
        'reasonable_age': lambda df: df['age'].between(0, 120)
    }
    
    # Executar análise
    metrics = dq.analyze_dataset(
        df,
        key_columns=['customer_id'],
        validation_rules=validation_rules
    )
    
    # Imprimir resumo
    print(dq.get_quality_summary(metrics))
    
    # Detalhes adicionais
    print("\n Detalhamento por coluna:\n")
    print(f"Completude por coluna:")
    for col, value in metrics.details['completeness']['by_column'].items():
        print(f"  • {col}: {value}%")
    
    print(f"\nUnicidade por coluna:")
    for col, value in metrics.details['uniqueness']['by_column'].items():
        print(f"  • {col}: {value}%")
    
    print(f"\nValidações:")
    for rule, value in metrics.details['validity']['by_rule'].items():
        status = "✓" if value >= 95 else "✗"
        print(f"  {status} {rule}: {value}%")
    
    # Salvar métricas
    output_file = f"metrics_{dataset_name}.json"
    dq.save_metrics(metrics, output_file)
    print(f"\n Métricas salvas em: {output_file}")
    
    return metrics

def compare_datasets(all_metrics):
    """Compara métricas entre diferentes datasets"""
    print(f"\n{'='*60}")
    print("COMPARAÇÃO DE QUALIDADE ENTRE DATASETS")
    print(f"{'='*60}\n")
    
    comparison_data = []
    for name, metrics in all_metrics.items():
        comparison_data.append({
            'Dataset': name,
            'Score': f"{metrics.quality_score}%",
            'Completude': f"{metrics.completeness}%",
            'Unicidade': f"{metrics.uniqueness}%",
            'Validade': f"{metrics.validity}%",
            'Consistência': f"{metrics.consistency}%"
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    print(comparison_df.to_string(index=False))
    
    # Identificar melhor e pior
    scores = [(name, m.quality_score) for name, m in all_metrics.items()]
    best = max(scores, key=lambda x: x[1])
    worst = min(scores, key=lambda x: x[1])
    
    print(f"\n🏆 Melhor dataset: {best[0]} ({best[1]}%)")
    print(f"⚠️  Pior dataset: {worst[0]} ({worst[1]}%)")

def main():
    print("Iniciando testes do Sistema de Métricas de Data Quality\n")
    
    # Cria datasets de exemplo
    print("Criando datasets de exemplo...")
    datasets = create_sample_datasets()
    
    # Testa cada dataset
    all_metrics = {}
    for name, df in datasets.items():
        metrics = test_single_dataset(df, name)
        all_metrics[name] = metrics
    
    # Comparar resultados
    compare_datasets(all_metrics)
    
    print("\n Testes concluídos com sucesso!")
    print("\n Próximos passos:")
    print("  1. Revisar JSONs gerados")
    print("  2. Ajustar thresholds conforme necessário")
    print("  3. Integrar com Great Expectations")
    print("  4. Preparar para detecção de inconsistências")

if __name__ == "__main__":
    main()