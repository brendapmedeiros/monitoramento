import pytest
import pandas as pd


class TestDataQualityMetrics:
    
    def test_completeness_calculation(self, dataframe_with_nulls):
        ## testa cálculo de completness
        df = dataframe_with_nulls
        
        # col1: 4 válidos de 5 = 80%
        completeness_col1 = df['col1'].notna().sum() / len(df)
        assert completeness_col1 == 0.8
        
        # col2: 3 valores de 5 = 60%
        completeness_col2 = df['col2'].notna().sum() / len(df)
        assert completeness_col2 == 0.6
    
    def test_uniqueness_calculation(self, sample_dataframe):
        # testa cálculo de unicidade
        df = sample_dataframe
        
        # 'id' único 
        uniqueness_id = df['id'].nunique() / len(df)
        assert uniqueness_id == 1.0
        
        # 'category' tem  3 valores únicos em 10 linhas
        uniqueness_category = df['category'].nunique() / len(df)
        assert uniqueness_category == 0.3
    
    def test_overall_quality_score(self, sample_dataframe):
       # testa cálculo de score geral 
        df = sample_dataframe
        
        # calcula métricas
        completeness = df.notna().sum().sum() / (len(df) * len(df.columns))
        
        # dados limpos devem ter completeness em 100%
        assert completeness == 1.0
    
    def test_detect_duplicates(self, sample_dataframe):
        # detecção de duplicatas
        df = sample_dataframe
        
        # dados originais não tem duplicatas
        duplicates = df.duplicated().sum()
        assert duplicates == 0
        
        # adicion linha duplicada
        df_with_dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        duplicates = df_with_dup.duplicated().sum()
        assert duplicates == 1