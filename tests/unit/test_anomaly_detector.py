import pytest
import numpy as np


class TestAnomalyDetector:
    
    def test_zscore_outlier_detection(self, dataframe_with_outliers):
       # testa detecção de outliers 
        df = dataframe_with_outliers
        
        mean = df['value'].mean()
        std = df['value'].std()
        z_scores = np.abs((df['value'] - mean) / std)

        outliers = z_scores > 3
        
        assert outliers.sum() == 2
        
        # verifica se valores estão corretos
        outlier_values = df[outliers]['value'].values
        assert 100 in outlier_values
        assert 105 in outlier_values
    
    def test_iqr_outlier_detection(self, dataframe_with_outliers):
    ## testa detecção com iqr
        df = dataframe_with_outliers
        
        Q1 = df['value'].quantile(0.25)
        Q3 = df['value'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = (df['value'] < lower_bound) | (df['value'] > upper_bound)

        assert outliers.sum() >= 2
    
    def test_no_outliers_in_clean_data(self, sample_dataframe):
        # testa que dados limpos não geram positivo incorreto
        df = sample_dataframe

        mean = df['value'].mean()
        std = df['value'].std()
        z_scores = np.abs((df['value'] - mean) / std)

        outliers = z_scores > 3
        assert outliers.sum() == 0