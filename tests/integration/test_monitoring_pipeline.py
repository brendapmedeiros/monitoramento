import pytest
import pandas as pd


@pytest.mark.integration
class TestMonitoringPipeline:
    
    def test_load_config_and_process_data(self, temp_config_file, sample_csv_file):
        #testa fluxo de carregar config > ler dados > validar
        from config.config_manager import ConfigManager
        
        #carrega configuração
        config_manager = ConfigManager(str(temp_config_file))
        config = config_manager.get()
        assert config.quality.min_completeness == 0.95
        
        #lê dados
        df = pd.read_csv(sample_csv_file)
        assert len(df) == 10
        
        #calcula métricas
        completeness = df.notna().sum().sum() / (len(df) * len(df.columns))
        
        #verifica se passa no limite
        assert completeness >= config.quality.min_completeness
    
    def test_detect_quality_issues(self, temp_config_file):
        from config.config_manager import ConfigManager
        import pandas as pd

        config = ConfigManager(str(temp_config_file)).get()

        df = pd.DataFrame({
            "a": [1, None, None, None],
            "b": [1, None, None, None],
        })

        completeness = df.notna().sum().sum() / (len(df) * len(df.columns))

        should_alert = completeness < config.quality.min_completeness

        assert should_alert
