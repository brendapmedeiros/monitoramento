import pytest
from config.config_manager import ConfigManager


class TestConfigManager:
    
    def test_load_config_success(self, temp_config_file):
       # testa carregamento de configuração
        config_manager = ConfigManager(str(temp_config_file))
        config = config_manager.get()
        
        # acessa com pydantic
        assert config.slack.webhook_url == 'https://hooks.slack.com/test'
        assert config.slack.channel == '#test-alerts'
        assert config.quality.min_completeness == 0.95
    
    def test_get_nested_config(self, temp_config_file):
        config_manager = ConfigManager(str(temp_config_file))
        config = config_manager.get()
        
        # acessa atributos do pydantic
        assert config.slack.enabled is True
        assert config.slack.channel == '#test-alerts'
        assert config.quality.anomaly_threshold == 3.0
    
    def test_data_sources_list(self, temp_config_file):
        # testa se consegue pegar lista de dados
        config_manager = ConfigManager(str(temp_config_file))
        config = config_manager.get()
        
    
        sources = config.monitoring.data_sources
        assert isinstance(sources, list)
        assert len(sources) == 1
        
        # acessa com pydantic
        assert sources[0].name == 'test_db'
        assert sources[0].type == 'database'
    
    def test_reload_config(self, temp_config_file):
        config_manager = ConfigManager(str(temp_config_file))
        
        # carrega primeira vez
        config1 = config_manager.get()
        assert config1.slack.webhook_url == 'https://hooks.slack.com/test'
        
        # recarrega
        config2 = config_manager.reload()
        assert config2.slack.webhook_url == 'https://hooks.slack.com/test'