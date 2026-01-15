import yaml
from pathlib import Path
from typing import Optional
from config.config_schema import Config


class ConfigManager:
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = Path(__file__).parent / "config.yaml"
        self.config_path = Path(config_path)
        self._config: Optional[Config] = None

    def load(self) -> Config:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config. file não encontrado: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        self._config = Config(**config_data)
        return self._config

    def get(self) -> Config:
        if self._config is None:
            self.load()
        return self._config

    def save(self, config: Config):
        config_dict = config.model_dump()
        with open(self.config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
        self._config = config

    def reload(self) -> Config:
        self._config = None
        return self.load()


_manager = ConfigManager()


def get_config() -> Config:
    return _manager.get()


def reload_config() -> Config:
    return _manager.reload()