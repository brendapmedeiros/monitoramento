import click
import yaml
from pathlib import Path
from config.config_manager import ConfigManager
from rich.console import Console
from rich.table import Table


console = Console()


@click.group()
def cli():
    pass


@cli.command()
@click.option('--config', default=None, help='Path to config file')
def show(config):
    ##Mostra config atual
    try:
        manager = ConfigManager(config)
        cfg = manager.load()
        
        console.print("\nConfiguração Atual\n")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Section", style="cyan")
        table.add_column("Key", style="green")
        table.add_column("Value", style="yellow")
        
        config_dict = cfg.model_dump()
        for section, values in config_dict.items():
            if isinstance(values, dict):
                for key, value in values.items():
                    table.add_row(section, key, str(value))
            elif isinstance(values, list):
                table.add_row(section, "items", str(len(values)))
        
        console.print(table)
        
    except Exception as e:
        console.print(f"Erro {str(e)}")


@cli.command()
@click.option('--config', default=None, help='Path to config file')
def validate(config):
##Validação do arquivo de configuração
    try:
        manager = ConfigManager(config)
        cfg = manager.load()
        console.print("Configuração válida!")
    except Exception as e:
        console.print(f"Erro na validação:{str(e)}")


@cli.command()
@click.argument('section')
@click.argument('key')
@click.argument('value')
@click.option('--config', default=None, help= 'Diretório o arquivo de configuração')
def set(section, key, value, config):
    """Set a configuration value"""
    try:
        manager = ConfigManager(config)
        cfg = manager.load()
        config_dict = cfg.model_dump()
        
        if section not in config_dict:
            console.print(f"Sessão '{section}' não encontrada")
            return
        
        if key not in config_dict[section]:
            console.print(f"Chave '{key}' não encontrada na sessão '{section}'")
            return
        
        try:
            if isinstance(config_dict[section][key], bool):
                value = value.lower() in ('true', '1', 'yes')
            elif isinstance(config_dict[section][key], int):
                value = int(value)
            elif isinstance(config_dict[section][key], float):
                value = float(value)
        except ValueError:
            console.print(f"Tipo de valor inválido")
            return
        
        config_dict[section][key] = value
        new_config = type(cfg)(**config_dict)
        manager.save(new_config)
        
        console.print(f"Atualizado: {section}.{key} = {value}")
        
    except Exception as e:
        console.print(f"Erro {str(e)}")


@cli.command()
@click.option('--config', default=None, help='Diretório do arquivo de configuração')
def init(config):
    try:
        if config is None:
            config = Path("config/config.yaml")
        else:
            config = Path(config)
        
        if config.exists():
            console.print(f"[bold yellow]Config file already exists:[/bold yellow] {config}")
            return
        
        config.parent.mkdir(parents=True, exist_ok=True)
        
        default_config = {
            "slack": {
                "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
                "channel": "#data-alerts",
                "enabled": True
            },
            "quality": {
                "min_completeness": 0.95,
                "min_uniqueness": 0.90,
                "anomaly_threshold": 3.0
            },
            "monitoring": {
                "check_interval_minutes": 30,
                "data_sources": [
                    {"name": "users_table", "type": "database", "path": None},
                    {"name": "transactions.csv", "type": "csv", "path": "data/transactions.csv"}
                ]
            }
        }
        
        with open(config, 'w') as f:
            yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)
        
        console.print(f"Arquivo de configuração criado. {config}")
        
    except Exception as e:
        console.print(f"Erro {str(e)}")


if __name__ == '__main__':
    cli()