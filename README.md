![CI](https://github.com/brendapmedeiros/monitoramento/workflows/CI/badge.svg)
![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![License](https://img.shields.io/badge/license-MIT-green) 


# Sistema modular para monitoramento contínuo da qualidade de dados, com métricas padronizadas, detecção de anomalias, alertas automáticos e visualização analítica.

Em ambientes orientados a dados, problemas de qualidade raramente falham de forma explícita,eles se propagam silenciosamente para dashboards, modelos e decisões de negócio.

Este projeto foi desenvolvido para:

- detectar quebras de qualidade o mais cedo possível

- fornecer métricas objetivas e rastreáveis

- permitir observabilidade da qualidade ao longo do tempo

- funcionar de forma automatizada e extensível

O foco não é apenas validar dados, mas tornar a qualidade observável.

 Principais Funcionalidades:

- Validação de dados baseada em regras com Great Expectations

- Métricas de qualidade: completude, unicidade e validade

- Detecção de anomalias usando Z-score, IQR eIsolation Forest

- Alertas automáticos via Slack

- Orquestração com Apache Airflow

- Dashboard interativo com Streamlit

- Persistência dos relatórios em JSON, permitindo histórico e auditorias.

Visão geral:

Airflow coordena a execução dos pipelines de monitoramento

Cada execução gera relatórios estruturados com métricas de qualidade e anomalias detectadas.

Os relatórios são armazenados de forma versionável

Streamlit consome esses relatórios para visualização analítica.

O sistema foi projetado para ser tolerante a evolução de esquema e desacoplado entre pipeline e dashboard

# Instalação

Pré-requisitos

Python 3.9+

pip

# Setup rápido
- Clone o repositório

git clone https://github.com/brendapmedeiros/monitoramento.git

cd monitoramento

- Crie o ambiente virtual
python -m venv venv

- Instale as dependências
pip install -r requirements.txt

# Configuração
1. Alertas via Slack (opcional)

Crie um webhook em: https://api.slack.com/messaging/webhooks

Informe a URL no arquivo:

> config/config.yaml

alerting:

  slack_webhook_url: "https://hooks.slack.com/..."

- Fontes de dados a serem monitoradas:

    monitoring:

    data_sources:

    - name: "users_table"

      type: "database"

      path: null


    - name: "transactions"

      type: "csv"

      path: "data/transactions.csv"


- Limites de qualidade e anomalias:

  quality:

  min_completeness: 0.95

  min_uniqueness: 0.90

  anomaly_threshold: 3.0


Esses parâmetros controlam quando uma execução é considerada saudável ou inconsistente.

# Como Usar

-- Execução via CLI

-  Exibe a configuração atual

python -m cli.config_cli show

- Valida as fontes configuradas

python -m cli.config_cli validate

- Executa o monitoramento

python -m src.monitoring.runner


-- Dashboard

O dashboard consome os relatórios gerados pelo pipeline e apresenta:

- Evolução das métricas de qualidade

- Distribuição de severidade das anomalias

- Histórico de execuções

- Indicadores agregados por dataset

Para iniciar:

streamlit run src/dashboard/app.py


Acesse: http://localhost:8501

 # Testes

- Todos os testes
pytest

- Testes unitários
pytest tests/unit -v

- Testes de integração
pytest tests/integration -v





Brenda Medeiros

Analista com foco em Engenharia e Observabilidade de Dados

GitHub: [@brendapmedeiros](https://github.com/brendapmedeiros)

LinkedIn: [brenda-pm](https://www.linkedin.com/in/brenda-pm/)