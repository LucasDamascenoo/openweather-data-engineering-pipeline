# openweather-data-engineering-pipeline

Pipeline de Engenharia de Dados para coletar dados climaticos da API OpenWeather, transformar e carregar em PostgreSQL usando Apache Airflow (Docker Compose).

## 🎯 Objetivo do Projeto

Este projeto implementa um fluxo ETL para dados de clima de Sao Paulo (BR):

- `Extract`: consulta a API OpenWeather e salva JSON bruto.
- `Transform`: normaliza campos aninhados, renomeia colunas, remove colunas desnecessarias e converte timestamps para `America/Sao_Paulo`.
- `Load`: grava os dados transformados na tabela `sp_weather` no PostgreSQL.

## 🧱 Arquitetura Utilizada

Orquestracao e execucao:

- Apache Airflow (DAG `weather_pipeline`) para agendamento e dependencia entre tarefas.
- CeleryExecutor com Redis e Postgres interno do Airflow (infra em `docker-compose.yaml`).
- PostgreSQL externo (maquina local) como destino final, acessado pelos containers via `host.docker.internal`.

Fluxo da DAG (`dags/weather_dag.py`):

1. `extract()` chama `extract_weather_data(url)` em `src/extract.py`.
2. `transform()` chama `data_transformations()` em `src/transform_data.py` e grava `data/temp_data.parquet`.
3. `load()` le o parquet e chama `load_data('sp_weather', df)` em `src/load_data.py`.

Agendamento atual:

- Cron: `0 */1 * * *` (a cada 1 hora).
- `catchup=False`.

## Estrutura do projeto

```text
.
|-- dags/
|   `-- weather_dag.py
|-- src/
|   |-- extract.py
|   |-- transform_data.py
|   `-- load_data.py
|-- data/
|-- config/
|   `-- .env
|-- docker-compose.yaml
|-- pyproject.toml
`-- README.md
```



![Python](https://img.shields.io/badge/python-3.11-0A0A0A?style=for-the-badge&logo=python)
![Docker](https://img.shields.io/badge/docker-containerized-0A0A0A?style=for-the-badge&logo=docker)
![Postgres](https://img.shields.io/badge/postgres-database-0A0A0A?style=for-the-badge&logo=postgresql)
![Airflow](https://img.shields.io/badge/airflow-orchestration-0A0A0A?style=for-the-badge&logo=apacheairflow)
![SQL](https://img.shields.io/badge/sql-query--language-0A0A0A?style=for-the-badge&logo=postgresql)
