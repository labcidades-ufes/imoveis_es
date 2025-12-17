"""
DAG que orquestra o pipeline de dados em R:
1. Coleta de dados de API pública
2. Processamento dos dados
3. Visualização dos resultados

Cada etapa roda em um container Docker separado com scripts R.

As imagens são buildadas automaticamente pelo Makefile quando você executa:
  make start   # ou make restart
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

# Configurações padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Rede Docker (variável de ambiente do .env)
docker_network = os.getenv('DOCKER_NETWORK')

# Define o DAG
with DAG(
    'pipeline_imoveis_es',
    default_args=default_args,
    description='Pipeline de dados em R: Coleta -> Processamento -> Visualização',
    schedule='@daily',  # Execução diária
    catchup=False,
    tags=['olx', 'r', 'pipeline', 'minio', 'imoveis_es'],
) as dag:

    # Task 1: Coleta de dados olx
    coleta_task = DockerOperator(
        task_id='coleta_imoveis_es',
        image='imoveis_es-coleta:latest',
        api_version='auto',
        auto_remove='success', #True dependendo da versão do airflow
        docker_url='unix://var/run/docker.sock',
        network_mode=docker_network,
        mount_tmp_dir=False,
    )
    # Task 2: Pré-processamento de dados
    pre_processamento_task = DockerOperator(
        task_id='pre_processamento_imoveis_es',
        image='imoveis_es-pre_processamento:latest',
        api_version='auto',
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    # Task 3: Processamento de dados
    processamento_task = DockerOperator(
        task_id='processamento_imoveis_es',
        image='imoveis_es-processamento:latest',
        api_version='auto',
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        network_mode=docker_network,
        mount_tmp_dir=False,
    )


    # Define a ordem de execução
    coleta_task >> pre_processamento_task >> processamento_task