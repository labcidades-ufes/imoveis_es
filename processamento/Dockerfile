FROM utils-r_duckdb:latest

# Instala pacotes R necessários incluindo DuckDB
RUN R -e "install.packages(c('jsonlite', 'dplyr', 'tidyr','readr'), repos='https://cloud.r-project.org/')"

# Cria diretório de trabalho
WORKDIR /app

# Copia utils.R da raiz do projeto
COPY dags/utils/utils.R .

#Scripts de pré processamento
COPY dags/imoveis_es/processamento/gold_olx_hex.R .
COPY dags/imoveis_es/processamento/gold_olx_munic.R .

#Script de entrypoint - executa os scripts de pré processamento em ordem
COPY dags/imoveis_es/processamento/entrypoint.R .

CMD ["Rscript", "entrypoint.R"]

