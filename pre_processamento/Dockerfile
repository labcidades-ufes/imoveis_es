FROM utils-r_duckdb:latest

# Instala dependências do sistema necessárias para utilizar a biblioteca sf
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgdal-dev libgeos-dev libproj-dev libudunits2-dev \
        build-essential gfortran gdal-bin libgeotiff-dev \
        libprotobuf-dev protobuf-compiler libjq-dev \
        r-cran-sf && \
    rm -rf /var/lib/apt/lists/*

# Instala pacotes R necessários incluindo DuckDB
RUN R -e "install.packages(c('jsonlite', 'dplyr', 'tidyr','tidyverse','sf','geobr','readr','duckdb'), repos='https://cloud.r-project.org/')"

# Cria diretório de trabalho
WORKDIR /app

# Copia utils.R da raiz do projeto
COPY dags/utils/utils.R .

#Scripts de pré processamento
COPY dags/imoveis_es/pre_processamento/silver_olx_hex.R .
COPY dags/imoveis_es/pre_processamento/silver_olx_munic.R .

#Script de entrypoint - executa os scripts de pré processamento em ordem
COPY dags/imoveis_es/pre_processamento/entrypoint.R .

CMD ["Rscript", "entrypoint.R"]

