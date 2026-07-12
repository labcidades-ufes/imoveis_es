FROM utils-r_duckdb:latest

# Instala dependências do sistema necessárias para utilizar a biblioteca sf
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         libgdal-dev libgeos-dev libproj-dev libudunits2-dev \
#         build-essential gfortran gdal-bin libgeotiff-dev \
#         libprotobuf-dev protobuf-compiler libjq-dev \
#         r-cran-sf && \
#     rm -rf /var/lib/apt/lists/*

# Dependências de sistema necessárias para compilar units, s2 e sf
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgdal-dev \
        libgeos-dev \
        libproj-dev \
        libudunits2-dev \
        build-essential \
        gfortran \
        gdal-bin \
        libgeotiff-dev \
        libprotobuf-dev \
        protobuf-compiler \
        libjq-dev \
        libzstd-dev \
        liblz4-dev \
        zlib1g-dev \
        libcurl4-openssl-dev \
        libssl-dev \
        libxml2-dev \
        libuv1-dev \
        cmake \
        ninja-build && \
    rm -rf /var/lib/apt/lists/*


# Compila os pacotes usando a mesma versão do R da imagem
RUN R -e "install.packages('units', type = 'source', repos = 'https://cloud.r-project.org/')" && \
    R -e "install.packages('sf', type = 'source', repos = 'https://cloud.r-project.org/')" && \
    R -e "install.packages(c('jsonlite', 'dplyr', 'tidyr', 'geobr', 'readr'), repos = 'https://cloud.r-project.org/')"

# Falha durante o build se houver incompatibilidade
RUN R -e "library(units); library(sf); library(geobr); sessionInfo()"


# Instala pacotes R necessários
# RUN R -e "install.packages(c('jsonlite', 'dplyr', 'tidyr','tidyverse','sf','geobr','readr'), repos='https://cloud.r-project.org/')"

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

