FROM utils-r_duckdb:latest

# Instala dependências do sistema necessárias para utilizar a biblioteca sf 
# e manipulação de arquivos no formato "zstd"
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgdal-dev libgeos-dev libproj-dev libudunits2-dev \
        build-essential gfortran gdal-bin libgeotiff-dev \
        libprotobuf-dev protobuf-compiler libjq-dev r-cran-sf \
        libzstd-dev liblz4-dev zlib1g-dev libcurl4-openssl-dev \
        libssl-dev libxml2-dev cmake ninja-build && \
    rm -rf /var/lib/apt/lists/*


# Instala pacote R necessário para manipulação de arquivos no formato "zstd"
RUN ARROW_WITH_ZSTD=ON ARROW_WITH_LZ4=ON \
    R -e "install.packages('arrow', type='source', repos='https://cloud.r-project.org/')"

# Instala pacotes R necessários
RUN R -e "install.packages(c('httr', 'jsonlite', 'rvest', 'purrr', 'stringr', 'dplyr', 'tibble', 'glue', 'sf','geocodebr','geobr'), repos='https://cloud.r-project.org/')"

# Cria diretório de trabalho
WORKDIR /app

# Copia utils.R da raiz do projeto
COPY dags/utils/utils.R .

# Copia o script
COPY dags/imoveis_es/coleta/dados_olx_brutos.R .
COPY dags/imoveis_es/coleta/olx_brutos_geoloc.R .

COPY dags/imoveis_es/coleta/entrypoint.R .

# Executa o script
#CMD ["Rscript", "entrypoint.R"]
CMD ["Rscript", "olx_brutos_geoloc.R"]
