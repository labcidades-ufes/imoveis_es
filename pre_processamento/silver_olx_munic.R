#!/usr/bin/env Rscript

# Script de processamento de dados
# Lê dados do MinIO via DuckDB, processa e salva resultado

library(dplyr)
library(tidyr)
library(readr)


# Carrega funções utilitárias
source("utils.R")

cat("============================================================\n")
cat("ETAPA 2: PRÉ-PROCESSAMENTO DE DADOS MUNICIPAIS\n")
cat("============================================================\n")

read_from_minio_duckdb <- function() {
  cat("[PRÉ-PROCESSAMENTO] Lendo dados do MinIO via DuckDB\n")

  tryCatch({
    arquivos <- list_parquet_files_in_minio("bronze/imoveis_es/municipal/")
    if (length(arquivos) == 0) stop("Nenhum arquivo encontrado em bronze/imoveis_es/municipal/")

    # nomes vêm como "s3://bucket/bronze/.../arquivo.parquet"
    ultimo <- sort(arquivos, decreasing = TRUE)[1]
    # remove o prefixo "s3://bucket/"
    caminho_rel <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", ultimo)

    data <- read_parquet_from_minio(caminho_rel)
    cat("[COLETA] Dados lidos com sucesso:", nrow(data), "registros\n")
    data
  }, error = function(e) {
    cat("[COLETA] Erro ao ler dados:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}


# Pré-processamento dos dados bronze
process_data <- function(data) {
  cat("[PRÉ-PROCESSAMENTO] Aplicando pré-processamento aos", nrow(data), "registros\n")
  tryCatch({
    anuncios <- as_tibble(data)

    anuncios <- anuncios %>%
      mutate(
        `preco_R$` = parse_number(`preco_R$`),
        area_m2 = parse_number(area_m2),
        n_quartos = parse_number(n_quartos),
        n_banheiros = parse_number(n_banheiros),
        n_vagas_garagem = parse_number(n_vagas_garagem),
        `preco_R$` = ifelse(`preco_R$` < 10000, `preco_R$` * 1000, `preco_R$`),
        n_vagas_garagem = ifelse(n_vagas_garagem == 0, NA, n_vagas_garagem),
        n_quartos = ifelse(n_quartos == 0, NA, n_quartos),
        n_banheiros = ifelse(n_banheiros == 0, NA, n_banheiros)
      ) %>%
      distinct(across(-url), .keep_all = TRUE) %>%
      filter(
        !is.na(`preco_R$`),
        `preco_R$` != 0,
        `preco_R$` < 100000000
      )

    cat("[PRÉ-PROCESSAMENTO] Registros após limpeza:", nrow(anuncios), "\n")
    anuncios
  }, error = function(e) {
    cat("[PRÉ-PROCESSAMENTO] Erro no processamento:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# Função para salvar dados pré-processados no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[SILVER] Salvando dados OLX Municipais pré-processados no MinIO\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("silver/imoveis_es/municipal/imoveis_es_munic_silver_%s.parquet", timestamp)
    
    write_parquet_to_minio(data, filepath)
    
    cat("[SILVER] Dados salvos com sucesso:", filepath, "\n")
    return(filepath)
    
  }, error = function(e) {
    cat("[SILVER] Erro ao salvar dados OLX Municipais pré-processados no MinIO:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}


# Execução principal
tryCatch({
  # Lê dados do MinIO
  raw_data <- read_from_minio_duckdb()
  
  # Processa os dados
  processed_data <- process_data(raw_data)
  
  # Salva no MinIO
  filepaths <- save_to_minio_duckdb(processed_data)
  
  cat("============================================================\n")
  cat("[PRÉ-PROCESSAMENTO] Processamento finalizado com sucesso!\n")
  cat("============================================================\n")
  
}, error = function(e) {
  cat("[PRÉ-PROCESSAMENTO] Erro fatal:", conditionMessage(e), "\n")
  quit(status = 1)
})
