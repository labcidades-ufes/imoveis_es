#!/usr/bin/env Rscript

# Script de processamento de dados
# Lê dados do MinIO via DuckDB, processa e salva resultado

library(dplyr)
library(tidyr)


# Carrega funções utilitárias
source("utils.R")

cat("============================================================\n")
cat("ETAPA 3: PROCESSAMENTO DE DADOS HEXAGONAIS\n")
cat("============================================================\n")

# Função para ler dados OLX silver do MinIO via DuckDB

read_from_minio_duckdb <- function() {
  cat("[PROCESSAMENTO] Lendo dados do MinIO via DuckDB\n")

  tryCatch({
    arquivos <- list_parquet_files_in_minio("silver/imoveis_es/hexagonal/")
    if (length(arquivos) == 0) stop("Nenhum arquivo encontrado em silver/imoveis_es/hexagonal/")

    # nomes vêm como "s3://bucket/silver/.../arquivo.parquet"
    ultimo <- sort(arquivos, decreasing = TRUE)[1]
    # remove o prefixo "s3://bucket/"
    caminho_rel <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", ultimo)

    data <- read_parquet_from_minio(caminho_rel)
    cat("[PROCESSAMENTO] Dados lidos com sucesso:", nrow(data), "registros\n")
    data
  }, error = function(e) {
    cat("[PROCESSAMENTO] Erro ao ler dados:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# Processamento dos dados silver
process_data <- function(data) {
  cat("[PROCESSAMENTO] Aplicando processamento aos", nrow(data), "registros\n")
  tryCatch({
    anuncios <- as_tibble(data)
    Dados_hex_geral <- anuncios %>%
        group_by(hex_id)

    # Seleciona apenas as colunas necessárias
    Dados_hex_geral <- Dados_hex_geral %>%
    select(`preco_R$`,area_m2,hex_id) %>%
    summarise(
        preco_max = max(`preco_R$`, na.rm = TRUE),
        preco_min = min(`preco_R$`, na.rm = TRUE),
        preco_med = mean(`preco_R$`, na.rm = TRUE)
        
    ) %>%
    arrange(hex_id)

    #Calcula preco_area apenas com linhas completas - área e preço
    preco_area_limpo <- anuncios %>%
    filter(!is.na(`preco_R$`) , !is.na(area_m2), area_m2 > 10) %>%
    group_by(hex_id) %>%
    summarise(preco_medio_area = round((sum(`preco_R$`) / sum(area_m2)),1)) %>%
    arrange(hex_id)


    # Junta os dois resultados - INDICADORES SEM QUARTOS
    Indicadores_hex_geral <- left_join(Dados_hex_geral,preco_area_limpo, by = "hex_id")

    # CALCULO DE INDICADORES PARA QUARTOS
    Dados_hex_quartos <- anuncios %>%
    filter(!is.na(n_quartos)) %>%
    mutate(
        n_quartos = paste(n_quartos,ifelse(n_quartos == 1, "quarto", "quartos"), sep = "_")
    ) %>%
    group_by(n_quartos, hex_id) %>%
    summarize(media_quarto = mean(`preco_R$`, na.rm = TRUE)) %>%
    arrange(n_quartos)


    # Faz a conversão de linhas para colunas
    preco_quarto_corrigido <- Dados_hex_quartos %>%
    pivot_wider(
        names_from = n_quartos,
        values_from = media_quarto
    )

    Indicadores_hex <- left_join(Indicadores_hex_geral,preco_quarto_corrigido, by = "hex_id")


    cat("[PROCESSAMENTO] Registros após cálculos:", nrow(anuncios), "\n")
    Indicadores_hex
  }, error = function(e) {
    cat("[PROCESSAMENTO] Erro no processamento:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}



# Função para salvar dados pré-processados no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[GOLD] Salvando dados OLX Hexagonais processados no MinIO\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("gold/imoveis_es/hexagonal/imoveis_es_hex_gold_%s.parquet", timestamp)
    
    write_parquet_to_minio(data, filepath)
    
    cat("[GOLD] Dados salvos com sucesso:", filepath, "\n")
    return(filepath)
    
  }, error = function(e) {
    cat("[GOLD] Erro ao salvar dados OLX Hexagonais processados no MinIO:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}


# Execução principal
tryCatch({
  # Lê dados do MinIO
  silver_data <- read_from_minio_duckdb()
  
  # Processa os dados
  gold_data <- process_data(silver_data)
  
  # Salva no MinIO
  filepaths <- save_to_minio_duckdb(gold_data)
  
  cat("============================================================\n")
  cat("[PROCESSAMENTO] Processamento finalizado com sucesso!\n")
  cat("============================================================\n")
  
}, error = function(e) {
  cat("[PROCESSAMENTO] Erro fatal:", conditionMessage(e), "\n")
  quit(status = 1)
})