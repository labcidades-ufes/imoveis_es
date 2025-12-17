#!/usr/bin/env Rscript

# Script de processamento de dados
# Lê dados do MinIO via DuckDB, processa e salva resultado

library(dplyr)
library(tidyr)


# Carrega funções utilitárias
source("utils.R")

cat("============================================================\n")
cat("ETAPA 3: PROCESSAMENTO DE DADOS MUNICIPAIS\n")
cat("============================================================\n")

# Função para ler dados OLX bronze do MinIO via DuckDB


read_from_minio_duckdb <- function() {
  cat("[PROCESSAMENTO] Lendo dados do MinIO via DuckDB\n")

  tryCatch({
    arquivos <- list_parquet_files_in_minio("silver/imoveis_es/municipal/")
    if (length(arquivos) == 0) stop("Nenhum arquivo encontrado em silver/imoveis_es/municipal/")

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
    #------------------------------------------------------------------------------------------------------------------------
    # CALCULO DE INDICADORES
    #------------------------------------------------------------------------------------------------------------------------

    Dados_munic_geral <- anuncios %>%
    group_by(municipio)

    # Seleciona apenas as colunas necessárias
    Dados_munic_geral <- Dados_munic_geral %>%
    select(`preco_R$`,area_m2,municipio) %>%
    summarise(
        preco_max = max(`preco_R$`, na.rm = TRUE),
        preco_min = min(`preco_R$`, na.rm = TRUE),
        preco_med = mean(`preco_R$`, na.rm = TRUE)
        
    ) %>%
    arrange(municipio)

    #Calcula preco_area apenas com linhas completas - área e preço
    preco_area_limpo <- anuncios %>%
    filter(!is.na(`preco_R$`) , !is.na(area_m2), area_m2 > 10) %>%
    group_by(municipio) %>%
    summarise(preco_medio_area = round((sum(`preco_R$`) / sum(area_m2)),1)) %>%
    arrange(municipio)

    # Junta os dois resultados - INDICADORES SEM QUARTOS
    Indicadores_munic_geral <- left_join(Dados_munic_geral,preco_area_limpo, by = "municipio")

    # CALCULO DE INDICADORES PARA QUARTOS
    Dados_munic_quartos <- anuncios %>%
    filter(!is.na(n_quartos)) %>%
    mutate(
        n_quartos = paste(n_quartos,ifelse(n_quartos == 1, "quarto", "quartos"), sep = "_")
    ) %>%
    group_by(n_quartos, municipio) %>%
    summarize(media_quarto = mean(`preco_R$`, na.rm = TRUE)) %>%
    arrange(n_quartos)

    # Faz a conversão de linhas para colunas
    preco_quarto_corrigido <- Dados_munic_quartos %>%
    pivot_wider(
        names_from = n_quartos,
        values_from = media_quarto
    )

    Indicadores_munic<- left_join(Indicadores_munic_geral,preco_quarto_corrigido, by = "municipio")

    cat("[PROCESSAMENTO] Registros após limpeza:", nrow(anuncios), "\n")
    Indicadores_munic
  }, error = function(e) {
    cat("[PROCESSAMENTO] Erro no processamento:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}


# Função para salvar dados pré-processados no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[GOLD] Salvando dados OLX Municipais processados no MinIO\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("gold/imoveis_es/municipal/imoveis_es_munic_gold_%s.parquet", timestamp)
    
    write_parquet_to_minio(data, filepath)
    
    cat("[GOLD] Dados salvos com sucesso:", filepath, "\n")
    return(filepath)
    
  }, error = function(e) {
    cat("[GOLD] Erro ao salvar dados OLX Municipais processados no MinIO:", conditionMessage(e), "\n")
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
