#!/usr/bin/env Rscript

# Script de processamento de dados
# Lê dados do MinIO via DuckDB, processa e salva resultado

# Pacotes necessários

library(sf)
library(geobr)
library(dplyr)
library(readr)

# Carrega funções utilitárias
source("utils.R")

cat("============================================================\n")
cat("ETAPA 2: PRÉ-PROCESSAMENTO DE DADOS HEXÁGONAIS\n")
cat("============================================================\n")

read_from_minio_duckdb <- function() {
  cat("[PRÉ-PROCESSAMENTO] Lendo dados do MinIO via DuckDB\n")

  tryCatch({
    # arquivos <- list_parquet_files_in_minio("bronze/imoveis_es/hexagonal/")
    # if (length(arquivos) == 0) stop("Nenhum arquivo encontrado em bronze/imoveis_es/hexagonal/")

    # # nomes vêm como "s3://bucket/bronze/.../arquivo.parquet"
    # ultimo <- sort(arquivos, decreasing = TRUE)[1]
    # # remove o prefixo "s3://bucket/"
    # caminho_rel <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", ultimo)

    # data <- read_parquet_from_minio(caminho_rel)
    data <- read_latest_parquet_from_minio("bronze/imoveis_es/hexagonal/")
    cat("[COLETA] Dados lidos com sucesso:", nrow(data), "registros\n")
    data
  }, error = function(e) {
    cat("[COLETA] Erro ao ler dados:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}





# Cria grade hexagonal no Espírito Santo
criar_hexgrid_es <- function(hex_size = 5000, proj_crs = 3857) {
  es <- geobr::read_state(code_state = "ES", year = 2020)
  es_proj <- st_transform(es, crs = proj_crs)
  
  grid_hex <- st_make_grid(
    es_proj,
    cellsize = hex_size,
    square = FALSE
  ) %>%
    st_as_sf() %>%
    st_intersection(st_union(es_proj)) %>%
    st_collection_extract("POLYGON") %>%   # garante apenas polígonos
    mutate(hex_id = row_number())
  
  grid_hex_wgs84 <- st_transform(grid_hex, crs = 4326)
  return(grid_hex_wgs84)
}



# Associa regiões a hexágonos e filtra apenas hexágonos válidos
atribuir_hex_pontos <- function(df_pontos_sf, hex_grid) {
  # Garantir CRS compatível
  if (st_crs(df_pontos_sf) != st_crs(hex_grid)) {
    df_pontos_sf <- st_transform(df_pontos_sf, st_crs(hex_grid))
  }
  
  # Join: cada anúncio recebe hex_id
  joined <- st_join(df_pontos_sf, hex_grid, join = st_intersects, left = TRUE)
  
  # Substituir geometria ativa pela dos hexágonos
  joined$geometry_anuncio <- st_geometry(joined)  # salvar geometria original do anúncio
  st_geometry(joined) <- st_geometry(hex_grid)[match(joined$hex_id, hex_grid$hex_id)]
  
  #Remove coluna de geometria do anúncio, mantendo apenas a hexagonal
  ex_grid_anuncios <- joined %>%
    select(-geometry_anuncio)
  return(ex_grid_anuncios)
}

# Pré-processamento dos dados bronze
process_data <- function(data) {
  cat("[PROCESSAMENTO] Aplicando pré-processamento aos", nrow(data), "registros\n")
  tryCatch({
        anuncios_brutos <- as_tibble(data)

              #----------------------------------------------------------------------------------------------------------------------
        # LIMPEZA DOS DADOS
        #----------------------------------------------------------------------------------------------------------------------
        anuncios_limpos <- anuncios_brutos %>%
          mutate(
            `preco_R$` = parse_number(`preco_R$`),            # Converte o preço para valor numérico
            area_m2 = parse_number(area_m2),                  # Converte a área para valor numérico
            n_quartos = parse_number(n_quartos),              # Converte o nº de quartos para valor numérico
            n_banheiros = parse_number(n_banheiros),          # Converte o nº de banheiros para valor numérico
            n_vagas_garagem = parse_number(n_vagas_garagem),  # Converte o nº de vagas para valor numérico
            `preco_R$` = ifelse(`preco_R$` < 10000, `preco_R$`*1000, `preco_R$`),    # Preço < 10^3 é multiplicado por 1000
            
            n_vagas_garagem = ifelse(n_vagas_garagem == 0,NA,n_vagas_garagem),   # Transforma 0 em NA
            n_quartos = ifelse(n_quartos == 0,NA,n_quartos),                       # Transforma 0 em NA
            n_banheiros = ifelse(n_banheiros == 0,NA,n_banheiros)                  # Transforma 0 em NA
            
          )

        # Filtra os dados do data frame 
        anuncios_limpos <- anuncios_limpos %>%
          distinct(across(-url), .keep_all = TRUE)  %>% # Remove duplicatas
          filter(
            !is.na(`preco_R$`), # Preço = NA    
            `preco_R$` != 0,    # Preço = 0      
            `preco_R$` < 100000000,     # Preço acima de 100 Milhões é removido   
            !is.na(latitude),   # Latitude = NA
            !is.na(longitude)   # Longitude = NA  
          ) 
        #----------------------------------------------------------------------------------------------------------------------
        # APLICA BUFFER NOS PONTOS
        #----------------------------------------------------------------------------------------------------------------------
        # Criar coluna com distância condicional
        anuncios <- anuncios_limpos %>%
        mutate(
            dist_buffer = ifelse(grepl("000$", cep), 1000, 100) # Muda de acordo com o fim do cep.
        )

        anuncios_sf_utm  <- anuncios %>%
        # Transformar em objeto sf com CRS WGS84
        st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
        # Transformar para uma projeção métrica (exemplo: UTM zona 24S para ES)
        st_transform( crs = 31984) # SIRGAS 2000 / UTM zone 24S 
        
        # Suponha que anuncios_sf_utm seja seu objeto sf com geometria e coluna dist_buffer
        geometrias_buffer <- lapply(seq_len(nrow(anuncios_sf_utm)), function(i) {
        st_buffer(anuncios_sf_utm$geometry[i][[1]], dist = anuncios_sf_utm$dist_buffer[i], nQuadSegs = 30)
        })

        # 6. Substituir geometria e voltar para WGS84
        anuncios_buffered <- anuncios_sf_utm
        st_geometry(anuncios_buffered) <- st_sfc(geometrias_buffer, crs = 31984)
        anuncios_buffered <- st_transform(anuncios_buffered, crs = 4326)

        #----------------------------------------------------------------------------------------------------------------------
        # DEFINIÇÃO DE HEXÁGONOS VÁLIDOS
        #----------------------------------------------------------------------------------------------------------------------
        # Criar hex grid de 5 km
        hex_grid_es <- criar_hexgrid_es(hex_size = 5000)

        # Rodar função para os anuncios coletados
        hex_grid_anuncios <- atribuir_hex_pontos(anuncios_buffered, hex_grid_es)
        
        #----------------------------------------------------------------------------------------------------------------------
        # CONVERSÃO DE GEOMETRY PARA WKT
        #----------------------------------------------------------------------------------------------------------------------
        
        hex_grid_anuncios$geometry_wkt <- st_as_text(hex_grid_anuncios$geometry)
        pre_processed_data <- st_set_geometry(hex_grid_anuncios, NULL)          # opcional, se quiser tirar a coluna geometry original
  
    cat("[PROCESSAMENTO] Registros após limpeza:", nrow(pre_processed_data), "\n")
    pre_processed_data
  }, error = function(e) {
    cat("[PROCESSAMENTO] Erro no processamento:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}



# Função para salvar dados pré-processados no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[SILVER] Salvando dados imobiliários Municipais pré-processados no MinIO\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("silver/imoveis_es/hexagonal/imoveis_es_hex_silver_%s.parquet", timestamp)
    
    write_parquet_to_minio(data, filepath)
    
    cat("[SILVER] Dados salvos com sucesso:", filepath, "\n")
    return(filepath)
    
  }, error = function(e) {
    cat("[SILVER] Erro ao salvar dados imobiliários Municipais pré-processados no MinIO:", conditionMessage(e), "\n")
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
  cat("[PROCESSAMENTO] Processamento finalizado com sucesso!\n")
  cat("============================================================\n")
  
}, error = function(e) {
  cat("[PROCESSAMENTO] Erro fatal:", conditionMessage(e), "\n")
  quit(status = 1)
})
