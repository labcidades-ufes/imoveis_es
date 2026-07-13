#!/usr/bin/env Rscript

# Script de coleta de dados de geolocalização dos anúncios imobiliários a venda do OLX Espírito Santo
# Coleta dados e salva no MinIO (S3)

# Inicializa as bibliotecas necessárias
library(httr)
library(jsonlite)
library(geocodebr)
library(sf)
library(geobr)
library(dplyr)

# Carrega funções utilitárias
source("utils.R")
#-------------------------------------------------------------------------------------------------------------------------------------------
# FUNÇÕES UTILIZADAS
#-------------------------------------------------------------------------------------------------------------------------------------------


read_from_minio_duckdb <- function() {
  cat("[COLETA] Lendo dados do MinIO via DuckDB\n")

  tryCatch({
    prefixo <- "bronze/imoveis_es/municipal/"
    bucket <- Sys.getenv("MINIO_BUCKET", "airflow")

    if (!minio_prefix_exists(prefixo, bucket)) {
      stop(sprintf("Container %s nao encontrado no bucket %s", prefixo, bucket))
    }

    arquivos <- list_parquet_files_in_minio(prefixo, bucket)
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






# ---------------------------------------------------------------------------
# Configuração da API do Google
# ---------------------------------------------------------------------------

# Se ficar como "", o script usará apenas geocodebr
minha_api_key <- ""

# Função para obter geolocalização a partir de um CEP usando Google Maps API
coleta_geoloc_google <- function(cep, api_key) {
  
  url <- paste0(
    "https://maps.googleapis.com/maps/api/geocode/json?address=",
    URLencode(cep),
    "&components=country:BR",
    "&key=", api_key
  )
  
  res <- httr::GET(url)
  
  if (status_code(res) != 200) {
    stop("Erro ao consultar a API do Google Maps")
  }
  
  dados <- fromJSON(content(res, "text", encoding = "UTF-8"), simplifyVector = FALSE)
  
  if (dados$status != "OK" || length(dados$results) == 0) {
    stop(paste("Erro na resposta da API:", dados$status))
  }
  
  result <- dados$results[[1]]
  
  list(
    cep_1    = cep,
    latitude = result$geometry$location$lat,
    longitude = result$geometry$location$lng
  )
}

# ---------------------------------------------------------------------------

# Geocodificação via geocodebr (mediana das coordenadas por CEP)
coleta_geoloc_geocodebr <- function(cep) {
  
  df_ceps <- tryCatch(
    geocodebr::busca_por_cep(
      cep = cep,
      resultado_sf = FALSE,
      verboso = FALSE
    ),
    error = function(e) NULL
  )
  
  # Se não encontrou nada, devolve NA
  if (is.null(df_ceps) || nrow(df_ceps) == 0) {
    return(data.frame(
      cep_1    = cep,
      latitude = NA_real_,
      longitude = NA_real_
    ))
  }
  
  # Mediana das coordenadas (1 ou vários pontos)
  lat_med <- stats::median(df_ceps$lat, na.rm = TRUE)
  lon_med <- stats::median(df_ceps$lon, na.rm = TRUE)
  
  data.frame(
    cep_1    = cep,
    latitude = lat_med,
    longitude = lon_med
  )
}



# Função para coletar dados de geolocalização
coleta_geoloc <- function(anuncios_brutos) {
  cat("[COLETA] Iniciando coleta de dados de geolocalização...\n")
  
  tryCatch({
    # garante que está em tibble/data.frame "limpo"
    anuncios_brutos <- as_tibble(anuncios_brutos)
    
    # Padroniza os CEPs
    anuncios_brutos <- anuncios_brutos |>
      dplyr::mutate(cep = enderecobr::padronizar_ceps(cep))
    
    # -----------------------------------------------------------------------
    # Fluxo condicional: apenas geocodebr  x  Google + geocodebr
    # -----------------------------------------------------------------------
    
    if (minha_api_key == "") {
      
      # Caso 1: sem chave do Google → apenas geocodebr para todos os CEPs
      geoloc_list <- lapply(anuncios_brutos$cep, coleta_geoloc_geocodebr)
      geoloc_df   <- dplyr::bind_rows(geoloc_list)
      
      anuncios_geoloc <- cbind(anuncios_brutos, geoloc_df) |>
        dplyr::select(-cep_1)
      
    } else {
      
      # Caso 2: com chave do Google → Google + recoleta dos inválidos via geocodebr
      
      # Primeira coleta: Google
      geolocalizacao <- lapply(
        anuncios_brutos$cep,
        coleta_geoloc_google,
        api_key = minha_api_key
      )
      geolocalizacao_df <- do.call(rbind, lapply(geolocalizacao, as.data.frame))
      
      anuncios_geoloc <- cbind(anuncios_brutos, geolocalizacao_df) |>
        dplyr::select(-cep_1) |>
        dplyr::mutate(primary_key = dplyr::row_number())
      
      # Limites do estado do ES – tenta sempre o ano mais recente possível
      ano_atual   <- as.integer(format(Sys.Date(), "%Y"))
      anos_tentar <- seq(from = ano_atual, to = 2000, by = -1)
      
      Limites_ES <- NULL
      
      for (ano in anos_tentar) {
        Limites_ES <- tryCatch(
          geobr::read_state(code_state = "ES", year = ano),
          error = function(e) NULL
        )
        
        if (!is.null(Limites_ES)) {
          message(paste("Usando limites do ES para o ano", ano))
          break
        }
      }
      
      if (is.null(Limites_ES)) {
        stop("Não foi possível carregar os limites do ES para nenhum ano entre 2000 e o ano atual.")
      }
      
      # Verifica quais pontos do Google caem dentro do ES
      Dentro_es <- anuncios_geoloc |>
        sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
        sf::st_transform(sf::st_crs(Limites_ES)) |>
        dplyr::mutate(
          valido = sf::st_within(geometry, Limites_ES, sparse = FALSE)[, 1])
      
      # Registros fora do ES ou inválidos
      registros_invalidos <- Dentro_es |>
        dplyr::filter(!valido) |>
        sf::st_drop_geometry() |>
        dplyr::select(primary_key, cep)
      
      # Só faz recoleta se existir algum inválido
      if (nrow(registros_invalidos) > 0) {
        geoloc_recoletada_list <- lapply(
          registros_invalidos$cep,
          coleta_geoloc_geocodebr
        )
        
        geoloc_recoletada_df <- dplyr::bind_rows(geoloc_recoletada_list) |>
          dplyr::mutate(primary_key = registros_invalidos$primary_key)
        
        # Atualiza latitude/longitude apenas onde houve recoleta válida
        anuncios_geoloc <- anuncios_geoloc |>
          dplyr::left_join(
            geoloc_recoletada_df,
            by = "primary_key",
            suffix = c("", "_novo")
          ) |>
          dplyr::mutate(
            latitude  = dplyr::if_else(!is.na(latitude_novo),  latitude_novo,  latitude),
            longitude = dplyr::if_else(!is.na(longitude_novo), longitude_novo, longitude)
          ) |>
          dplyr::select(-latitude_novo, -longitude_novo)
      }
      
      # Remove colunas auxiliares
      anuncios_geoloc <- anuncios_geoloc |>
        dplyr::select(-primary_key)
    }
    
    cat("[COLETA] Coletados", nrow(anuncios_geoloc), "registros de geolocalização.\n")
    return(anuncios_geoloc)
    
  }, error = function(e) {
    cat("[COLETA] Erro ao coletar dados:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}





# Função para salvar no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[COLETA] Salvando dados no MinIO via DuckDB\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("bronze/imoveis_es/hexagonal/anuncios_com_geolocalizacao_%s.parquet", timestamp)

    write_parquet_to_minio(data, filepath)
    
    cat("[COLETA] Dados salvos com sucesso:", filepath, "\n")
    return(filepath)
    
  }, error = function(e) {
    cat("[COLETA] Erro ao salvar no MinIO:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# Execução principal
tryCatch({

  # -------------------------------------------------------------------------
  # 1. Lê os dados brutos mais recentes
  # -------------------------------------------------------------------------

  raw_data <- read_latest_parquet_from_minio("bronze/imoveis_es/municipal/") %>%
    as_tibble() %>%
    filter(!is.na(url)) %>%
    distinct(url, .keep_all = TRUE)

  # -------------------------------------------------------------------------
  # 2. Lê o último arquivo geolocalizado
  # -------------------------------------------------------------------------

  geoloc_anterior <- tryCatch(
    read_latest_parquet_from_minio(
      "bronze/imoveis_es/hexagonal/"
    ),
    error = function(e) {
      cat(
        "[COLETA] Nenhum arquivo geolocalizado anterior encontrado:",
        conditionMessage(e),
        "\n"
      )

      tibble::tibble()
    }
  )

  geoloc_anterior <- as_tibble(geoloc_anterior)

  # -------------------------------------------------------------------------
  # 3. Identifica URLs novas
  # -------------------------------------------------------------------------

  if (
    nrow(geoloc_anterior) == 0 ||
    !"url" %in% names(geoloc_anterior)
  ) {
    # Primeira execução: todos os registros são novos
    anuncios_novos <- raw_data
  } else {
    anuncios_novos <- raw_data %>%
      anti_join(
        geoloc_anterior %>%
          filter(!is.na(url), nzchar(url)) %>%
          distinct(url),
        by = "url"
      )
  }

  # -------------------------------------------------------------------------
  # 4. Identifica URLs antigas com coordenadas ausentes
  # -------------------------------------------------------------------------

  if (
    nrow(geoloc_anterior) > 0 &&
    all(c("url", "latitude", "longitude") %in% names(geoloc_anterior))
  ) {
    urls_reprocessar <- geoloc_anterior %>%
      filter(
        !is.na(url),
        nzchar(url),
        is.na(latitude) | is.na(longitude)
      ) %>%
      distinct(url) %>%
      select(url)

    # Usa os dados brutos mais recentes dessas URLs
    anuncios_reprocessar <- raw_data %>%
      semi_join(urls_reprocessar, by = "url")
  } else {
    anuncios_reprocessar <- raw_data[0, ]
  }

  # -------------------------------------------------------------------------
  # 5. Junta URLs novas e URLs antigas sem coordenadas
  # -------------------------------------------------------------------------

  anuncios_pendentes <- bind_rows(
    anuncios_novos,
    anuncios_reprocessar
  ) %>%
    distinct(url, .keep_all = TRUE)

  cat(
    "[COLETA] URLs novas:",
    nrow(anuncios_novos),
    "\n"
  )

  cat(
    "[COLETA] URLs antigas para reprocessar:",
    nrow(anuncios_reprocessar),
    "\n"
  )

  # -------------------------------------------------------------------------
  # 6. Geolocaliza somente os registros pendentes
  # -------------------------------------------------------------------------

  if (nrow(anuncios_pendentes) > 0) {
    novos_geolocalizados <- coleta_geoloc(anuncios_pendentes)
  } else {
    cat("[COLETA] Nenhuma URL pendente de geolocalização.\n")

    novos_geolocalizados <- tibble::tibble(
      url = character(),
      latitude = numeric(),
      longitude = numeric()
    )
  }

  # -------------------------------------------------------------------------
  # 7. Recupera as coordenadas anteriores
  # -------------------------------------------------------------------------

  if (
    nrow(geoloc_anterior) > 0 &&
    all(c("url", "latitude", "longitude") %in% names(geoloc_anterior))
  ) {
    coordenadas_anteriores <- geoloc_anterior %>%
      filter(!is.na(url), nzchar(url)) %>%
      select(url, latitude, longitude) %>%
      distinct(url, .keep_all = TRUE)
  } else {
    coordenadas_anteriores <- tibble::tibble(
      url = character(),
      latitude = numeric(),
      longitude = numeric()
    )
  }

  # -------------------------------------------------------------------------
  # 8. Separa as coordenadas novas ou reprocessadas
  # -------------------------------------------------------------------------

  coordenadas_atualizadas <- novos_geolocalizados %>%
    select(url, latitude, longitude) %>%
    distinct(url, .keep_all = TRUE)

  # As coordenadas atualizadas devem vir primeiro.
  # Assim, distinct() mantém o resultado mais recente da URL.
  coordenadas_finais <- bind_rows(
    coordenadas_atualizadas,
    coordenadas_anteriores
  ) %>%
    distinct(url, .keep_all = TRUE)

  # -------------------------------------------------------------------------
  # 9. Junta as coordenadas aos dados brutos mais recentes
  # -------------------------------------------------------------------------

  data <- raw_data %>%
    select(-any_of(c("latitude", "longitude"))) %>%
    left_join(
      coordenadas_finais,
      by = "url"
    )

  # -------------------------------------------------------------------------
  # 10. Salva o resultado completo
  # -------------------------------------------------------------------------

  filepath <- save_to_minio_duckdb(data)

  cat("============================================================\n")
  cat("[COLETA] Coleta finalizada com sucesso!\n")
  cat("[COLETA] Arquivo:", filepath, "\n")
  cat("[COLETA] Total de registros:", nrow(data), "\n")
  cat("============================================================\n")

}, error = function(e) {
  cat("[COLETA] Erro fatal:", conditionMessage(e), "\n")
  quit(status = 1)
})