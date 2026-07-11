
# Script de coleta de dados de anúncios imobiliários a venda do OLX Espírito Santo
# Coleta dados e salva no MinIO (S3)

# Inicializa as bibliotecas necessárias
library(httr)
library(jsonlite)
library(rvest)
library(purrr)
library(stringr)
library(dplyr)
library(tibble)
library(glue)
library(sf)


# Carrega funções utilitárias
source("utils.R")

cat("============================================================\n")
cat("ETAPA 1: COLETA DE DADOS\n")
cat("============================================================\n")

#-------------------------------------------------------------------------------------------------------------------------------------------
# FUNÇÕES UTILIZADAS
#-------------------------------------------------------------------------------------------------------------------------------------------


acessar_pagina_ler_html <- function(url_pagina){
  res <- httr::GET(
    url_pagina,
    httr::add_headers(
      `user-agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
      `accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      `accept-language` = "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
      `referer` = "https://www.google.com/",
      `sec-fetch-mode` = "navigate",
      `sec-fetch-site` = "same-origin",
      `upgrade-insecure-requests` = "1"
    )
  )
  if (httr::status_code(res) != 200) return(FALSE)
  #document <- read_html(res)
  return(read_html(res))
}


numero_paginas <- function(document) {
  total_resultados <- document %>%
    html_element("p.typo-body-small.font-regular.text-neutral-110") %>%
    html_text()
  
  nums <- str_extract_all(total_resultados, "\\d{1,3}(\\.\\d{3})*") %>% unlist()
  
  total <- tail(nums, 1)
  total_num <- as.integer(str_replace_all(total, "\\.", ""))
  total_paginas <- ceiling(total_num / 50)
  return(total_paginas)
}


gerar_intervalos <- function() {

  # Fase 1: 0 até 2.000.000, de 50.000 em 50.000
  fase1_inicio <- seq(0,       2000000 - 50000,  by = 50000)
  fase1_fim    <- fase1_inicio + 49999

  # Fase 2: 2.000.000 até 10.000.000, de 100.000 em 100.000
  fase2_inicio <- seq(2000000, 10000000 - 100000, by = 100000)
  fase2_fim    <- fase2_inicio + 99999

  # Fase 3: 10.000.000 até 1.000.000.000 (par único)
  fase3_inicio <- 10000000
  fase3_fim    <- 10000000000

  # Combina tudo
  data.frame(
    valor_inicio = c(fase1_inicio, fase2_inicio, fase3_inicio),
    valor_fim    = c(fase1_fim,    fase2_fim,    fase3_fim)
  )
}


extrair_detalhes_olx <- function(url, max_tentativas = 3, tempo_entre_tentativas = c(1, 3)) {
  stopifnot(length(tempo_entre_tentativas) == 2)
  
  for (tentativa in seq_len(max_tentativas)) {
    res <- tryCatch({
      httr::GET(
        url,
        httr::add_headers(
          `user-agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
          `accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
          `accept-language` = "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
          `referer` = "https://www.google.com/",
          `upgrade-insecure-requests` = "1"
        )
      )
    }, error = function(e) NULL)
    
    if (is.null(res) || httr::status_code(res) != 200) {
      message(sprintf("Tentativa %d: erro ao acessar URL.", tentativa))
      Sys.sleep(runif(1, tempo_entre_tentativas[1], tempo_entre_tentativas[2]))
      next
    }
    
    html_text <- tryCatch(
      httr::content(res, as = "text", encoding = "UTF-8"),
      error = function(e) NULL
    )
    if (is.null(html_text) || grepl("Are you a human", html_text, ignore.case = TRUE)) {
      message(sprintf("Tentativa %d: conteúdo bloqueado ou suspeito.", tentativa))
      Sys.sleep(runif(1, tempo_entre_tentativas[1], tempo_entre_tentativas[2]))
      next
    }
    
    
    # 1) Matches de endereço
    json_matches_addr <- stringr::str_extract_all(
      html_text,
      "\\{&quot;address&quot;:.*?\\}"
    )[[1]]
    
    campos_end <- c("address", "neighbourhood", "municipality", "zipcode", "uf")
    
    parse_addr <- function(m) {
      json_str <- gsub("&quot;", "\"", m, fixed = TRUE)
      dados <- tryCatch(jsonlite::fromJSON(json_str), error = function(e) list())
      tibble::as_tibble(
        stats::setNames(
          lapply(campos_end, function(c) if (!is.null(dados[[c]])) dados[[c]] else NA_character_),
          campos_end
        )
      )
    }
    
    enderecos_df <- if (length(json_matches_addr)) {
      purrr::map_dfr(json_matches_addr, parse_addr)
    } else {
      tibble::as_tibble(
        stats::setNames(as.list(rep(NA_character_, length(campos_end))), campos_end)
      )
    }
    
    # 2) Matches de adDetail
    mm <- stringr::str_match_all(
      html_text,
      "\"adDetail\"\\s*:\\s*(\\{.*?\\})"
    )[[1]]
    json_matches_detail <- if (nrow(mm)) mm[, 2] else character(0)
    
    campos_det <- c("price", "size", "rooms", "bathrooms", "garage_spaces")
    
    parse_detail <- function(m) {
      dados <- tryCatch(jsonlite::fromJSON(m), error = function(e) list())
      tibble::as_tibble(
        stats::setNames(
          lapply(campos_det, function(c) if (!is.null(dados[[c]])) dados[[c]] else NA_real_),
          campos_det
        )
      )
    }
    
    detalhes_df <- if (length(json_matches_detail)) {
      purrr::map_dfr(json_matches_detail, parse_detail)
    } else {
      tibble::as_tibble(
        stats::setNames(as.list(rep(NA_real_, length(campos_det))), campos_det)
      )
    }
    
    # 3) Harmonização: usa a primeira linha de cada (caso haja múltiplos matches)
    enderecos_row <- enderecos_df[1, , drop = FALSE]
    detalhes_row  <- detalhes_df[1,  , drop = FALSE]
    
    dados_coletados <- dplyr::bind_cols(detalhes_row, enderecos_row)
    return(dados_coletados)  # sucesso -> sai da função
  }
  
  # Se todas as tentativas falharem: retorna tibble com NAs
  campos_det <- c("price", "size", "rooms", "bathrooms", "garage_spaces")
  campos_end <- c("address", "neighbourhood", "municipality", "zipcode", "uf")
  
  tibble::as_tibble(
    c(
      stats::setNames(as.list(rep(NA_real_, length(campos_det))), campos_det),
      stats::setNames(as.list(rep(NA_character_, length(campos_end))), campos_end)
    )
  )
}




collect_raw_data <- function() {
  tryCatch({
    # gerar_intervalos <- function() {
      
    #   # Teste: 0 até 200.000, de 50.000 em 50.000
    #   inicio <- seq(0, 100000 - 50000, by = 50000)
    #   fim    <- inicio + 49999
      
    #   # Combina tudo
    #   data.frame(
    #     valor_inicio = format(inicio, scientific = FALSE, trim = TRUE),
    #     valor_fim    = format(fim, scientific = FALSE, trim = TRUE)
    #   )
    # }
    
    intervalos <- gerar_intervalos()
    
    
    dados_anuncios <- map(seq_len(nrow(intervalos)),function(i){
      
      valor_inicio <- intervalos$valor_inicio[i]
      valor_fim <- intervalos$valor_fim[i]
      
      
      url_base <- paste0("https://www.olx.com.br/imoveis/venda/estado-es?ps=",valor_inicio,"&pe=",valor_fim,"&o=")
      
      url_base_num_paginas <- paste0(url_base,1)
      print(url_base_num_paginas)
      document <- acessar_pagina_ler_html(url_base_num_paginas)
      
      ##----------------------------------------------------------------------------------------------------------------
      ##                                        VALIDA SE HÁ PÁGINAS VÁLIDAS NO INTERVALO DE BUSCA
      ##----------------------------------------------------------------------------------------------------------------
      # Pula se retornou FALSE (status != 200)
      if (isFALSE(document)) {
        cat(sprintf("  ✖ Erro ao acessar intervalo %s-%s\n", valor_inicio, valor_fim))
        return(invisible(NULL))
      }
      
      
      total_paginas <- numero_paginas(document)
      
      # Pula se não houver resultados
      if (is.na(total_paginas) || total_paginas == 0) {
        cat(sprintf("[%d/%d] Intervalo %s-%s sem resultados, pulando.\n",
                    i, nrow(intervalos), valor_inicio, valor_fim))
        return(invisible(NULL))
      }
      
      cat(sprintf("[%d/%d] Intervalo: %s-%s | Páginas encontradas: %d\n",
                  i, nrow(intervalos), valor_inicio, valor_fim, total_paginas))
      
      ##----------------------------------------------------------------------------------------------------------------
      ##                                    ITERA AS PÁGINAS E COLETA OS DADOS
      ##----------------------------------------------------------------------------------------------------------------
      resultados <- map(seq_len(total_paginas), function(pagina) {
        
        cat(sprintf("  → Processando página %d de %d\n", pagina, total_paginas))
        
        url_iteracao <- paste0(url_base,pagina)
        document_pag <- acessar_pagina_ler_html(url_iteracao)
        if (isFALSE(document_pag)) return(NULL)
        anuncios_html <- document_pag %>% html_elements("section.olx-adcard")
        
        
        #Sys.sleep(runif(1, 1, 3))  # pausa entre requisições, variando de 1 a 3
        
        data.frame(
          url = anuncios_html %>% html_element("a") %>% html_attr("href"),
          titulos = anuncios_html %>% html_element("h2") %>% html_text(trim = TRUE)
        )
        
        
      })
      # Lista com um vetor de títulos por página
      # Para achatar em um único vetor:
      return(bind_rows(resultados))  
      
    })
    anuncios_olx <- bind_rows(dados_anuncios)
    
    #-------------------------------------------------------------------------------------------------------------------------------------------
    # CRIA UM DATA FRAME COM OS DADOS E ADICIONA AS COLUNAS QUE SERÃO POSTERIORMENTE PREENCHIDAS
    #-------------------------------------------------------------------------------------------------------------------------------------------
    
    # Garante que as colunas existem
    novas_colunas <- c( "preco_R$","area_m2", "n_quartos", "n_banheiros", "n_vagas_garagem","endereco","bairro","municipio","cep","UF")
    
    
    for (col in novas_colunas) {
      if (!col %in% names(anuncios_olx)) {
        anuncios_olx[[col]] <- NA_character_
      }
    }
    
    #-------------------------------------------------------------------------------------------------------------------------------------------
    # COLETA DE DADOS DOS LINKS OLX 
    #-------------------------------------------------------------------------------------------------------------------------------------------
    # Dados de coletas anteriores
    anuncios_olx_anteriores <- read_latest_parquet_from_minio("bronze/imoveis_es/municipal/") 

    # Somente URLs nunca coletadas
    anuncios_pendentes <- anuncios_olx %>%
    filter(!is.na(url), nzchar(url)) %>%
    distinct(url, .keep_all = TRUE) %>%
    anti_join(
        anuncios_olx_anteriores %>% distinct(url),
        by = "url"
    )

    anuncios_olx <- anuncios_pendentes
    # Inicializa vetor de falhas
    urls_falhas <- character(0)
    
    # Loop principal - itera sobre todas as linhas do data frame
    for (i in seq_len(nrow(anuncios_olx))) {
      url <- anuncios_olx[i, 1]
      
      if (!is.na(url)) {
        info <- extrair_detalhes_olx(url)
        
        if (all(is.na(unlist(info)))) {
          urls_falhas <- c(urls_falhas, url)
        } else {
          anuncios_olx[i, "preco_R$"] <- info$price
          anuncios_olx[i, "area_m2"] <- info$size
          anuncios_olx[i, "n_quartos"] <- info$rooms
          anuncios_olx[i, "n_banheiros"] <- info$bathrooms
          anuncios_olx[i, "n_vagas_garagem"] <- info$garage_spaces
          anuncios_olx[i, "endereco"] <- info$address
          anuncios_olx[i, "bairro"] <- info$neighbourhood
          anuncios_olx[i, "municipio"] <- info$municipality
          anuncios_olx[i, "cep"] <- info$zipcode
          anuncios_olx[i, "UF"] <- info$uf
        }
      }
      
      # Delay para evitar bloqueio
      #Sys.sleep(runif(1, 1, 3))
      
      cat("Processado:", i, "/", nrow(anuncios_olx), "\n")
    }
    
    # Cria um data frame com as valores anteriores e os atuais
    anuncios_olx_final <- bind_rows(
        anuncios_olx_anteriores,
        anuncios_olx
        ) %>%
        distinct(url, .keep_all = TRUE)

    # Quantidade de casos em que não foi possível coletar informações antes
    qtd_na_antes <- sum(is.na(anuncios_olx_final[["preco_R$"]]))

    #-------------------------------------------------------------------------------------------------------------------------------------------
    # REFAZ A COLETA DOS ANÚNCIOS QUE FALHARAM
    #-------------------------------------------------------------------------------------------------------------------------------------------
    # Índices das linhas ainda sem preço
    idx_na_preco <- which(is.na(anuncios_olx_final[["preco_R$"]]))
    
    urls_falhas_retry <- character(0)
    # Loop de reprocessamento
    for (j in seq_along(idx_na_preco)) {
      i <- idx_na_preco[j]
      
      # Pega a URL da primeira coluna (ajuste se sua coluna tiver nome, ex.: anuncios_olx[i, "url"])
      #url <- as.character(anuncios_olx_final[i, "url"])
      url <- as.character(anuncios_olx_final$url[i])
      
      if (!is.na(url) && nzchar(url)) {
        info <- extrair_detalhes_olx(url)
        
        if (all(is.na(unlist(info)))) {
          urls_falhas_retry <- c(urls_falhas_retry, url)
        } else {
          anuncios_olx_final[i, "preco_R$"]           <- info$price
          anuncios_olx_final[i, "area_m2"]            <- info$size
          anuncios_olx_final[i, "n_quartos"]          <- info$rooms
          anuncios_olx_final[i, "n_banheiros"]        <- info$bathrooms
          anuncios_olx_final[i, "n_vagas_garagem"]    <- info$garage_spaces
          anuncios_olx_final[i, "endereco"]           <- info$address
          anuncios_olx_final[i, "bairro"]             <- info$neighbourhood
          anuncios_olx_final[i, "municipio"]          <- info$municipality
          anuncios_olx_final[i, "cep"]                <- info$zipcode
          anuncios_olx_final[i, "UF"]                 <- info$uf
        }
      }
      
      # Delay anti-bloqueio
      #Sys.sleep(runif(1, 1, 3))
      cat("Reprocessado:", j, "/", length(idx_na_preco), "(linha", i, ")\n")
    }
    

    qtd_na_depois <- sum(is.na(anuncios_olx_final[["preco_R$"]]))

    houve_correcao <- qtd_na_depois < qtd_na_antes
    tem_novos <- nrow(anuncios_olx) > 0

    if (!tem_novos && !houve_correcao) {
    cat("[COLETA] Nenhum anúncio novo e nenhuma pendência corrigida.\n")
    return(NULL)
    }

    return(anuncios_olx_final)
    
  }, error = function(e) {
    cat("[IMOVEIS_ES] Erro na coleta:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}



# Função para salvar no MinIO via DuckDB
save_to_minio_duckdb <- function(data) {
  cat("[COLETA] Salvando dados no MinIO via DuckDB\n")
  
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    #filepath <- sprintf("coleta/raw_data_%s.parquet", timestamp)
    filepath <- sprintf("bronze/imoveis_es/municipal/anuncios_brutos_%s.parquet", timestamp)
    
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
  # Coleta os dados
  data <- collect_raw_data()
  
  # Só salva se não for NULL
  if (!is.null(data)) {
    # Salva no MinIO via DuckDB
    filepath <- save_to_minio_duckdb(data)
    
    cat("============================================================\n")
    cat("[COLETA] Coleta finalizada com sucesso!\n")
    cat("[COLETA] Arquivo:", filepath, "\n")
    cat("============================================================\n")
  } else {
    cat("============================================================\n")
    cat("[COLETA] Nenhum dado novo. \n")
    cat("============================================================\n")
  }
  
  
}, error = function(e) {
  cat("[COLETA] Erro fatal:", conditionMessage(e), "\n")
  quit(status = 1)
})

