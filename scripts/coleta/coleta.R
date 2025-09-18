#-------------------------------------------------------------------------------------------------------------------------------------------
# ITENS INICIAIS NECESSÁRIOS
#-------------------------------------------------------------------------------------------------------------------------------------------
# Inicializa as bibliotecas necessárias
library(rvest)
library(httr)
library(purrr)
library(stringr)
library(dplyr)
library(jsonlite)
library(tibble)
library(glue)
library(tcltk)
library(sf)

#-------------------------------------------------------------------------------------------------------------------------------------------
# CONTABILIZA O NÚMERO DE PÁGINAS COM ANÚNCIOS IMOBILIÁRIOS DE VENDA NO ES
#-------------------------------------------------------------------------------------------------------------------------------------------
# Função para verificar se uma página tem anúncios
tem_anuncios <- function(pagina_url) {
  #res <- httr::GET(pagina_url, httr::add_headers(`user-agent` = "Mozilla/5.0"))
  # Simula a entrada de um usuário
  res <- httr::GET(
    pagina_url,
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
  
  document <- read_html(res)
  anuncios <- document %>% html_elements("section.olx-adcard")
  
  return(length(anuncios) > 0)
}



# Descobrindo o limite real de páginas
base_url <- "https://www.olx.com.br/imoveis/venda/estado-es?o="
num_paginas <- 1

repeat {
  url_atual <- paste0(base_url, num_paginas)
  cat("Verificando página:", num_paginas, "\n")
  
  if (!tem_anuncios(url_atual)) {
    num_paginas <- num_paginas - 1
    break
  }
  
  num_paginas <- num_paginas + 1
  
  
  
  # limite de segurança (por precaução)
  if (num_paginas > 200) break
  Sys.sleep(runif(1, 1, 3))  # pausa entre requisições, variando de 1 a 3
}

cat("Última página com anúncios:", num_paginas, "\n")




#-------------------------------------------------------------------------------------------------------------------------------------------
# DECLARA AS LISTAS QUE SERÃO USADAS
#-------------------------------------------------------------------------------------------------------------------------------------------

# Inicializa as listas de itens dos anuncios

url_anuncios <- list()  # lista de urls
titulo_anuncios <- list() # lista de titulos





#-------------------------------------------------------------------------------------------------------------------------------------------
# LOOP QUE COLETA OS DADOS GERAIS DE ANÚNCIOS OLX
#-------------------------------------------------------------------------------------------------------------------------------------------

# Iteração inicial
i <- 1

# Itera sobre todos as páginas coletando os itens
while (i <= num_paginas) {
  
  # Altera o URL para cada página
  url_iterado <- paste0(base_url, i)
  cat("Coletando página:", i, "\n")
  
  # Simula a entrada de um usuário
  res <- httr::GET(
    url_iterado,
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
  
  
  document <- read_html(res)
  
  # Lista de elementos HTML
  anuncios_html <- document %>% html_elements("section.olx-adcard")
  
  
  # Coleta os dados dos anúncios presentes na página atual
  url_anuncios <- append(url_anuncios, list(anuncios_html %>% html_element("a") %>% html_attr("href")))
  
  titulo_anuncios <- append(titulo_anuncios, list(anuncios_html %>%    html_element("h2") %>%    html_text(trim = TRUE)))
  
  # I aumenta de 1 em 1, até alcançar o valor limite
  i <- i + 1
  
  Sys.sleep(runif(1, 1, 3))  # pausa entre requisições, variando de 1 a 3
}





#-------------------------------------------------------------------------------------------------------------------------------------------
# CRIA UM DATA FRAME COM OS DADOS E ADICIONA AS COLUNAS QUE SERÃO POSTERIORMENTE PREENCHIDAS
#-------------------------------------------------------------------------------------------------------------------------------------------

# Cria um data frame com os dados coletados
anuncios_olx <- data.frame(
  
  url = unlist(url_anuncios),
  
  titulo = unlist(titulo_anuncios)
)

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
  Sys.sleep(runif(1, 1, 3))
  
  cat("Processado:", i, "/", nrow(anuncios_olx), "\n")
}


#-------------------------------------------------------------------------------------------------------------------------------------------
# REFAZ A COLETA DOS ANÚNCIOS QUE FALHARAM
#-------------------------------------------------------------------------------------------------------------------------------------------
# Índices das linhas ainda sem preço
idx_na_preco <- which(is.na(anuncios_olx[["preco_R$"]]))

urls_falhas_retry <- character(0)
# Loop de reprocessamento
for (j in seq_along(idx_na_preco)) {
  i <- idx_na_preco[j]
  
  # Pega a URL da primeira coluna (ajuste se sua coluna tiver nome, ex.: anuncios_olx[i, "url"])
  url <- as.character(anuncios_olx[i, "url"])
  
  if (!is.na(url) && nzchar(url)) {
    info <- extrair_detalhes_olx(url)
    
    if (all(is.na(unlist(info)))) {
      urls_falhas_retry <- c(urls_falhas_retry, url)
    } else {
      anuncios_olx[i, "preco_R$"]           <- info$price
      anuncios_olx[i, "area_m2"]            <- info$size
      anuncios_olx[i, "n_quartos"]          <- info$rooms
      anuncios_olx[i, "n_banheiros"]        <- info$bathrooms
      anuncios_olx[i, "n_vagas_garagem"]    <- info$garage_spaces
      anuncios_olx[i, "endereco"]           <- info$address
      anuncios_olx[i, "bairro"]             <- info$neighbourhood
      anuncios_olx[i, "municipio"]          <- info$municipality
      anuncios_olx[i, "cep"]                <- info$zipcode
      anuncios_olx[i, "UF"]                 <- info$uf
    }
  }
  
  # Delay anti-bloqueio
  Sys.sleep(runif(1, 1, 3))
  cat("Reprocessado:", j, "/", length(idx_na_preco), "(linha", i, ")\n")
}

#-------------------------------------------------------------------------------------------------------------------------------------------
# SALVA O DATA FRAME COLETADO
#-------------------------------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar os anuncios na coleta inicial")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Anuncios_brutos.rds")
  saveRDS(anuncios_olx, file = caminho_arquivo)
}

