#-------------------------------------------- ITENS INICIAIS NECESSÁRIOS --------------------------------------------------
# Inicializa as bibliotecas necessárias
library(rvest)
library(httr)
library(purrr)
library(stringr)
library(dplyr)


#------------------------------------------ CONTABILIZA O NÚMERO DE PÁGINAS ------------------------------------------------

# Função para verificar se uma página tem anúncios
tem_anuncios <- function(pagina_url) {
  res <- httr::GET(pagina_url, httr::add_headers(`user-agent` = "Mozilla/5.0"))
  
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


#---------------------------------------- DECLARA AS LISTAS QUE SERÃO USADAS ----------------------------------------------


# Inicializa as listas de itens dos anuncios

url_anuncios <- list()  # lista de urls
titulo_anuncios <- list() # lista de titulos
preco_anuncios <- list() # lista de preços
endereco_anuncios <- list() # lista de enredeços
area_anuncios <- list() # lista de áreas


#---------------------------------------- LOOP QUE COLETA OS DADOS DO OLX ------------------------------------------------

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
  
  preco_anuncios <- append(preco_anuncios, list(anuncios_html %>%    html_element("h3") %>%    html_text(trim = TRUE)))
  
  endereco_anuncios <- append(endereco_anuncios, list(anuncios_html %>%
    html_element("p.olx-adcard__location") %>%    html_text(trim = TRUE)))
  
  area_anuncios <- append(area_anuncios, list(anuncios_html %>%    map_chr(~ .x %>%  
    html_elements("div.olx-adcard__detail") %>%   html_text(trim = TRUE) %>%                                                                       keep(~ str_detect(.x, "m²")) %>%
    first()  # pega a primeira ocorrência com m² (ou NA)
  )))
  
  # I aumenta de 1 em 1, até alcançar o valor limite
  i <- i + 1
  
  Sys.sleep(runif(1, 1, 3))  # pausa entre requisições, variando de 1 a 3
}



#---------------------------------------- CRIA UM DATA FRAME COM OS DADOS ------------------------------------------------


# Cria um data frame com os dados coletados
anuncios_olx <- data.frame(
  
  url = unlist(url_anuncios),
  
  titulo = unlist(titulo_anuncios),
  
  preco = unlist(preco_anuncios),
  
  endereco = unlist(endereco_anuncios),
  
  area = unlist(area_anuncios)
)



#--------------------------------------- SALVA O DATA FRAME EM UM ARQUIVO CSV ----------------------------------------------


# write the data to a CSV file

write.csv(anuncios_olx, "D:/Projeto UFES/Scripts_Projeto/Imoveis_olx.csv", row.names = FALSE)
  