
# Responsável por receber o data frame com CEPS e converter para Latitude e Longitude.
#-------------------------------------------------------------------------------------------------------------------------------------------
# ITENS INICIAIS NECESSÁRIOS
#-------------------------------------------------------------------------------------------------------------------------------------------

## Instalar pacotes (se necessário)
#install.packages(c("httr", "jsonlite", "URLencode"))

library(httr)
library(jsonlite)
library(tcltk)
library(geocodebr)
library(sf)


#-------------------------------------------------------------------------------------------------------------------------------------------
# LÊ RDS COM ANÚNCIOS BRUTOS (SEM GEOLOCALIZAÇÃO DE CEP) - Anuncios_brutos.rds
#-------------------------------------------------------------------------------------------------------------------------------------------

# Recebe os dados com ceps
anuncios_brutos <- readRDS(file.choose()) 


#-------------------------------------------------------------------------------------------------------------------------------------------
# FUNÇÕES UTILIZADAS
#-------------------------------------------------------------------------------------------------------------------------------------------

# Função para obter geolocalização a partir de um CEP usando Google Maps API
coleta_geoloc_google <- function(cep, api_key = "AIzaSyDV6pmcrRkLvylpoQyh4Z5laZT-sI1v6zQ") {
  
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
  
  return(list(
    cep_1 = cep,
    #endereco = result$formatted_address,
    latitude = result$geometry$location$lat,
    longitude = result$geometry$location$lng
  ))
}


# Substitua pela sua chave de API do Google
#minha_api_key <- "AIzaSyDV6pmcrRkLvylpoQyh4Z5laZT-sI1v6zQ"

# Função para obter geolocalização a partir de um CEP usando Geocodebr
coleta_geoloc_geocodebr <- function(cep,primary_key){
  df_ceps <- geocodebr::busca_por_cep(
    cep = ceps,
    resultado_sf = FALSE,
    verboso = FALSE
  )
  
  return(list(
    cep_1 = cep,
    primary_key = primary_key,
    latitude = df_ceps$lat,
    longitude = df_ceps$lon
  ))
}
#-------------------------------------------------------------------------------------------------------------------------------------------
# PRIMEIRA COLETA DE GEOLOCALIZAÇÃO
#-------------------------------------------------------------------------------------------------------------------------------------------

# Aplicando a função uma única vez por elemento
geolocalizacao <- lapply(anuncios_brutos$cep, coleta_geoloc_google)

# Convertendo a lista de listas para data frame
geolocalizacao_df <- do.call(rbind, lapply(geolocalizacao, as.data.frame))

# Juntando ao data frame original
anuncios_geoloc <- cbind(anuncios_brutos, geolocalizacao_df)

anuncios_geoloc <- anuncios_geoloc %>%
  select(-cep_1) # remove a coluna auxiliar e a de municipios

anuncios_geoloc <- anuncios_geoloc %>%
  mutate(primary_key = row_number())

#-------------------------------------------------------------------------------------------------------------------------------------------
# SEGUNDA COLETA DE GEOLOCALIZAÇÃO - RECOLETA DE ITENS INVÁLIDOS
#-------------------------------------------------------------------------------------------------------------------------------------------

# Coleta os limites do Estado do ES
Limites_ES <- read_state(code_state = "ES", year = 2020)

# Confere se todos os dados coletados com o Google Maps estão dentro do ES e os marca
Dentro_es <- anuncios_geoloc %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform( st_crs(Limites_ES)) %>%
  mutate( valido = st_within(.,Limites_ES, sparse = FALSE)[,1])

# Adiciona a coluna primary_key aos registros inválidos
registros_invalidos <- Dentro_es %>%
  filter(!valido) %>%
  select(primary_key, cep)

# Recoleta os dados
geoloc_recoletada <- Map(coleta_geoloc_geocodebr,
                         cep = registros_invalidos$cep,
                         primary_key = registros_invalidos$primary_key)

geoloc_recoletada_df <- do.call(rbind, lapply(geoloc_recoletada, as.data.frame))


# Atualiza latitude e longitude apenas nos registros com primary_key presente em geoloc_recoletada_df
anuncios_geoloc <- anuncios_geoloc %>%
  left_join(geoloc_recoletada_df, by = "primary_key", suffix = c("", "_novo")) %>%
  mutate(
    latitude = if_else(primary_key %in% geoloc_recoletada_df$primary_key & !is.na(latitude_novo), latitude_novo, latitude),
    longitude = if_else(primary_key %in% geoloc_recoletada_df$primary_key & !is.na(longitude_novo), longitude_novo, longitude)
  ) %>%
  select(-latitude_novo, -longitude_novo,-cep_1,-primary_key)




#-------------------------------------------------------------------------------------------------------------------------------------------
# SALVA O DATA FRAME COM GEOLOCALIZAÇÃO
#-------------------------------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar os anuncios com a coleta concluída")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Anuncios_geoloc.rds")
  saveRDS(anuncios_geoloc, file = caminho_arquivo)
}
