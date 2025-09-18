#-----------------------------------------------------------------------------------------------------------------------
# CARREGAR PACOTES NECESSÁRIOS
#-----------------------------------------------------------------------------------------------------------------------
library(shiny)
library(leaflet)
library(geobr)
library(dplyr)
library(htmltools)
library(tcltk)


#------------------------------------------------------------------------------------------------------------------------
# IMPORTA O ARQUIVO Indicadores_munic.RDS
#------------------------------------------------------------------------------------------------------------------------

# Recebe os indicadores
indicadores_munic_ES <- readRDS(file.choose()) 


#------------------------------------------------------------------------------------------------------------------------
# PREPARAR DADOS
#------------------------------------------------------------------------------------------------------------------------
# Baixar os dados dos limites municipais do Espírito Santo (ES)
tryCatch({
  munis_es_geo <- read_municipality(code_muni = "ES", year = 2020)
}, error = function(e) {
  stop("Falha ao baixar os dados geográficos. Verifique sua conexão com a internet.")
})

# Juntar os dados geográficos com os dados de população
munis_es <- (indicadores_munic_ES %>% mutate(municipio = tolower(municipio))) %>%
  left_join(
    (munis_es_geo %>% select(name_muni, geom) %>% mutate(municipio = tolower(name_muni))),
    by = "municipio"
    
  ) %>%
  select(-municipio)
               
# Reconverte para sf
munis_es <- st_as_sf(munis_es, sf_column_name = "geom", crs = 4674) # 4674 = SIRGAS 2000 (usado pelo IBGE)
munis_es <- st_transform(munis_es, crs = 4326)



#------------------------------------------------------------------------------------------------------------------------
# SALVA OS DADOS PREPARADOS
#------------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar o arquivo")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Indicadores_munic_prep.rds")
  saveRDS(munis_es, file = caminho_arquivo)
}
