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
# IMPORTA O ARQUIVO Indicadores_hex.RDS
#------------------------------------------------------------------------------------------------------------------------

# Recebe os indicadores
indicadores_hex <- readRDS(file.choose()) 


#-------------------------------------------------------------------------------------------------------------------------------------------
# GRADE HEXAGONAL DO ES - IDENTIFICADA POR HEX_ID
#-------------------------------------------------------------------------------------------------------------------------------------------
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

grade_es <- criar_hexgrid_es()


#------------------------------------------------------------------------------------------------------------------------
# PREPARA DADOS
#------------------------------------------------------------------------------------------------------------------------

hex_es <- indicadores_hex %>%
  left_join(
    grade_es,
    by = "hex_id"
  )

# Converte para o formato correto que o leaflet interpreta: EPSG:4326 = WGS84
hex_es <- st_as_sf(hex_es, sf_column_name = "x", crs = 4674) %>% # 4674 = SIRGAS 2000 (usado pelo IBGE)
  st_transform(crs = 4326) 

#Reorganiza as colunas
hex_es <- hex_es %>% relocate(hex_id, .after = '5_quartos')


#------------------------------------------------------------------------------------------------------------------------
# SALVA OS DADOS PREPARADOS
#------------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar o arquivo")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Indicadores_hex_prep.rds")
  saveRDS(hex_es, file = caminho_arquivo)
}

