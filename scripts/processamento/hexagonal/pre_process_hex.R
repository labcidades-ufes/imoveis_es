#-------------------------------------------------------------------------------------------------------------------------------------------
# ITENS INICIAIS NECESSÁRIOS
#-------------------------------------------------------------------------------------------------------------------------------------------
# Pacotes necessários

library(sf)
library(geobr)
library(dplyr)



#-------------------------------------------------------------------------------------------------------------------------------------------
# LÊ RDS COM ANÚNCIOS COLETADOS Anuncios_geoloc.RDS (APENAS LAT E LONG)
#-------------------------------------------------------------------------------------------------------------------------------------------

# Recebe os anúncios com geolocalização
anuncios_coletados <- readRDS(file.choose()) 


#-------------------------------------------------------------------------------------------------------------------------------------------
# FUNÇÕES UTILIZADAS
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
  
  return(joined)
}


#----------------------------------------------------------------------------------------------------------------------
# APLICA BUFFER NOS PONTOS
#----------------------------------------------------------------------------------------------------------------------
# Criar coluna com distância condicional
anuncios_coletados <- anuncios_coletados %>%
  mutate(
    dist_buffer = ifelse(grepl("000$", cep), 1000, 100) # Muda de acordo com o fim do cep.
  )

anuncios_sf_utm  <- anuncios_coletados %>%
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

#Remove coluna de geometria do anúncio, mantendo apenas a hexagonal
hex_grid_anuncios <- hex_grid_anuncios %>%
  select(-geometry_anuncio)

#----------------------------------------------------------------------------------------------------------------------
# SALVA O DATA FRAME COLETADO
#----------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar os hexágonos de anúncios do ES")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Pre_process_hex.rds")
  saveRDS(hex_grid_anuncios, file = caminho_arquivo)
}


