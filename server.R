# server.R

library(shiny)
library(geobr)
library(sf)
library(ggplot2)
library(dplyr) # Pacote para manipulação de dados (usado em group_by, summarise)
library(viridisLite) # Para escalas de cores bonitas

# --- Configuração Inicial (Executada uma vez quando o app inicia) ---
# 1. Instalar e carregar pacotes adicionais se ainda não tiver
# install.packages("remotes")
# remotes::install_github("ipeaGIT/geocodebr")
# install.packages("shinydashboard")

# Carregar pacotes (já carregados em ui.R e aqui para boa prática)
library(geobr)
library(sf)
library(ggplot2)
library(dplyr)
library(viridisLite)
library(shiny)
library(shinydashboard)

# Carregando os dados do OLX (AJUSTE O NOME DO ARQUIVO E DA COLUNA DE CEP)
# Certifique-se de que o arquivo 'dados_olx.csv' esteja na mesma pasta do seu app Shiny
# ou forneça o caminho completo.
dados_olx_original <- read.csv("dados_olx.csv")

# Se seus CEPs não estiverem em uma coluna chamada 'cep_coluna', ajuste o nome aqui:
nome_coluna_cep <- "cep_coluna" # <--- AJUSTE ESTE NOME

# 2. Geocodificação dos CEPs (Pode levar tempo!)
# Esta etapa é feita uma vez ao iniciar o app. Se tiver muitos dados,
# pode ser lento. Considere salvar os dados geocodificados em um arquivo RData
# ou CSV para carregar mais rápido em futuras execuções.
dados_geocodificados <- geocodebr::busca_por_cep(cep = dados_olx_original[[nome_coluna_cep]], verboso = FALSE)

# Unir os dados geocodificados com o dataframe original
dados_olx_com_coords <- left_join(dados_olx_original, dados_geocodificados, by = c(nome_coluna_cep = "cep"))

# Verificar se a geocodificação foi bem sucedida para as colunas lat/lon
# Se as colunas 'lat' e 'lon' não existirem ou estiverem vazias após o join,
# você precisará investigar por que os CEPs não foram encontrados.
# Para este exemplo, vamos assumir que elas foram criadas.

# Garantir que as colunas de coordenada existem e são numéricas
if (!"lat" %in% colnames(dados_olx_com_coords) || !"lon" %in% colnames(dados_olx_com_coords)) {
  stop("As colunas 'lat' ou 'lon' não foram criadas após a geocodificação. Verifique os CEPs e o pacote geocodebr.")
}
dados_olx_com_coords$lat <- as.numeric(dados_olx_com_coords$lat)
dados_olx_com_coords$lon <- as.numeric(dados_olx_com_coords$lon)

# Remover linhas onde a geocodificação falhou (lat/lon são NA)
dados_olx_com_coords <- na.omit(dados_olx_com_coords, cols = c("lat", "lon"))


# 3. Ler o shapefile do estado do Espírito Santo
limites_es <- geobr::read_state(code_state = 32)

# 4. Criar a malha hexagonal
# Ajuste cellsize para a granularidade que você deseja. Valores menores criam hexágonos menores.
malha_es <- st_make_grid(
  x = limites_es,
  cellsize = c(0.1, 0.1), # Pode precisar de ajuste. Experimente valores como 0.05, 0.1, 0.2
  what = "polygons",
  square = FALSE
) %>%
  st_sf() %>%
  st_set_crs(st_crs(limites_es)) # Define o CRS da malha igual ao do estado

# 5. Converter os dados de imóveis para um objeto sf (Spatial Features)
dados_sf <- st_as_sf(dados_olx_com_coords, coords = c("lon", "lat"), crs = 4326) # CRS padrão para coordenadas geográficas

# 6. Contar os pontos (imóveis) em cada hexágono
# Usamos st_join para "atribuir" cada ponto a um hexágono e depois agrupamos para contar.
contagem_hexagonos <- st_join(malha_es, dados_sf) %>%
  dplyr::group_by(ID_MALHA = seq_len(nrow(.))) %>% # Cria um ID único para cada hexágono
  dplyr::summarise(n_imoveis = n()) %>%
  dplyr::filter(n_imoveis > 0) # Filtra hexágonos sem imóveis, se desejar


# --- Lógica do Servidor (Executada sempre que o usuário interage) ---
server <- function(input, output) {

  # Renderiza o mapa hexagonal
  output$mapaHexagonal <- renderPlot({
    ggplot() +
      geom_sf(data = contagem_hexagonos, aes(fill = n_imoveis), color = NA, alpha = 0.8) + # Sem borda para melhor visualização, alpha para transparência
      geom_sf(data = limites_es, fill = NA, color = "darkgray", linewidth = 0.7) + # Contorno do ES
      scale_fill_viridis_c(option = "magma", name = "Nº de Imóveis") + # Escala de cores
      labs(
        title = "Densidade de Imóveis no Espírito Santo (OLX)",
        subtitle = "Agregação em Malha Hexagonal",
        caption = "Dados: OLX (coletados via script R)"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(hjust = 0.5, face = "bold"), # Centraliza e negrita o título
        plot.subtitle = element_text(hjust = 0.5), # Centraliza o subtítulo
        legend.title = element_text(face = "bold")
      )
  })

  # Você pode adicionar mais renderizações aqui para outros elementos da UI no futuro
  # Ex: output$outra_coisa <- renderText({...})
}
