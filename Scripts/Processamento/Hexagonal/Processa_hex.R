#------------------------------------------------------------------------------------------------------------------------
# ITENS INICIAIS NECESSÁRIOS
#------------------------------------------------------------------------------------------------------------------------
# Inicializa as bibliotecas necessárias
library(dplyr)
library(tidyverse)
library(skimr)
library(janitor)
library(tidyr)
library(sf)

#-------------------------------------------------------------------------------------------------------------------------------------------
# LÊ RDS COM ANÚNCIOS PRÉ-PROCESSADOS - Pre_process_hex.RDS
#-------------------------------------------------------------------------------------------------------------------------------------------

# Recebe os anúncios com geolocalização
anuncios_hex_coletados <- readRDS(file.choose()) 


anuncios_hex_coletados <- as.data.frame(anuncios_hex_coletados) %>%
  select(-geometry)


#------------------------------------------------------------------------------------------------------------------------
# CALCULO DE INDICADORES
#------------------------------------------------------------------------------------------------------------------------


anuncios_hex_coletados <- anuncios_hex_coletados %>%
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
anuncios_hex_coletados <- anuncios_hex_coletados %>%
  distinct(across(-url), .keep_all = TRUE)  %>% # Remove duplicatas
  filter(
    !is.na(`preco_R$`), # Preço = NA    
    `preco_R$` != 0,    # Preço = 0      
    `preco_R$` < 100000000     # Preço acima de 100 Milhões é removido   
    
  ) 


Dados_hex_geral <- anuncios_hex_coletados %>%
  group_by(hex_id)

# Seleciona apenas as colunas necessárias
Dados_hex_geral <- Dados_hex_geral %>%
  select(`preco_R$`,area_m2,hex_id) %>%
  summarise(
    preco_max = max(`preco_R$`, na.rm = TRUE),
    preco_min = min(`preco_R$`, na.rm = TRUE),
    preco_med = mean(`preco_R$`, na.rm = TRUE)
    
  ) %>%
  arrange(hex_id)

#Calcula preco_area apenas com linhas completas - área e preço
preco_area_limpo <- anuncios_hex_coletados %>%
  filter(!is.na(`preco_R$`) , !is.na(area_m2), area_m2 > 10) %>%
  group_by(hex_id) %>%
  summarise(preco_medio_area = round((sum(`preco_R$`) / sum(area_m2)),1)) %>%
  arrange(hex_id)


# Junta os dois resultados - INDICADORES SEM QUARTOS
Indicadores_hex_geral <- left_join(Dados_hex_geral,preco_area_limpo, by = "hex_id")



# CALCULO DE INDICADORES PARA QUARTOS

Dados_hex_quartos <- anuncios_hex_coletados %>%
  filter(!is.na(n_quartos)) %>%
  mutate(
    n_quartos = paste(n_quartos,ifelse(n_quartos == 1, "quarto", "quartos"), sep = "_")
  ) %>%
  group_by(n_quartos, hex_id) %>%
  summarize(media_quarto = mean(`preco_R$`, na.rm = TRUE)) %>%
  arrange(n_quartos)



# Faz a conversão de linhas para colunas
preco_quarto_corrigido <- Dados_hex_quartos %>%
  pivot_wider(
    names_from = n_quartos,
    values_from = media_quarto
  )



Indicadores_hex<- left_join(Indicadores_hex_geral,preco_quarto_corrigido, by = "hex_id")


#------------------------------------------------------------------------------------------------------------------------
# SALVA OS INDICADORES
#------------------------------------------------------------------------------------------------------------------------

pasta <- tk_choose.dir(caption = "Escolha a pasta para salvar o arquivo Indicadores_hex.RDS")

if (!is.na(pasta)) {
  caminho_arquivo <- file.path(pasta, "Indicadores_hex.rds")
  saveRDS(Indicadores_hex, file = caminho_arquivo)
}