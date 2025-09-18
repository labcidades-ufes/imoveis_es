#-----------------------------------------------------------------------------------------------------------------------
# CARREGA PACOTES NECESSÁRIOS
#-----------------------------------------------------------------------------------------------------------------------
library(shiny)
library(leaflet)
library(geobr)
library(dplyr)
library(htmltools)
library(tcltk)
library(sf)
#---------------------------------------------------------------------------------------------
# OBTÉM DADOS PREPARADOS 
#---------------------------------------------------------------------------------------------
# Ler RDS
munis_es <- readRDS(file.choose()) # Seleciona os indicadores por municipio
hexag_es <- readRDS(file.choose()) # Seleciona os indicadores por haxágono


#---------------------------------------------------------------------------------------------
# GERAL 
#---------------------------------------------------------------------------------------------
# Cria nomes mais amigáveis para os usuários selecionarem
Opcoes <- c("Preço Máximo", "Preço Mínimo","Preço Médio", "Média R$/m²",
            "Preço médio 1 quarto","Preço médio 2 quartos","Preço médio 3 quartos",
            "Preço médio 4 quartos","Preço médio 5 quartos") # Nomes para os usuários
colunas <- names(munis_es)[1:9]  # nomes reais das colunas
opcoes_nomeadas <- setNames(colunas, Opcoes) # Atrela uma coluna a um nome


