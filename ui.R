# ui.R

library(shiny)
library(shinydashboard) # Um pacote para deixar a interface mais bonita e organizada

# Define a interface do usuário
ui <- dashboardPage(
  dashboardHeader(title = "Imóveis ES - OLX"), # Título da barra superior
  dashboardSidebar(
    sidebarMenu(
      menuItem("Mapa de Imóveis", tabName = "mapa", icon = icon("map-marked-alt"))
      # Você pode adicionar outros menus aqui no futuro, como "Sobre", "Dados Brutos"
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "mapa",
              fluidRow(
                box(
                  title = "Visualização de Imóveis por Hexágono",
                  status = "primary",
                  solidHeader = TRUE,
                  width = 12, # Ocupa toda a largura
                  plotOutput("mapaHexagonal", height = "600px") # Área para exibir o mapa
                )
              )
      )
    )
  )
)
