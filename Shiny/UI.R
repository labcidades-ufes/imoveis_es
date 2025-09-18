#-----------------------------------------------------------------------------------------------------------------------
# UI
#-----------------------------------------------------------------------------------------------------------------------
ui <- fluidPage(
  
  titlePanel("Imóveis de Municípios do Espírito Santo"),
  
  sidebarLayout(
    sidebarPanel(
      width = 3,
      radioButtons(
        inputId = "Indicador_desejado",
        label = "Indicador desejado:",
        choices = opcoes_nomeadas,
        selected = opcoes_nomeadas[1],
        inline = FALSE
      )
    ),
    mainPanel(
      width = 9,
      leafletOutput("mapa_es", width = "100%", height = "calc(100vh - 120px)")
    )
  )
)

