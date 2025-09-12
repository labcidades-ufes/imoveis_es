# Instalar pacotes necessários (se não tiver)
# install.packages(c("shiny", "DBI", "RSQLite", "DT"))

library(shiny)
library(DBI)
library(RSQLite)
library(DT)

# Função para conectar e buscar dados do SQLite
get_data <- function() {
  con <- dbConnect(RSQLite::SQLite(), "olx.db")
  df <- dbReadTable(con, "anuncios")
  dbDisconnect(con)
  return(df)
}

ui <- fluidPage(
  
  titlePanel("📊 OLX - Coleta de Dados"),
  
  sidebarLayout(
    sidebarPanel(
      textInput("titulo", "Buscar por título:", value = ""),
      numericInput("preco_min", "Preço mínimo:", value = 0, min = 0),
      numericInput("preco_max", "Preço máximo:", value = 10000, min = 0),
      actionButton("filtrar", "Aplicar Filtros"),
      br(),
      downloadButton("baixar", "📥 Baixar CSV")
    ),
    
    mainPanel(
      DTOutput("tabela")
    )
  )
)

server <- function(input, output, session) {
  
  # Dados reativos
  dados_filtrados <- reactiveVal(get_data())
  
  # Aplicar filtros ao clicar no botão
  observeEvent(input$filtrar, {
    df <- get_data()
    
    # Filtro por título
    if (input$titulo != "") {
      df <- df[grepl(input$titulo, df$titulo, ignore.case = TRUE), ]
    }
    
    # Filtro por preço
    df <- df[df$preco >= input$preco_min & df$preco <= input$preco_max, ]
    
    dados_filtrados(df)
  })
  
  # Renderizar tabela
  output$tabela <- renderDT({
    datatable(dados_filtrados(), options = list(pageLength = 10))
  })
  
  # Download CSV
  output$baixar <- downloadHandler(
    filename = function() {
      paste("anuncios_olx.csv")
    },
    content = function(file) {
      write.csv(dados_filtrados(), file, row.names = FALSE)
    }
  )
}

# Rodar o app
shinyApp(ui, server)

