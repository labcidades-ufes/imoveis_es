#-----------------------------------------------------------------------------------------------------------------------
# SERVER
#-----------------------------------------------------------------------------------------------------------------------

server <- function(input, output, session) {
  
  # Mapa inicial (apenas estrutura, sem depender de input$Indicador_desejado)
  output$mapa_es <- renderLeaflet({
    leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron,
                       options = providerTileOptions(minZoom = 7, maxZoom = 12)) %>%
      setView(lng = -40.3381, lat = -19.8, zoom = 7.4) %>%
      addLayersControl(
        baseGroups = c("Por Município", "Por Hexágono"),
        options = layersControlOptions(collapsed = FALSE)
      )
  })
  
  # Observa mudanças no selectInput e atualiza as camadas
  observeEvent(input$Indicador_desejado, {
    campo <- input$Indicador_desejado
    rotulo <- names(opcoes_nomeadas)[opcoes_nomeadas == campo]
    
    pal_munis <- colorNumeric("viridis", domain = munis_es[[campo]], na.color = "transparent") # Pode trocar por "Blues", "viridis", "YlorRd",  etc.
    pal_hex   <- colorNumeric("viridis", domain = hexag_es[[campo]], na.color = "transparent")
    
    leafletProxy("mapa_es") %>%
      clearShapes() %>%
      addPolygons(
        data = munis_es,
        fillColor = ~pal_munis(munis_es[[campo]]),
        color = "white", weight = 1, fillOpacity = 0.8,
        popup = ~sprintf("<strong>%s</strong><br/>%s: %s",
                         name_muni, rotulo,
                         format(munis_es[[campo]], big.mark = ".", decimal.mark = ",")),
        group = "Por Município",
        layerId = ~name_muni
      ) %>%
      addPolygons(
        data = hexag_es,
        fillColor = ~pal_hex(hexag_es[[campo]]),
        color = "white", weight = 1, fillOpacity = 0.8,
        popup = ~sprintf("<strong>Região nº: %s</strong><br/>%s: %s",
                         hex_id, rotulo,
                         format(hexag_es[[campo]], big.mark = ".", decimal.mark = ",")),
        group = "Por Hexágono",
        layerId = ~hex_id
      ) 
    
  })
  
  # Atualiza legenda conforme o grupo ativo
  observe({
    grupo_ativo <- input$mapa_es_groups  # vetor de grupos ativos (pode ter mais de um)
    
    campo <- input$Indicador_desejado
    rotulo <- names(opcoes_nomeadas)[opcoes_nomeadas == campo]
    
    pal_munis <- colorNumeric("viridis", domain = munis_es[[campo]], na.color = "transparent")
    pal_hex   <- colorNumeric("viridis", domain = hexag_es[[campo]], na.color = "transparent")
    
    proxy <- leafletProxy("mapa_es") %>% clearControls()  # limpa legendas antigas
    
    if ("Por Município" %in% grupo_ativo) {
      proxy %>% addLegend(
        position = "bottomright",
        pal = pal_munis,
        values = munis_es[[campo]],
        title = paste(rotulo, "- Municípios"),
        labFormat = function(type, cuts, p) { 
          prettyNum(
            format(cuts, scientific = FALSE), 
            big.mark = ".", decimal.mark = ","
          )
        }
        
      )
    }
    
    if ("Por Hexágono" %in% grupo_ativo) {
      proxy %>% addLegend(
        position = "bottomright",
        pal = pal_hex,
        values = hexag_es[[campo]],
        title = paste(rotulo, "- Hexágonos"),
        labFormat = function(type, cuts, p) { 
          prettyNum(
            format(cuts, scientific = FALSE), 
            big.mark = ".", decimal.mark = ","
          )
        }
        
      )
    }
  })
  
  # Observa cliques em qualquer layer
  observeEvent(input$mapa_es_shape_click, {
    click <- input$mapa_es_shape_click
    
    if (!is.null(click)) {
      proxy <- leafletProxy("mapa_es") %>%
        clearGroup("highlight")
      
      if (click$group == "Por Município") {
        proxy %>% addPolygons(
          data = subset(munis_es, name_muni == click$id),
          color = "#E31B23",
          weight = 4,
          fillOpacity = 0.2,
          group = "highlight"
        )
      }
      else if (click$group == "Por Hexágono") {
        proxy %>% addPolygons(
          data = subset(hexag_es, hex_id == click$id),
          color = "#E31B23",
          weight = 4,
          fillOpacity = 0.2,
          group = "highlight"
        )
      }
    }
  })
  
  
}
