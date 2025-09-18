# imoveis_es
Solução para levantamento do valor de imóveis no Espírito Santo
## Visão geral
A solução realiza a coleta bruta de dados de imóveis anunciados para venda na OLX, seguida do processamento e preparo desses dados para que estejam em um formato compatível com visualização via Leaflet. Essa visualização é implementada por meio de uma aplicação Shiny.

## Fluxo de atualização
Os scrips contidos em Scripts/ devem ser executados na seguinte ordem:
1. Coleta de dados de anúncios e geolocalização.
    - `Coleta.R` - faz a coleta de dados brutos do OLX e atribui a `Anuncios_brutos.RDS`.
    - `Coleta_Geoloc` - converte os dados de CEP em `Anuncios_brutos.RDS` em pontos de Latitude e Longitude.
## Banco de Dados
