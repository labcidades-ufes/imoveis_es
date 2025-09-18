# imoveis_es
Solução para levantamento do valor de imóveis no Espírito Santo
## Visão geral
Esta solução realiza a coleta bruta de dados de imóveis anunciados para venda na OLX, seguida do processamento e preparo desses dados para que estejam em um formato compatível com visualização via Leaflet. Essa visualização é implementada por meio de uma aplicação Shiny.

## Fluxo de atualização
Os scripts contidos em `scripts/` devem ser executados na seguinte ordem:
1. **Coleta de dados de anúncios e geolocalização.**
    - `coleta.R` - realiza a coleta de dados brutos do OLX e os salva em `Anuncios_brutos.RDS`.
    - `coleta_geoloc.R` - converte os dados de CEP em `Anuncios_brutos.RDS` em pontos de latitude e longitude. O novo data frame é salvo como `Anuncios_geoloc.RDS`
2. **Processamento e preparo de dados.**
Ao final, são gerados os arquivos necessários para criar a visualização.

    2.1 **Indicadores para geometria de Municípios.**
   - `processa_munic.R` - lê o arquivo `anuncios_brutos.RDS` e calcula indicadores por município. Gera o arquivo `indicadores_munic.RDS`.
   - `prepara_munic.R` - prepara os dados de `indicadores_munic.RDS` e agrega a geometria compatível com o Leaflet. Gera o arquivo `indicadores_munic_prep.RDS`.
   
    2.2 **Indicadores para geometria de Hexágonos.**
     - `pre_process_hex.R` - lê o arquivo `anuncios_geoloc.RDS`, aplica um buffer aos pontos e gera uma malha hexagonal do ES. Mescla as bases, mantendo apenas os hexágonos que têm anúncios correspondentes. Exporta a base coomo `pre_process_hex.RDS`.
     - `processa_hex.R` - lê o arquivo `pre_process_hex.RDS`, remove a coluna de geometria (para otimizar o processamento), calcula os indicadores e salva em `indicadores_hex.RDS`. 
     - `prepara_hex.R` - lê o arquivo `indicadores_hex.RDS`, agrega a coluna com a geometria correspondente a cada hexágono e transforma o data frame para um formato compatível com o Leaflet. Gera o arquivo `indicadores_hex_prep.RDS`.

## Aplicação Shiny
- `global.R` - importa os arquivos `indicadores_munic_prep.RDS` e `indicadores_hex_prep.RDS`, e gera os arquivos iniciais necessários para os filtros.
- `ui.R` - cria a interface de visualização, incluindo a caixa de seleção de indicadores.
- `server.R` - implementa a lógica da visualização, utilizando controle de *Layer* em conjunto com *Proxy*, o que permite alternar entre os mapas de hexágonos e municípios.
