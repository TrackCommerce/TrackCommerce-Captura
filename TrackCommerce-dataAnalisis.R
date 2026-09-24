# Arquivo para análise dos dados dos servidores facilitado
### O Arquivo a seguir deve identificar valores e métricas importantes dos dados de captura encontrados no arquivo .csv gerado pelo script-escrita.py
### Importe os arquivos .csv e identifique-os abaixo:

installNecessaries <- function(){
  if('dplyr' %in% installed.packages()[, "Package"]){
    print("Encontrado Pacote >> dplyr")
  }
  else{
    print("Instalando Pacote >> dplyr")
    install.packages("dplyr")
  }
  if('ggplot2' %in% installed.packages()[, "Package"]){
    print("Encontrado Pacote >> ggpllot2")
  }
  else{
    print("Instalando Pacote >> ggplot2")
    install.packages("ggplot2")
  }
  if('knitr' %in% installed.packages()[, "Package"]){
    print("Encontrado Pacote >> knitr")
  }
  else{
    print("Instalando Pacote >> knitr")
    install.packages("knitr")
  }
  library(dplyr)
  library(ggplot2)
  library(knitr)
}
installNecessaries()


df_RawLeague <- data.frame(
  #### Identifique o seu df aqui dentro
)

### Exemplo de como deve ficar:
### df_RawLeague <- data.frame(
###  ServidorFinancas
###  ServidorAplicacao
### )

# Análises que devem ser feitas:
### Inicialmente, é de extrema importância que todos os parâmetros analizados sejam comparados uns com os outros.
### As análises recebidas contém informações de interesse e devem abranjer N dados.
### Com o arquivo disponibilizado no bucket Silver, é possível Analizar as seguintes condições:
### 1 - Comparação de Empresa e usos de componentes (Histórico de uso de cada componente separados por EMPRESA)
### 2 - Comparação de Região e uso de componentes (Histórico de uso de cada componente separados por REGIÃO) - Deve verificar implementação
### 3 - Verificação de quantidade de DownTimes antes e depois da nossa aplicação a fim de otimização de código.
### 4 - Verificar DownTimes por tempo (separando por meses) - Caso possível avançar implementação verificando Temperatura de Região.
### 5 - Temperatura de componentes e seus usos (Ex: Tmp ºC CPU e Uso %)
### 6 - Partes específicas de componentes e seus desgates (Ex: Threads CPU e Uso %).
### 7 - Verificar partes de um componente e outro (Ex: Uso RAM e Uso CPU).
### 8 - Verificar correções entre dados: Pode ser que a relação entre clima e uso dos componentes esteja diretamente relacionado aos horários de pico e não necessariamente a uma reação meteorológica.
### 9 - Verificar comparação empresa e problema com dados reais (Quantidade de servidores de cada empresa pode afetar diretamente essa métrica)
### 10 - Verificar se as comparações entre os dados apresentados são realmente diretamente proporcionais ou apenas uma coincidencia.
### 11 - Fazer análise quantitativa de gasto monetário. --- Deve verificar implementação
# Fazer comparações absurdas que podem não ter um sentido inicial são sempre importantes.