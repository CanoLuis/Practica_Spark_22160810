options(max.print=100000)
pacman::p_load(httr, tidyverse,leaflet,janitor,readr,sparklyr)
url<-	"https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
httr::GET(url)
library(sparklyr)
library(dplyr)
library(tidyverse)
library(stringr)
library(readxl)
sc<-spark_connect(master="local")





#a
#Limpie el/los dataset(s)
jsonlite::fromJSON(url)
dataset<- jsonlite::fromJSON(url) 
dataset<-dataset$ListaEESSPrecio
dataset<- dataset %>%  as_tibble() %>% clean_names() 
dataset<-dataset%>% type_convert(locale = locale(decimal_mark = ","))  %>% view() %>% clean_names()


dataset <- select(dataset, -c(margen, remision))

names(dataset)[names(dataset) == "c_p"] <- "Codigo_Postal"
dataset$tipo_venta = ifelse(dataset$tipo_venta == "P", "Venta al público en general", 
                            ifelse(dataset$tipo_venta == "R", "Venta restringida a socios o cooperativistas", "No definido el tipo de venta"))

View(dataset)


#cree una columna nueva que deberá llamarse low-cost, y determine cuál es el precio promedio de todos los combustibles 
#a nivel comunidades autónomas, así como para las provincias, tanto para el territorio peninsular e insular,
#esta columna deberá clasificar las estaciones por lowcost y no lowcost

dataset<- dataset %>%mutate(low_cost=rotulo%in%c('REPSOL','CAMPSA','BP','SHELL','GALP','CEPSA')) %>% view()
view(dataset)

dataset$low_cost = ifelse(dataset$low_cost == TRUE, "Gasolinera low-cost", "Gasolinera no low-cost")

precio_medio_ccaa <- dataset %>%
  select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, 
         precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, 
         precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, 
         precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(idccaa) %>% 
  summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), 
            mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), 
            mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), 
            mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), 
            mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), 
            mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), 
            mean(precio_hidrogeno, na.rm=TRUE)) %>% view()

View(precio_medio_ccaa)

precio_medio_provincia <- dataset %>%
  select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, 
         precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, 
         precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, 
         precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, id_provincia) %>% 
  group_by(id_provincia) %>% 
  summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), 
            mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), 
            mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), 
            mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), 
            mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), 
            mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), 
            mean(precio_hidrogeno, na.rm=TRUE)) %>% view()


write.csv(precio_medio_provincia,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\precio_medio_provincia.csv", row.names = FALSE)


#Imprima en un mapa interactivo, la localización del top 10 mas caras y otro mapa interactivo del top 20 mas baratas


#Gasoleo A. Top 10 más caras
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_a, localidad, direccion) %>% 
  top_n(10, precio_gasoleo_a) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_a)

#Gasoleo A. Top 20 más baratas
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_a, localidad, direccion) %>%
  top_n(-20, precio_gasoleo_a) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_a)


#Gasoleo Premium. Top 10 más caras
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_premium, localidad, direccion) %>% 
  top_n(10, precio_gasoleo_premium) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_premium)


#Gasoleo Premium. Top 20 más baratas
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_premium, localidad, direccion) %>%
  top_n(-20, precio_gasoleo_premium) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_premium)


#Gasolina 95. Top 10 más caras
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5, localidad, direccion) %>% 
  top_n(10, precio_gasolina_95_e5) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5)

View(top10)


#Gasolina 95. Top 20 más baratas
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5, localidad, direccion) %>%
  top_n(-20, precio_gasolina_95_e5) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5)



#Gasolina 98. Top 10 más caras
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion) %>% 
  top_n(10, precio_gasolina_98_e5) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_98_e5)


#Gasolina 98. Top 20 más baratas
dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion) %>%
  top_n(-20, precio_gasolina_98_e5) %>%
  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_98_e5)


#Guardar este "archivo" en una nueva tabla llamada low-cost_num_expediente

write.csv(dataset,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\low_cost_22160810.csv", row.names = FALSE)



#b

#cuántas gasolineras tiene la comunidad de Madrid y en la comunidad de Cataluña, cuántas son low-cost, cuantas no lo son

gasolineras_comunidad_madrid <- dataset %>% select(idccaa, low_cost, provincia) %>% filter(idccaa=="13") %>% count(low_cost)
View(gasolineras_comunidad_madrid)


gasolineras_cataluna <- dataset %>% select(idccaa, low_cost, provincia) %>% filter(idccaa=="09") %>% count(low_cost)
View(gasolineras_cataluna)


#precio promedio, el precio más bajo y el más caro de gasóleo A, y gasolina 95 e Premium.


datos_combustibles_madrid <- dataset %>% select(idccaa, low_cost, provincia, municipio, ideess, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>%
  filter(idccaa=="13") %>%
  drop_na %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))
  
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "max(precio_gasoleo_a)"] <- "Gasóleo A. Precio Máximo"
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "min(precio_gasoleo_a)"] <- "Gasóleo A. Precio Mínimo"
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "mean(precio_gasoleo_a)"] <- "Gasóleo A. Precio Medio"
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "max(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Máximo"
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "min(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Mínimo"
names(datos_combustibles_madrid)[names(datos_combustibles_madrid) == "mean(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Medio"


View(datos_combustibles_madrid)




datos_combustibles_cataluna <- dataset %>% select(idccaa, low_cost, provincia, municipio, ideess, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>%
  filter(idccaa=="09") %>%
  drop_na %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium))

names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "max(precio_gasoleo_a)"] <- "Gasóleo A. Precio Máximo"
names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "min(precio_gasoleo_a)"] <- "Gasóleo A. Precio Mínimo"
names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "mean(precio_gasoleo_a)"] <- "Gasóleo A. Precio Medio"
names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "max(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Máximo"
names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "min(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Mínimo"
names(datos_combustibles_cataluna)[names(datos_combustibles_cataluna) == "mean(precio_gasolina_95_e5_premium)"] <- "Gasolina 95. Precio Medio"


View(datos_combustibles_cataluna)




#Guardar este "archivo" en una nueva tabla llamada Informe_MAD_BCN_expediente

write.csv(datos_combustibles_madrid,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\Informe_MAD_22160810.csv", row.names = FALSE)

write.csv(datos_combustibles_cataluna,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\Informe_BCN_22160810.csv", row.names = FALSE)







#c 

Informe_no_grandes_ciudades_22160810 <- dataset %>% select(idccaa, id_municipio, municipio, ideess, low_cost, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% 
  group_by(municipio, low_cost) %>%
  filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>%
  summarise(mean(precio_gasoleo_a, na.rm = TRUE), mean(precio_gasolina_95_e5_premium, na.rm = TRUE), max(precio_gasoleo_a, na.rm = TRUE), max(precio_gasolina_95_e5_premium, na.rm = TRUE), min(precio_gasoleo_a, na.rm = TRUE), min(precio_gasolina_95_e5_premium, na.rm = TRUE))

View(Informe_no_grandes_ciudades_22160810)
#Cuántas gasolineras son low-cost

num_eess_low_cost<-Informe_no_grandes_ciudades_22160810 %>% filter(low_cost=="Gasolinera low-cost")
nrow(num_eess_low_cost)

#Cuántas gasolineras no son low-cost

num_eess_no_low_cost<-Informe_no_grandes_ciudades_22160810 %>% filter(low_cost=="Gasolinera no low-cost")
nrow(num_eess_no_low_cost)


write.csv(Informe_no_grandes_ciudades_22160810,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\Informe_no_grandes_ciudades_22160810.csv", row.names = FALSE)



#d

no_24_horas_22160810 <- dataset %>% select(Codigo_Postal, direccion, localidad, municipio, id_provincia, ideess) %>%
  filter(dataset$horario == "L-D: 24H")

View(no_24_horas_22160810)


write.csv(no_24_horas_22160810,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\no_24_horas_22160810.csv", row.names = FALSE)



#e
#i
poblacion <- read_excel("D:/Máster/Posicionamiento empresarial del Big Data/Práctica Spark/poblacion.xlsx") #Fuente: https://www.ine.es/dynt3/inebase/es/index.htm?padre=517&capsel=525

names(poblacion)[names(poblacion) == "Cifras de población resultantes de la Revisión del Padrón municipal a 1 de enero de 2020"] <- "Código de provincia"
names(poblacion)[names(poblacion) == "...2"] <- "Provincia"
names(poblacion)[names(poblacion) == "...3"] <- "Código de municipio"
names(poblacion)[names(poblacion) == "Municipio"] <- "municipio"
names(poblacion)[names(poblacion) == "...5"] <- "Población"


poblacion <- select(poblacion, -c(...6, ...7))

View(dataset)

dataset_poblacion <- merge(x = dataset, y = poblacion, by = "municipio", all = TRUE)
View(dataset_poblacion)


#ii

dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 1)

dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 2)

dataset %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_98_e5, localidad, direccion, ideess) %>% 
  leaflet() %>% addTiles() %>%
  addCircles(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~ideess, radius = 4)



#iii genere el TopTen

#El parámetro elegido para la ordenación de los datos y su consecuente elección ha sido el precio más barato de la gasolina 95

dataset_top_no24 <- dataset_poblacion %>% 
  select(Provincia, municipio, horario, precio_gasolina_95_e5,ideess, direccion, low_cost) %>%
  filter(dataset_poblacion$horario!="L-D: 24H") %>%
  group_by(low_cost) %>%
  top_n(-10, precio_gasolina_95_e5)

View(dataset_top_no24)

write.csv(dataset_top_no24,"D:\\Máster\\Posicionamiento empresarial del Big Data\\Práctica Spark\\informe_top_ten_22160810.csv", row.names = FALSE)




