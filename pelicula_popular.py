from pyspark import SparkConf, SparkContext

# Cargamos los nombres de las peliculas para reemplazar sus ID
def loadMoviesNames():
    movieNames= {}
#abrimos el archivo con los nombres de las pelis
    with open('ml-100k/u.item') as f:
        for  line in f:
# Separamos los datos por "|"
            fields=line.split('|')
# Luego convertimos la linea 0 a numero y en esa posicion del diccionario vamos a 
# guardar su nombre que esta el la linea 1 
# se veria asi fields: (1, 'Toy Story(1995),'01-Jan-1995','','http://...')
            movieNames[int(fields[0])]= fields[1]
    return movieNames


conf= SparkConf().setMaster('local').setAppName('Popular_Movies')
sc= SparkContext(conf=conf)

#Creamos un objeto 'nameDict', contiene el broadcast que estamos haciendo del mapeo
#de 'moviesID' con 'movieNames'
nameDict=sc.broadcast(loadMoviesNames())


line= sc.textFile('ml-100k/u.data') 
#     0          1           2           3
# ID_User    ID_Movie     rating     timestamp

# Con lambda separamos la columna 1(ID_Movie) 
# con el mismo lambda convertimos esa columna en una 'key' con 'value'= 1)
movies= line.map(lambda x: (int(x.split()[1]),1))

# Fusionamos las 'key' que se repiten y sumamos los 'values'
movie_counts= movies.reduceByKey(lambda x,y: x+y)

# Damos vuelta y covertimos las 'key' en 'value' (ID, conteo) -> (conteo,ID)
flipped= movie_counts.map(lambda x: (x[1],x[0]))

# Ordenamos el RDD de menor a mayor recurrencia(mayor conteo)
sorted_RDD = flipped.sortByKey() 

#Ahora le vamos a decir que en vez de mostrarnos el ID muestre el nombre perteneciente al ID
# El .map va a tomar cada par clave/valor, va a reemplazar eso con el nameDict creado por el broadcast,
# podemos hacer esto con el .map() por que usamos broadcast.
sorted_MovieNames= sorted_RDD.map(lambda count_Movie: (nameDict.value[count_Movie[1]],count_Movie[0]))


# Recuperamos los datos del RDD
results= sorted_MovieNames.collect()

# Imprimimos la lista
print("Count \t ID Movie")
for key,val in results:
    print(key,"-",val)