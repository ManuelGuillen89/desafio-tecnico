from pyspark.sql import SparkSession
import functools as ft

import os

CSV_FILE_NAMES = ['homologacion_pais', 'homologacion_rating','rating_empresa','rating_soberano']
INPUT_DIRECTORY = "data"

def load_csv_into_spark_tempview(csv_file_name, directory, spark_session):
    dataframe = spark_session.read \
        .option("delimiter", "|") \
        .option("header", "true") \
        .csv(f"{directory}/{csv_file_name}.csv")
    dataframe.createOrReplaceTempView(csv_file_name)   
    pass

print (" >> INICIANDO GENERACION DE 'Maestro de Ratings' << ")

print (" --> Cargando Spark ... ")
spark_session = SparkSession \
    .builder \
    .appName("Desafío Técnico") \
    .getOrCreate()

if not (os.path.isdir(INPUT_DIRECTORY)):
    print (f"Directorio de entrada no encontrado: INPUT_DIRECTORY = ./{INPUT_DIRECTORY}")
    print ("Proceso abortado. Saliendo... ")
    exit()

print (" --> Cargando datasets desde archivos CSV a TempViews en Spark ... ")
for dataset in CSV_FILE_NAMES:
    print (f'{dataset} ...')
    if not (os.path.isfile(f"{INPUT_DIRECTORY}/{dataset}.csv")):
        print (f"Arhivo de entrada no econtrado: {INPUT_DIRECTORY}/{dataset}.csv")
        print ("Proceso abortado. Saliendo... ")
        exit()
    load_csv_into_spark_tempview(dataset, INPUT_DIRECTORY, spark_session)
    print (f"Dataset '{dataset}' cargado!")

print ("--> Ejecutando querys ... ")

#Filtra homologacion_rating dejando solo las agencias requeridas
homol_rating_agencias = spark_session.sql(''' 
SELECT *
FROM homologacion_rating
WHERE agencia_homol IN ('SP','MDY','FITCH')
ORDER BY orden_norma DESC
''')

#crea dataframe con la primera muestra del master
master_1_df = spark_session.sql('''
SELECT 
    rat_emp.rut AS rut, 
    rat_emp.dv AS dv, 
    rat_emp.nombre AS nombre,
    rat_emp.sp AS sp,
    rat_emp.mdy AS mdy,
    rat_emp.fitch AS fitch, 
    homol_pais.pais AS pais
FROM rating_empresa rat_emp
LEFT JOIN homologacion_pais homol_pais ON ucase(rat_emp.pais_bbg) = ucase(homol_pais.pais_bbg)
''')
#crea vista de la primera muestra del master
master_1_df.createOrReplaceTempView("master_1_df")


#Funcion reductora que genera el cruce use entre homologacion_rating y rating_empresa
def reduce_getting_rating_norma_x_rut(acc, row):
    sp = row['sp']
    mdy = row['mdy']
    fitch = row['fitch']
    rut = row['rut']
    rating_agencia = homol_rating_agencias \
        .filter(f" rating IN ('{sp}', '{mdy}','{fitch}') ") \
        .select("rating_norma") \
        .first() 
    if (rating_agencia == None):
        acc.append((rut, "-"))    
    else:
        acc.append((rut, rating_agencia.rating_norma))
    return acc

print ("--> generando cruce rating_norma_x_rut")
#Aplica reductor a master_1_df y luego crea un dataframe con la data obtenida
master_df_collected = master_1_df.collect()
rating_norma_x_rut = ft.reduce(reduce_getting_rating_norma_x_rut, master_df_collected, [])
rating_norma_x_rut_df = spark_session.createDataFrame(data = rating_norma_x_rut, schema='rut string, rating_norma string')
# Crea una vista de rating_norma_x_rut_df (para finalmente agregarla al master via SQL) 
rating_norma_x_rut_df.createOrReplaceTempView("rating_norma_x_rut")
rating_norma_x_rut_df.show()


print ("--> agregando rating a master ... ")
master_2_df = spark_session.sql('''
SELECT 
    m.rut, m.dv, m.nombre, m.pais, r.rating_norma AS rating
FROM master_1_df m
LEFT JOIN rating_norma_x_rut r ON m.rut = r.rut
''')
master_2_df.createOrReplaceTempView("master_2_df")
master_2_df.show()




# rating = rating_norma_x_rut_df \
#     .filter("rut == '44000061'") \
#     .select("rating_norma") \
#     .first() \
#     .rating_norma

# print (rating)

""" 

def reduce_appending_rating_norma(acc, row):
    rut = row['rut']
    dv = row['dv']
    nombre = row['nombre']
    pais = row['pais']
    rating = rating_norma_x_rut_df \
        .filter(f"rut == '{rut}'") \
        .select("rating_norma") \
        .first() \
        .rating_norma
    acc.append((rut, dv, nombre, pais, rating))
    return acc 

def dummy(acc, row):
    acc.append(("test"))
    return acc """

# master_2 = ft.reduce(reduce_appending_rating_norma, master_df.collect(), [])



# for row in master_df_collected:
#     rut = row['rut']
#     rating = rating_norma_x_rut_df \
#         .filter(f"rut == '{rut}'") \
#         .select("rating_norma") \
#         .first() \
#         .rating_norma
#     print(rating)

print(" Agregado !")
# master_2_df = spark_session.createDataFrame(data = master_2, schema='rut string, dv string, nombre string, pais string, rating string')
# master_2_df.show()


#m1.show()
""" def predicate(x, sp, mdy, fitch):
    if (x['rating'] in [sp, mdy, fitch]):
        return             
    pass

filtered_rating = homologacion_rating_agencias.filter(lambda x: predicate(x)).collect()

m3 = {}
m1_iter = m1.rdd.toLocalIterator()
for row in m1_iter:
    sp = row['sp']
    mdy = row['mdy']
    fitch = row['fitch']
    rut = row['rut']
    filtered_ratings = homologacion_rating_agencias.rdd.filter(lambda x: predicate(x, sp, mdy, fitch)).collect() """

""" rating_norma_por_rut = []
m1_collected = m1.collect()
for row in m1_collected:
    sp = row['sp']
    mdy = row['mdy']
    fitch = row['fitch']
    rut = row['rut']
    rating_agencia = homol_rating_agencias \
        .filter(f" rating IN ('{sp}', '{mdy}','{fitch}') ") \
        .select("rating_norma") \
        .first() 
    if (rating_agencia == None):
        rating_norma_por_rut.append((rut, "-"))
    else:
        rating_norma_por_rut.append((rut, rating_agencia.rating_norma))
    
df = spark_session.createDataFrame(data = rating_norma_por_rut, schema='rut string, rating_norma string')
df.show()  """


""" def append_rating_norma(row):
    rut = row['rut']
    dv = row['dv']
    nombre = row['nombre']
    sp = row['sp']
    mdy = row['mdy']
    fitch = row['fitch']
    pais = row['pais']
    rating_agencia = homol_rating_agencias \
        .filter(f" rating IN ('{sp}', '{mdy}','{fitch}') ") \
        .select("rating_norma") \
        .first() 
    if (rating_agencia == None):
        return (rut, dv, nombre, pais, " ")    
    else:
        return (rut, dv, nombre, pais, rating_agencia.rating_norma)

def dummy(row):
    #print (row)
    return (row['rut'], 1)

master_2 = master_df.rdd.map(lambda row: dummy(row)).toDF()
master_2.show() """
""" m1_collect = m1.collect()
rating_norma_x_rut = ft.reduce(lambda a, b: a if a > b else b, lis) """



""" schema = StructType([
    StructField('rut', StringType(), True),
    StructField('rating_norma', StringType(), True)
])

df = spark_session.createDataFrame(data=rating_norma_por_rut, schema = schema)
df.printSchema()
df.show(truncate=False)
 """


#print(rating_norma_por_rut)



#agrega rating_empresa al master
""" def process1(rec):
    (sp, mdy, fitch) = (rec[3], rec[4], rec[5])
    return (rec[0], rec[1])

m2 = m1.rdd.map(lambda x: process1(x))
m3 = m2.toDF(["rut", "dv"])
m3.show()
 """




#m2.show()



"""
test_df_1 = spark_session.sql('''
SELECT FIRST(rating_norma) as rating_nomrma
FROM homol_rating_filt
WHERE rating IN ('A','A-','Baa2')
''')
test_df_1.show()

maestro_de_ratings_sqldf = spark_session.sql('''
SELECT 
    rat_emp.rut, 
    rat_emp.dv, 
    rat_emp.nombre, 
    -- homol_pais.pais,
    (
        SELECT FIRST(rating_norma)
        FROM homol_rating_filt
        --WHERE rating IN (rat_emp.sp, rat_emp.mdy, rat_emp.fitch) -- "Correlated column is not allowed in predicate" ...
        WHERE rating = rat_emp.sp -- Funciona pero no satisface el requerimiento..
    ) AS rating__empresa
FROM rating_empresa rat_emp
-- LEFT JOIN homologacion_pais homol_pais ON ucase(rat_emp.pais_bbg) = ucase(homol_pais.pais_bbg)
''')
maestro_de_ratings_sqldf.show() 
"""

""" test_df_2 = spark_session.sql('''
SELECT 
    ratemp.rut
FROM rating_empresa ratemp 
LEFT JOIN (SELECT hr.* FROM homol_rating_filt) rating_filt ON  
   
''')
test_df_2.show() """



""" 
ratings_sqldf_test = spark_session.sql('''
SELECT ra.* 
FROM (
    SELECT hr.* 
    FROM homologacion_rating hr
    WHERE hr.agencia_homol IN ('SP','MDY','FITCH')
    order by hr.orden_norma desc
) ra
WHERE ra.rating IN ('Baa2','BBB+','A-')
LIMIT 1
''')
ratings_sqldf_test.show() """

print (" >> OPERACION FINALIZADA << ")
