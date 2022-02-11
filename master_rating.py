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
    print (f'{dataset}.csv ...')
    if not (os.path.isfile(f"{INPUT_DIRECTORY}/{dataset}.csv")):
        print (f"Arhivo de entrada no econtrado: {INPUT_DIRECTORY}/{dataset}.csv")
        print ("Proceso abortado. Saliendo... ")
        exit()
    load_csv_into_spark_tempview(dataset, INPUT_DIRECTORY, spark_session)
    print (f"Dataset '{dataset}' cargado!")

print ("--> Ejecutando querys ... ")

#crea dataframe con la primera muestra del master
master_1_df = spark_session.sql('''
SELECT 
    r_emp.rut AS rut, 
    r_emp.dv AS dv, 
    r_emp.nombre AS nombre,
    r_emp.sp AS sp,
    r_emp.mdy AS mdy,
    r_emp.fitch AS fitch,
    r_emp.pais_bbg, 
    homol_pais.pais AS pais
FROM rating_empresa r_emp
LEFT JOIN homologacion_pais homol_pais ON ucase(r_emp.pais_bbg) = ucase(homol_pais.pais_bbg)
''')
#crea vista de la primera muestra del master
master_1_df.createOrReplaceTempView("master_1_df")

#Filtra homologacion_rating dejando solo las agencias requeridas
#ordenadas de forma descendente (orden_norma)
homol_rating_agencias = spark_session.sql(''' 
SELECT *
FROM homologacion_rating
WHERE agencia_homol IN ('SP','MDY','FITCH')
ORDER BY orden_norma DESC
''')

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
        acc.append((rut, " "))    
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
    m.*, r.rating_norma AS rating_empresa
FROM master_1_df m
LEFT JOIN rating_norma_x_rut r ON m.rut = r.rut
''')
master_2_df.createOrReplaceTempView("master_2_df")
master_2_df.show()
print("--> Agregado!")

print("--> generando cruce rating_soberano_x_pais ...")
rating_soberano_df = spark_session.sql('''
SELECT 
    *
FROM rating_soberano
''')

#Funcion reductora que genera el cruce use entre rating_soberano y homologacion_rating
def reduce_getting_rating_soberano_x_pais(acc, row):
    sp = row['sp']
    mdy = row['mdy']
    fitch = row['fitch']
    pais_bbg = row['pais_bbg']
    rating_agencia = homol_rating_agencias \
        .filter(f" rating IN ('{sp}', '{mdy}','{fitch}') ") \
        .select("rating_norma") \
        .first() 
    if (rating_agencia == None):
        acc.append((pais_bbg, " "))    
    else:
        acc.append((pais_bbg, rating_agencia.rating_norma))
    return acc

#Aplica reductor a rating_soberano y luego crea un dataframe con la data obtenida
rating_soberano_df_collected = rating_soberano_df.collect()
rating_soberano_x_pais = ft.reduce(reduce_getting_rating_soberano_x_pais, rating_soberano_df_collected, [])
print("--> Generado! ")
print("--> creando TempView de rating_soberano_x_pais ...")
rating_soberano_x_pais_df = spark_session.createDataFrame(data = rating_soberano_x_pais, schema='pais_bbg string, rating_norma string')
# Crea una vista de rating_soberano_x_pais_df (para finalmente agregarla al master via SQL) 
rating_soberano_x_pais_df.createOrReplaceTempView("rating_soberano_x_pais")
rating_soberano_x_pais_df.show()

print ("--> agregando rating_soberano a master y eliminando campos innecesarios ... ")
master_3_df = spark_session.sql('''
SELECT 
    m.rut, m.dv, m.nombre, m.pais, m.rating_empresa, r.rating_norma AS rating_soberano
FROM master_2_df m
LEFT JOIN rating_soberano_x_pais r ON ucase(m.pais_bbg) = ucase(r.pais_bbg)
''')
master_3_df.createOrReplaceTempView("master_3_df")
master_3_df.show()
print("--> Agregado!")

#finalmente, exporta el resultado  a un CSV en el path 'data'
print("--> Exportando resultado a CSV: 'data/maestro_ratings.csv' ...")
master_3_panda_df = master_3_df.toPandas()
master_3_panda_df.to_csv ("data/maestro_ratings.csv", 
                index = None,
                header=True,
                sep='|')
print("--> Hecho! ")

print (" >> OPERACION FINALIZADA << ")
