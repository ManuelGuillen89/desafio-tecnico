from pyspark.sql import SparkSession
import os

CSV_FILE_NAMES = ['homologacion_pais', 'homologacion_rating','rating_empresa','rating_soberano']
INPUT_DIRECTORY = "data"

def load_csv_to_spark_tempview(csv_file_name, directory, spark_session):
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
    load_csv_to_spark_tempview(dataset, INPUT_DIRECTORY, spark_session)
    print (f"Dataset '{dataset}' cargado!")

print (" --> Generando query ... ")

#Filtra homologacion_rating dejando las agencias requeridas
homologacion_rating_filtrado = spark_session.sql(''' 
SELECT hr.*
FROM homologacion_rating hr
WHERE hr.agencia_homol IN ('SP','MDY','FITCH')
ORDER BY hr.orden_norma DESC
''')
homologacion_rating_filtrado.createOrReplaceTempView("homol_rating_filt")
#homologacion_rating_filtrado.show()


""" test_df_1 = spark_session.sql('''
SELECT 
    rat_emp.rut,
    (SELECT FIRST(hrf.rating_norma) 
        FROM homol_rating_filt hrf
        WHERE hrf.rating IN (rat_no_homol.sp, rat_no_homol.mdy, rat_no_homol.fitch)
    ) as rating
FROM rating_empresa rat_emp,
(SELECT sp, mdy, fitch FROM rating_empresa WHERE rating_empresa.rut = rat_emp.rut ) as rat_no_homol  
''')
test_df_1.show()
 """


test_df_2 = spark_session.sql('''
SELECT 
    FIRST(rating_por_rut.sp), rat_empresa.rut
FROM 
    (SELECT sp, mdy, fitch FROM rating_empresa WHERE rating_empresa.rut = rat_empresa.rut) as rating_por_rut,
    rating_empresa rat_empresa
''')
test_df_2.show()

""" maestro_de_ratings_sqldf = spark_session.sql('''
SELECT 
    rat_emp.rut, 
    rat_emp.dv, 
    rat_emp.nombre, 
    homol_pais.pais,
    (
        SELECT FIRST(hr.rating_norma) 
        FROM homologacion_rating hr
        WHERE hr.agencia_homol IN ('SP','MDY','FITCH')
        AND hr.rating IN (SELECT sp, mdy, fitch FROM rating_empresa WHERE rating_empresa.rut = rat_emp.rut )
        ORDER BY hr.orden_norma desc
    ) AS rating__empresa 
FROM rating_empresa rat_emp
LEFT JOIN homologacion_pais homol_pais ON ucase(rat_emp.pais_bbg) = ucase(homol_pais.pais_bbg)
''')
maestro_de_ratings_sqldf.show()   """


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
