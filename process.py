import pandas as pd
import os

INPUT_FILE = "data.xlsx"
INPUT_SHEET_NAMES = ['homologacion_pais', 'homologacion_rating','rating_empresa','rating_soberano']
OUTPUT_DIR = "data"

def sheet_to_csv_from_xlsx(sheet_name, file_name, output_dir):
    dataframe = pd.read_excel (io=file_name, sheet_name=sheet_name)
    if(sheet_name == 'rating_soberano'):
        dataframe["sp"] = dataframe["sp"].map(lambda x: str(x).rstrip('u'))
    dataframe.to_csv (f"{output_dir}/{sheet_name}.csv", 
                index = None,
                header=True,
                sep='|')

def exist_sheet_in_file(sheet_name, file_name):
    dataframe = None
    try:
        dataframe = pd.read_excel (io=file_name, sheet_name=sheet_name)
    except Exception as e:
        print(e)
    return dataframe.__class__ != None 

print (" >> INICIANDO CONVERSION DE XLSX SHEETS EN ARCHIVOS CSV << ")

if not (os.path.isfile(INPUT_FILE)):
    print (f"Arhivo de entrada no econtrado: INPUT_FILE = ./{INPUT_FILE}")
    print ("Proceso abortado. Saliendo... ")
    exit()

if not (os.path.isdir(OUTPUT_DIR)):
    print (f"Directorio de salida no encontrado: OUTPUT_DIR = ./{OUTPUT_DIR}")
    print ("Proceso abortado. Saliendo... ")
    exit()

for sheet in INPUT_SHEET_NAMES:
    print (f'{sheet} ...')
    if not exist_sheet_in_file(sheet_name=sheet, file_name=INPUT_FILE):
        print (f"Hoja '{sheet}' no encontrada en el archivo '{INPUT_FILE}'")
        print ("Proceso incompleto. Saliendo... ")
        exit()
    sheet_to_csv_from_xlsx(sheet, INPUT_FILE, OUTPUT_DIR)
    print (f'{sheet} convertida y guardada!')

print (" >> OPERACION FINALIZADA << ")