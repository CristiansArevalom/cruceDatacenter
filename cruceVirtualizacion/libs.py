##METODOS COMUNES USADOS PARA TODOS LOS CRUCES DE PLATAFORMAS
#Librerias para fechas
from datetime import datetime #Clase
import datetime as dt #modulo
import locale
from sqlite3 import Date
#liberia, excel
#Pandas is used as a dataframe to handle Excel files
import pandas as pd
import numpy as np
from turtle import left
from xml.dom.pulldom import IGNORABLE_WHITESPACE
import os
from openpyxl import load_workbook

#locale.setlocale(locale.LC_TIME, "es_co")
RANGE_DAYS_LAST_ACCESS_TIME = 15

def csv_is_valid(file_csv_path):
    """Valida si el archivo csv tiene el formato correcto y si existe el archivo"""
    csv_is_valid=False
    if file_csv_path.endswith("csv") and os.path.isfile(file_csv_path):
        csv_is_valid=True
    else:
        raise Exception (f"El archivo csv {csv_is_valid} no es valido")
    return excel_is_valid

def excel_is_valid(file_excel_path):
    """Valida si el archivo excel tiene el formato correcto y si existe el archivo"""
    excel_is_valid=False
    if (file_excel_path.endswith(".xlsx") and os.path.isfile(file_excel_path)):
        excel_is_valid=True
    else:
        raise Exception (f"El archivo Excel {file_excel_path} no es valido")
    return excel_is_valid

def excel_and_sheet_name_is_valid(file_excel_path,sheet_name):
    """Valida si el archivo excel y la nhoja existen, retorna true o false"""
    print(file_excel_path)
    print(sheet_name)
    excel_is_valid=False
    if file_excel_path.endswith(".xlsx"):
        xl = pd.ExcelFile(file_excel_path)
        sheets_on_file=xl.sheet_names
        if(sheet_name in sheets_on_file):
                excel_is_valid=True
        else:
            #raise KeyError (f"La hoja {sheet_name} no se encuentra en el archivo excel {file_excel_path}")#TODOS LOS ARCHIVOS TEBEN TENER LAS HOJAS DICHAS
            print(f"hoja {sheet_name} no existe en archivo")
    else:
        raise Exception (f"El archivo Excel {file_excel_path} no es valido")
    return excel_is_valid

def append_df_to_excel(df, excel_path,sheet_name):
    """combina en un archivo excel nuevo/existente el dataframe que recibe como parametro"""
    if(os.path.isfile(excel_path)): 
        df_excel = pd.read_excel(excel_path)
        result = pd.concat([df_excel, df], ignore_index=True)
        result.to_excel(excel_path,sheet_name, index=False)
    else:
        df.to_excel(excel_path,sheet_name, index=False)

def combineExelfiles(input_file_path,start_name_files,sheet_names):
    """Combina en un archivo exel los que archivos de que empiecen por 'start_name' """
    excel_has_start_name_files=False
    excel_file_list = os.listdir(input_file_path)
    #print all the files stored in the folder, after defining the list
    excel_file_list
    #Once each file opens, use the append function to start consolidating the data stored in multiple files
    #create a new, blank dataframe, to handle the excel file imports
    df_sheet_names=[]
    #creando dataframe vacios por cada hoja que diga el metodo y se guardan en una lista
    for sheet_name in sheet_names:
        df_sheet_name=pd.DataFrame()
        df_sheet_names.append(df_sheet_name)
    #Run a for loop to loop through each file in the list
    print(f"Iniciando combinación de archivos que empiezan por {start_name_files}")
    for excel_files in excel_file_list:
    #check for .xlsx suffix files only
        print(excel_files)
        if excel_files.endswith(".xlsx"): 
            if excel_files.startswith(start_name_files) :
                excel_has_start_name_files=True
                xl = pd.ExcelFile(input_file_path+excel_files)
                sheets_on_file=xl.sheet_names
            #create a new dataframe to read/open each Excel file from the list of files created above
                for i in range (0, len(sheet_names)):
                    if(sheet_names[i] in sheets_on_file):
                        ##AQUI SE PUEDE DFINIR LAS HOJAS A COMBINAR
                        tempDf=pd.read_excel(input_file_path+excel_files,sheet_names[i])
                        ##Añade COLUMNA QUE DIGA DE QUE ARCHIVO SALIO EL DATO:
                        tempDf.insert(0,"file",excel_files,True)
                        #append each file into the original empty dataframe
                        df_sheet_names[i] = pd.concat([df_sheet_names[i], tempDf], ignore_index=True)
                        #df_sheet_names[i]=df_sheet_names[i].append(tempDf)
                    else:#raise KeyError (f"La hoja {sheet_names[i]} no se encuentra en el archivo excel {excel_files}")#TODOS LOS ARCHIVOS TEBEN TENER LAS HOJAS DICHAS
                         print(f"La hoja {sheet_names[i]} no se encuentra en el archivo excel {excel_files}")#TODOS LOS ARCHIVOS TEBEN TENER LAS HOJAS DICHAS)
            elif (not excel_has_start_name_files): raise Exception (f"Los archivos excel de la carpeta {input_file_path} no inician con {start_name_files}")#ALMENOS UN ARCHIVO DEBE INICIAR CON EL NOMBRE QUE DICE          
    output_file=input_file_path+"inv virtualizacion.xlsx"
    with pd.ExcelWriter(output_file) as writer:
            check_incorrect_df_flag=False
            for i in range (0, len(df_sheet_names)):
                df_sheet_names[i].to_excel(writer,sheet_name = sheet_names[i], index=False)
                if(int(df_sheet_names[i].size) !=0):                    
                    check_incorrect_df_flag=True
    print(f"El archivo combinado fue generado en la ruta {output_file}") if check_incorrect_df_flag else print("Error")
    return output_file

def extract_service_code(df,column_name):
    """Dado un dataFrame,retorna el Dataframe con codigos de servicios y los qe no cumplen el estandar los marca como na"""
    #se debe validar si el dato recibido es string
    regex_serv_Code='(?i)(([a-z]{3}[0-9]{4}|[a-z]{5}[0-9]{2}|[a-z]{4}[0-9]{3})|([a-z]{4}[0-9]{4}|[a-z]{5}[0-9]{3}))'
    df['servCode']=df[column_name].str.extract(regex_serv_Code, expand=False)[1]#tOMA LA PRIMERA COLUMNA (por alguna razón, creaba mas columnas)
    return df

def transform_Date(stringDate):
    """#Dada una fecha en formato (jueves 4 de agosto de 2022 23H27' COT) returna la fecha en formato datetime"""
   #return datetime.strptime(stringDate.replace("\' COT",""),"%A %d de %B de %Y %HH%M") if (type(stringDate)==str) else stringDate
    """Dada una fecha en formato Monday, January 23, 2023 12:47:05 PM COT returna la fecha en formato datetime """
    return datetime.strptime(stringDate.replace(" COT",""),"%A, %B %d, %Y %I:%M:%S %p") if (type(stringDate)==str) else stringDate

def check_day_stopover(last_AccessTime_Date):
    """#Dada una fecha en formato TIMESTAMP, retorna si esta en un rango de 0-15;15-30;30-60;mas de 60; en comparación con la fecha en que se realice la ejecución"""
    days_Range=""
    current_date = datetime.now()
    #mira si el tipo de dato es de TIMESTAMP, para poder hacer las restas
    if(type(last_AccessTime_Date))==pd._libs.tslibs.timestamps.Timestamp:
        last_AccessTime_Date.to_pydatetime()
        diffDays=(current_date-last_AccessTime_Date).days
        if(diffDays>=0 and diffDays<=15):
            days_Range="0 - 15 Dias"
        elif(diffDays>15 and diffDays<=30):
            days_Range="15 - 30 Dias"
        elif(diffDays>30 and diffDays<=60):
            days_Range="30 - 60 Dias"
        else:
           days_Range="Mas de 60 Días"
    else: 
        days_Range="Mas de 60 Días"
    return days_Range

def classify_access_time_status(update_days_range):
    """#Dado un rang de numero (0-15;15-30;30-60), retorna un string indicando el status descubrimieto (Actualizado,desctualizado,no descubierto)"""
    discovery_status=""
    if(update_days_range == "0 - 15 Dias"):
        discovery_status="Descubrimiento Actualizado"
    elif(update_days_range == "15 - 30 Dias"):
        discovery_status="Descubrimiento Desactualizado"
    elif(update_days_range=="30 - 60 Dias"):
       discovery_status="Descubrimiento Desactualizado"
    else:
        discovery_status="No descubierto"
    return discovery_status

