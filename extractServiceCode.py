import pandas as pd
def extract_service_code(df,column_name,new_column_name):
    """Dado un dataFrame,retorna el Dataframe con codigos de servicios y los qe no cumplen el estandar los marca como na"""
    #se debe validar si el dato recibido es string
    regex_serv_Code='(?i)(([a-z]{3}[0-9]{4}|[a-z]{5}[0-9]{2}|[a-z]{4}[0-9]{3})|([a-z]{4}[0-9]{4}|[a-z]{5}[0-9]{3}))'
    df[new_column_name]=df[column_name].str.extract(regex_serv_Code, expand=False)[1]#tOMA LA PRIMERA COLUMNA (por alguna razón, creaba mas columnas)
    df.insert(1, new_column_name, df.pop(new_column_name))            
    return df

dfcompleto=pd.read_excel(r"D:\OneDrive - GLOBAL HITSS\2023\6.Junio\Inventario Hardware Claro Recursos Tecnologicos.xlsx")

dfcompleto=extract_service_code(dfcompleto,'CODIGO_SERVICIO','SERVICECODE_TORRE_FORMAT')

excel_path=r"D:\ECM3200I\Desktop\test.xlsx"
sheet_name="test"
dfcompleto.to_excel(excel_path,sheet_name, index=False)