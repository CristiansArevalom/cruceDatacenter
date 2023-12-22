import pandas as pd
#rutaArchivo=r"D:\OneDrive - GLOBAL HITSS\2023\4.Abril\Auditorias DC\3 AMT\ORIGINALNUEVA LINEA BASE - AMT.xlsx"
#rutaArchivoSalida="D:/OneDrive - GLOBAL HITSS/2023/4.Abril/Auditorias DC/3 AMT/"
rutaArchivo=r"D:\OneDrive - GLOBAL HITSS\2023\4.Abril\Auditorias DC\16 PCCaaS\Infraestructura_Compilada.xlsx"
rutaArchivoSalida="D:/OneDrive - GLOBAL HITSS/2023/4.Abril/Auditorias DC/16 PCCaaS/"

class amt_model:
    def read_and_combine_excel(rutaArchivo):
        try:
            #SE debe dejar traer el nombre del cliente ne base al nombre de la hoja
            df_hojas=pd.read_excel(rutaArchivo,sheet_name=None)            
            df_completo = pd.concat((df_hojas.values()), ignore_index=True)
            print(df_completo)
            print("crero df")
            df_completo.to_excel(rutaArchivoSalida+"inventario.xlsx",index=False)
        except:
            print("error al leer"+KeyError )
    def read_and_combine_excel(rutaArchivo, rutaArchivoSalida):
        try:
            dfs = []
            # leer todas las hojas de Excel en un diccionario de dataframes
            df_dict = pd.read_excel(rutaArchivo, sheet_name=None)
            # recorrer todas las hojas y agregar el nombre de la hoja como columna adicional
            for sheet_name, df in df_dict.items():
                df['hoja'] = sheet_name
                dfs.append(df)
            # combinar todos los dataframes en uno solo
            df_completo = pd.concat(dfs, ignore_index=True)
            print(df_completo)
            # guardar el dataframe combinado en un archivo Excel
            df_completo.to_excel(rutaArchivoSalida+"inventario.xlsx",index=False)
        except Exception as e:
            print("Error al leer:", e)      
      
    #read_and_combine_excel(rutaArchivo)
    read_and_combine_excel(rutaArchivo,rutaArchivoSalida)