import pandas as pd
import numpy as np
import re

HOSTNAME_TORRE_COL = 'HOSTNAME_TORRE'
SERVICECODE_TORRE_COL = 'SERVICECODE_TORRE'
IP_GESTION_TORRE_COL = 'IP_GESTION_TORRE'

HOSTNAME_UCMDB_COL = 'HOSTNAME_UCMDB'
SERVICECODE_UCMDB_COL = 'SERVICECODE_UCMDB'
IP_GESTION_UCMDB_COL = 'IP_GESTION_UCMDB'
GLOBAlID_UCMDB_UCMDB_COL = 'GLOBAlID_UCMDB'


class general_tower_match:
    '''
    Definir los posibles nombres de hostname,ip gestion y codigo de servicio en 1 arreglo para torre
    # combinar todo en 1 solo dataframe garantizando que hostname, ip gestion y codserv sea igual
    #leer inv ucmdb, especificar los nombres de las columnas de inv ucmdb
    realizar cruce'''

    def read_and_combine_excel(self, rutaArchivo, tower_col_hostnames, tower_col_service_codes, tower_col_ip_gestion):
        try:
            dfs = []
            df_col_hostname = []
            df_col_ip_gestion = []
            df_col_service_code = []
            # leer todas las hojas de Excel en un diccionario de dataframes
            df_dict = pd.read_excel(rutaArchivo, sheet_name=None)
            # recorrer todas las hojas y agregar el nombre de la hoja como columna adicional
            for sheet_name, df in df_dict.items():
                df['hoja'] = sheet_name
                dfs.append(df)
            # combinar todos los dataframes en uno solo
            df_completo = pd.concat(dfs, ignore_index=True)
            # se extrae del dataframe los nombres de las columnas que se quieren
            # combinar y existen en el dataframe
            for cols in df_completo.columns:
                if cols in tower_col_hostnames:
                    df_col_hostname.append(cols)
                elif cols in tower_col_ip_gestion:
                    df_col_ip_gestion.append(cols)
                elif cols in tower_col_service_codes:
                    df_col_service_code.append(cols)
            # combinar columnas específicas en una sola columna
            df_completo[HOSTNAME_TORRE_COL] = df_completo[df_col_hostname].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            df_completo[SERVICECODE_TORRE_COL] = df_completo[df_col_service_code].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            df_completo[IP_GESTION_TORRE_COL] = df_completo[df_col_ip_gestion].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            # df_completo.drop(columns=df_col_hostname, inplace=True)
            # df_completo.drop(columns=df_col_service_code, inplace=True)
            # df_completo.drop(columns=df_col_ip_gestion, inplace=True)
            # mover laS columnas al principio del dataframe
            # hostnamme_torre =
            df_completo.insert(0, HOSTNAME_TORRE_COL,
                               df_completo.pop(HOSTNAME_TORRE_COL))
            df_completo.insert(1, SERVICECODE_TORRE_COL,
                               df_completo.pop(SERVICECODE_TORRE_COL))
            df_completo.insert(2, IP_GESTION_TORRE_COL,
                               df_completo.pop(IP_GESTION_TORRE_COL))
            return df_completo
        except Exception as e:
            print("Error al leer:", e)

    def extract_service_code(self, df, column_name, new_column_name):
        """Dado un dataFrame,retorna el Dataframe con codigos de servicios y los qe no cumplen el estandar los marca como na"""
        # se debe validar si el dato recibido es string
        regex_serv_Code = '(?i)(([a-z]{3}[0-9]{4}|[a-z]{5}[0-9]{2}|[a-z]{4}[0-9]{3})|([a-z]{4}[0-9]{4}|[a-z]{5}[0-9]{3}))'
        # tOMA LA PRIMERA COLUMNA (por alguna razón, creaba mas columnas)
        df[new_column_name] = df[column_name].str.extract(
            regex_serv_Code, expand=False)[1]
        df.insert(1, new_column_name, df.pop(new_column_name))
        return df

    def extract_ip(self, df, column_name, new_column_name):
        """Dado un dataFrame,retorna el Dataframe con las Ip SEPARADAS CON COMAs y los qe no cumplen el estandar los marca como na"""
        regex_ip = '(([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5])\.([01]?\d\d?|2[0-4]\d|25[0-5]))'

        def extract_ip_from_text(text):
            pattern = re.compile(regex_ip)
            listMatches = []
            for match in pattern.finditer(text):
                listMatches.append(match.group())
            return listMatches
        df[new_column_name] = df[column_name].apply(lambda ips: ','.join(
            extract_ip_from_text(ips)) if len(extract_ip_from_text(ips)) > 0 else "")
        df.insert(1, new_column_name, df.pop(new_column_name))
        return df
    # read_and_combine_excel(rutaArchivo)

    def read_ucmdb_excel(self, rutaArchivo, ucmdb_col_hostmame, ucmdb_col_servicecode, ucmdb_col_ip, ucmdb_col_global_id):
        try:
            df_col_hostname = []
            df_col_ip_gestion = []
            df_col_service_code = []
            df_col_globlal_id = []
            # Leyendo archivo excel
            df_ucmdb = pd.read_excel(rutaArchivo, sheet_name=None)
            # se extrae del dataframe los nombres de las columnas que se quieren
            # combinar y existen en el dataframe
            for column in df_ucmdb.columns:
                if column in ucmdb_col_hostmame:
                    df_col_hostname.append(column)
                elif column in ucmdb_col_ip(column):
                    df_col_ip_gestion.append(ucmdb_col_ip)
                elif column in ucmdb_col_servicecode:
                    df_col_service_code.append(column)
                elif column in ucmdb_col_global_id:
                    df_col_globlal_id.append(column)

            # combinar columnas específicas en una sola columna, pasando los campos vacios a string vacios
            df_ucmdb[HOSTNAME_UCMDB_COL] = df_ucmdb[df_col_hostname].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            df_ucmdb[SERVICECODE_UCMDB_COL] = df_ucmdb[df_col_service_code].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            df_ucmdb[IP_GESTION_UCMDB_COL] = df_ucmdb[df_col_ip_gestion].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            df_ucmdb[GLOBAlID_UCMDB_UCMDB_COL] = df_ucmdb[df_col_globlal_id].apply(
                lambda x: ''.join(x.dropna().astype(str)), axis=1)
            # mover laS columnas al principio del dataframe
            df_ucmdb.insert(0, HOSTNAME_UCMDB_COL,
                            df_ucmdb.pop(HOSTNAME_UCMDB_COL))
            df_ucmdb.insert(1, SERVICECODE_UCMDB_COL,
                            df_ucmdb.pop(SERVICECODE_UCMDB_COL))
            df_ucmdb.insert(2, IP_GESTION_UCMDB_COL,
                            df_ucmdb.pop(IP_GESTION_UCMDB_COL))
            df_ucmdb.insert(3, GLOBAlID_UCMDB_UCMDB_COL,
                            df_ucmdb.pop(GLOBAlID_UCMDB_UCMDB_COL))
            return df_ucmdb
        except Exception as e:
            print("Error al leer inv ucmdb: ", e)

    
    ##no se finalizo
    def match_invTown_vs_invUcmdb(self, df_tower, df_ucmdb, tower_col_hostname, tower_col_service_code, tower_col_ip_gestion, ucmdb_col_servicecode, ucmdb_col_displayLabel):
        """Realiza match de torre Vs Umcbd en base al codservicio y hostname, retorna df con los match"""
        print(
            f"Cantidad Host Inv torre = {len(df_tower)} | Cantidad registros Inv UCMDB = {len(df_ucmdb)}")
        df_tower = self.extract_service_code(
            df_tower, tower_col_service_code, 'SERVICECODE_TORRE_FORMAT')
        df_tower = test.extract_ip(
            df_tower, tower_col_ip_gestion, 'IP_GESTION_TORRE_FORMAT')
        # Colocado la ip extraida al df
        # df_tower=df_tower.merge(df_fileTowerWithIp,left_on=tower_col_hostnames,right_on=tower_col_hostnames, how='left').fillna(value='')
        # SE añade num consecutivos
        df_tower.insert(0, '#', np.arange(1, len(df_tower)+1))

        # 2 Left join de inv torre vs inv ucmdb en codigo de servicio,
        df_merge = df_tower.merge(df_ucmdb,
                                  left_on='SERVICECODE_TORRE_FORMAT', right_on=ucmdb_col_servicecode, how='left')
        print(df_tower)


test = general_tower_match()

'''
def main():

    caso_cruce = ""
    if (caso_cruce == "SAP"):
        rutaArchivo_sap = r"D:\OneDrive - GLOBAL HITSS\2023\5.Mayo\Auditorias DC\11 SAP\Inventario Consolidado_Codigos_de_Servicio.xlsx"
        col_hostname_sap = ['HOSTNAME', 'Hostname',
                            'hostname', 'NOMBRE', 'FQDN']
        col_service_code_sap = [
            'Codigo de Servicio', 'CODIGO DE SERVICIO', 'ID SERVICIO CLARO', 'Codigo de servicio']
        col_ip_gestion_sap = ['IP GESTION', 'IP_MGM',
                              'IPGestión', 'IP Gestion', 'IP Gestión', 'IP address']

        rutaArchivo_ucmdb = r"D:\OneDrive - GLOBAL HITSS\2023\6.Junio\Auditorias DC\16 PCCaaS\Infraestructura_Compilada.xlsx"
        ucmdb_col_hostmame = ['[Node] : Display Label',
                              '[RunningSoftware] : Display Label']
        ucmdb_col_servicecode = ['[Node] : [Onyx] - ServiceCode', '[Node] : Onyx ServiceCodes',
                                 '[Node] : Service Code', '[RunningSoftware] : Service Code',]
        ucmdb_col_ip = ['[Node] : IP Gestion',
                        '[RunningSoftware] : Application IP']
        ucmdb_col_global_id = ['[Node] : Global Id',
                               '[RunningSoftware] : Global Id']

        df_tower = test.read_and_combine_excel(
            rutaArchivo_sap, col_hostname_sap, col_service_code_sap, col_ip_gestion_sap)
        df_ucmdb = test.read_ucmdb_excel(
            rutaArchivo_ucmdb, ucmdb_col_hostmame, ucmdb_col_servicecode, ucmdb_col_ip, ucmdb_col_global_id)

        dfMatchTownVsUcmdb = test.match_invTown_vs_invUcmdb(
            df_tower, df_ucmdb, HOSTNAME_TORRE_COL, SERVICECODE_TORRE_COL, IP_GESTION_TORRE_COL, ucmdb_col_servicecode, ucmdb_col_displayLabel)

    print()

main()
'''



rutaArchivo_ucmdb=r"D:\OneDrive - GLOBAL HITSS\2023\6.Junio\Auditorias DC\16 PCCaaS\Infraestructura_Compilada.xlsx"

ucmdb_col_hostmame = ['[Node] : Display Label','[RunningSoftware] : Display Label']
ucmdb_col_servicecode=['[Node] : [Onyx] - ServiceCode','[Node] : Onyx ServiceCodes','[Node] : Service Code','[RunningSoftware] : Service Code',]
ucmdb_col_ip=['[Node] : IP Gestion','[RunningSoftware] : Application IP']
ucmdb_col_global_id=['[Node] : Global Id','[RunningSoftware] : Global Id']



try:
    #PARA PCCAS
    #rutaArchivo_pccaas=r"D:\OneDrive - GLOBAL HITSS\2023\4.Abril\Auditorias DC\16 PCCaaS\Infraestructura_Compilada.xlsx"
    rutaArchivo_pccaas=r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\16 PCCaaS\Copia de Infraestructura_Compilada. octubre.xlsx"

    col_hostname_pccaas = ['Roles ', 'Hostname', 'Nombre Maquina', 'HOSTNAME']
    col_service_code_pccaas=['HOST_NAME','Codigo','Cod Servicio','COD SERV','COD_SERVICIO','Codigo_hostname']
    col_ip_gestion_pccaas=['IP_Mangt','IP Gestion','Gestion /22','IP GESTION','IP-MGMT']
   
    #PARA AMT
    rutaArchivo_amt=r"D:\OneDrive - GLOBAL HITSS\2023\8.Agosto\Auditorias DC\3 AMT\NUEVA LINEA BASE - AMT 1.1.xlsx"
    rutaArchivoSalida="D:/OneDrive - GLOBAL HITSS/2023/"
    col_hostname_amt=['NOMBRE DEL SERVIDOR','SERVIDOR']
    col_service_code_amt=['CODIGO DE SERVICIO','ID DE SERVICIO','COD.SERV                  ']
    col_ip_gestion_amt=['IP DE GESTION ','IP GESTION','GESTION']
    #PARA SAP
    rutaArchivo_sap=r"D:\OneDrive - GLOBAL HITSS\2023\12. Diciembre\Auditorias DC\11 SAP\Consolidado_Codigos_de_Servicio_Diciembre23.xlsx"
    col_hostname_sap=['HOSTNAME','Hostname','hostname','NOMBRE','FQDN']
    col_service_code_sap=['Codigo de Servicio','CODIGO DE SERVICIO','ID SERVICIO CLARO','Codigo de servicio','COD SERV']
    col_ip_gestion_sap=['IP GESTION','IP_MGM','IPGestión','IP Gestion','IP Gestión','IP address']    
    
    #PARA EL INVENTARIO DE CIBERSEGURIDAD
    rutaArchivo_ciber=r"D:\OneDrive - GLOBAL HITSS\2023\9.Septiembre\Auditorias DC\15 Ciberseguridad\inv ciberseguridad.xlsx"
    col_hostname_ciber=['NOMBRE DE ACTIVO']
    col_service_code_ciber=['COD. SERVICIO']
    col_ip_gestion_ciber=['IP GESTION','IP_MGM','IPGestión','IP Gestion','IP Gestión','IP address']    
    
    #para inventario de firewall de ci administrados y retirados
    rutaArchivo_fw=r"D:\OneDrive - GLOBAL HITSS\2023\9.Septiembre\Auditorias DC\12 Firewall Administrado\INVENTARIO PLATAFORMAS SEGURIDAD V21 UNMERGED.xlsx"
    col_fw=['NOMBRE']
    col_service_code_fw=['CÓDIGO DE SERVICIOS']
    col_ip_gestion_fw=['IP']    
    
    #Firewall
    #dfcompleto=test.read_and_combine_excel(rutaArchivo_fw,col_fw,col_service_code_fw,col_ip_gestion_fw)
    #CIBERSEGURIDAD
    #dfcompleto=test.read_and_combine_excel(rutaArchivo_ciber,col_hostname_ciber,col_service_code_ciber,col_ip_gestion_ciber)
    
    #rutaArchivoSalida=r"D:\OneDrive - GLOBAL HITSS\2023\7 Julio\Auditorias DC"    
    #PCCAAS
    #dfcompleto=test.read_and_combine_excel(rutaArchivo_pccaas,col_hostname_pccaas,col_service_code_pccaas,col_ip_gestion_pccaas)
    #AMT
    #dfcompleto=test.read_and_combine_excel(rutaArchivo_amt,col_hostname_amt,col_service_code_amt,col_ip_gestion_amt)
    #SAP
    dfcompleto=test.read_and_combine_excel(rutaArchivo_sap,col_hostname_sap,col_service_code_sap,col_ip_gestion_sap)
    
    
    dfcompleto=test.extract_service_code(dfcompleto,'SERVICECODE_TORRE','SERVICECODE_TORRE_FORMAT')
    dfcompleto=test.extract_ip(dfcompleto,'IP_GESTION_TORRE','IP_GESTION_TORRE_FORMAT')
    # guardar el dataframe combinado en un archivo Excel
    dfcompleto.to_excel(rutaArchivoSalida+"\Test.xlsx",index=False)
    print(rutaArchivoSalida+"\Test.xlsx")
except RuntimeError as error:
        raise RuntimeError("Error ",error)

