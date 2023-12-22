from sqlite3 import Date
import libs
#Pandas is used as a dataframe to handle Excel files
import pandas as pd
import numpy as np
from datetime import date, datetime

STATUS_MATCH_UCMDB_COL='Status match en UCMDB'
OBSERVACIONES_MATCH_UCMDB_COL='Observaciones en UCMDB'

TOWER_COL_DISPLAYLABEL="Host"
TOWER_COL_IPADDRESS="IP Address"
TOWER_SHEET_NAMES=["vInfo","vHost","vCPU","vSC_VMK"] ##DEFINE LAS HOJAS QUE DESEA COMBINAR
TOWER_HOST_SHEET="vHost" #Hoja en donde estan los host de virtualización
TOWER_HOST_IPADDRESS_SHEET="vSC_VMK" #Hoja en donde estan los host de virtualización y las IP de gestion
UCMDB_COL_SERVICECODE="[Computer] : [Onyx] - ServiceCode"
UCMDB_COL_DISPLAYLABEL="[Computer] : Display Label"
UCMDB_COL_LAST_ACCESS_TIME="[Computer] : Last Access Time"
UCMDB_COL_IPADDRESS="[Computer] : IpAddress"
UCMDB_COL_VIRTUAL_CENTER="[Computer] : [VMware VirtualCenter] - Name"
SERVICE_MANAGER_COL_DISPLAYLABEL="Nombre para mostrar"
SERVICE_MANAGER_COL_SERVICECODE="Clr Service Code"
OUTPUT_FILE_PATH="D:/OneDrive - GLOBAL HITSS/Automatizmos/ArchivosParaPowerBi/Virtualizacion/"
RUN_DATE=datetime.now().strftime('%d/%m/%Y %H:%M:%S')

class Model:
    def __init__(self):
        self.input_file_path=""
        self.start_name_files=""
        self.ucmdb_inventory_path=""
        self.sm_inventory_path=""
        
        ##getters and setters
    def get_start_name_files(self):
        return self.start_name_files
    def set_start_name_files(self,start_name_files):
        self.start_name_files = start_name_files
    def get_input_file_path(self):
        return self.input_file_path
    def set_input_file_path(self,input_file_path):
        self.input_file_path = input_file_path
    def get_ucmdb_inventory_path(self):
        return self.ucmdb_inventory_path
    def set_ucmdb_inventory_path(self,ucmdb_inventory_path):
        self.ucmdb_inventory_path = ucmdb_inventory_path
    def get_sm_inventory_path(self):
        return self.sm_inventory_path
    def set_sm_inventory_path(self,sm_inventory_path):
        self.sm_inventory_path=sm_inventory_path

    ##toma hoja de archivo, extrae hoja con los hostnames y la Ip ADDRESS
    #Luego se debe hacer un join para poner ese atributo en el inventario torre
    #Luego hacer la busqueda por ip addres, leugo por cod servicio..
    def extract_hostname_ipAddress_from_sheet(self,tower_inventory_path,tower_host_ip_sheet):
        if(libs.excel_and_sheet_name_is_valid(tower_inventory_path,tower_host_ip_sheet)):
            df_fileTower=pd.read_excel(tower_inventory_path,sheet_name=tower_host_ip_sheet).fillna(value='')
            print(f"test cantidad registros{len(df_fileTower)}")
            #leer solo columnas host e ip address
            df_fileTower=df_fileTower[[TOWER_COL_DISPLAYLABEL,TOWER_COL_IPADDRESS]]
            #del resultado, agrupar por hostname
            df_fileTower[TOWER_COL_IPADDRESS]=df_fileTower.groupby([TOWER_COL_DISPLAYLABEL]
                                                                 ).transform(lambda ipAddress : ",".join(ipAddress))
            df_fileTowerWithIp=df_fileTower.drop_duplicates(subset=[TOWER_COL_DISPLAYLABEL])
            
        return df_fileTowerWithIp
    def ip_isin_string(self,ip,text):
        ips=ip.split(",")
        match=-1
        for ip in ips:
            if(ip+"," in text):
                match=1                
            elif(text.endswith(ip)):
                match=1 
        return match
    def check_match_status_ipAddress(self,df_PdtMatch,df_file_ucmdb,tower_col_ipAddress,ucmdb_col_ipAddress):
        #1 Remueve del df recibido las columnas que estan en el inv de ucmdb, para dejarlo sin el cruce.
        ucmdb_colums=df_file_ucmdb.columns.values.tolist()
        df_PdtMatch=df_PdtMatch.drop(columns=ucmdb_colums,axis=1)
        #2 HACE UN Fulljoin [cruce de todos x todos, no es lo mejor, pero por ahora se conserva], crea columna join con solo valores 1
        df_PdtMatch.insert(2,'join',1)
        df_file_ucmdb.insert(2,'join',1)
        dataFrame_Full = df_PdtMatch.merge(df_file_ucmdb, on='join').drop('join', axis=1)
        df_file_ucmdb.drop('join', axis=1, inplace=True) #Se borra la columna creada, join, El axis=1 indica que es columna, 0 es fila
        #3 Mira si la ip inv torre esta contenido entre las ip de Inv UCMDB, si es asi, lo marca como true en columna match. 
        dataFrame_Full['match'] = dataFrame_Full.apply(
            #lambda row: row[ucmdb_ipaddress].find(row[Tower_ipAddress]) if (len(row[Tower_ipAddress])>0 and len(row[ucmdb_ipaddress])>0) else -1, axis=1
            lambda row: self.ip_isin_string(row[tower_col_ipAddress],row[ucmdb_col_ipAddress]) if (len(row[tower_col_ipAddress])>0 and len(row[ucmdb_col_ipAddress])>0) else -1, axis=1
                ).ge(0)
        #4 SE guarda datos en un DF nuevo de solo lo que hizo match, se añade observacion y se vuelve a renombrar columnadel hostname en UCMDB
        df_match_ip=dataFrame_Full[dataFrame_Full['match']].copy()
        #5Validando si hostname o cod servicio coinciden
        df_match_ip[STATUS_MATCH_UCMDB_COL]=df_match_ip.apply(  
            lambda row:(self.check_match_status_serviceCode_Hostname((row[TOWER_COL_DISPLAYLABEL]),
            str(row['servCode']),str(row[UCMDB_COL_DISPLAYLABEL]),str(row[UCMDB_COL_SERVICECODE]))
            [0]),axis=1)
        df_match_ip[OBSERVACIONES_MATCH_UCMDB_COL]=df_match_ip.apply(  
            lambda row:(self.check_match_status_serviceCode_Hostname((row[TOWER_COL_DISPLAYLABEL]),
            str(row['servCode']),str(row[UCMDB_COL_DISPLAYLABEL]),str(row[UCMDB_COL_SERVICECODE]))
            [1]),axis=1)         
        df_match_ip.drop('match', axis=1, inplace=True) 
        return df_match_ip        


    def check_match_status_serviceCode_Hostname(self,tower_col_hostname,tower_col_servCode,ucmdb_col_displayLabel,ucmdb_col_servCode):
        status_match="pdt en reporte UCMDB"
        observations_match="No se logro identificar match"
        matchHostname=tower_col_hostname in ucmdb_col_displayLabel
        match_serviceCode=tower_col_servCode in ucmdb_col_servCode and (tower_col_servCode!='nan' and ucmdb_col_servCode!='nan')
        if(matchHostname and match_serviceCode):
            status_match="OK"
            observations_match=""
        elif(not matchHostname and match_serviceCode):
            status_match="pdt en reporte UCMDB"
            observations_match="No coincide hostname de inv torre en UCMDB"
        elif(matchHostname and not match_serviceCode):
            status_match="pdt en reporte UCMDB"
            observations_match="No coincide codigo de servicio de inv torre en UCMDB"
        elif(not matchHostname and not match_serviceCode):
            status_match="OK, Validar"
            observations_match="No coincide codigo de servicio ni hostname de inv torre en UCMDB"                
        return status_match,observations_match

    def match_by_Hostname(self,df_PdtMatch, df_file_ucmdb,ucmdb_col_displayLabel):
        """Realiza fullJoin para identificar match x hostname y retorna DF con ese match
            ##match_by_Hostname. BASADO EN https://www.geeksforgeeks.org/join-pandas-dataframes-matching-by-substring/
        """
        #1 Remueve del df recibido las columnas que estan en el inv de ucmdb, para dejarlo sin el cruce. 
        ucmdb_colums=df_file_ucmdb.columns.values.tolist()
        df_PdtMatch=df_PdtMatch.drop(columns=ucmdb_colums,axis=1)
        #2 HACE UN Fulljoin [cruce de todos x todos, no es lo mejor, pero por ahora se conserva]
        df_PdtMatch.insert(2,'join',1)
        df_file_ucmdb.insert(2,'join',1)
        dataFrame_Full = df_PdtMatch.merge(df_file_ucmdb, on='join').drop('join', axis=1)
        df_file_ucmdb.drop('join', axis=1, inplace=True) #Se borra la columna creada, join, El axis=1 indica que es columna, 0 es fila
        dataFrame_Full.rename(columns = {ucmdb_col_displayLabel:'display_label'},inplace = True) #Se renombra columna del hostname en UCMDB para poder usarlo como atributo del DF

        #3 Mira si el display label esta contenido entre el Host, si es asi, lo marca como true en columna match. 
        dataFrame_Full['match'] = dataFrame_Full.apply( #apply para aplicar a una sola fila
            lambda x: x.Host.find(x.display_label), axis=1).ge(0)  #AXIS 1 or ‘columns’: apply function to each row.
        #4 SE guarda datos en un DF nuevo de solo lo que hizo match, se añade observacion y se vuelve a renombrar columnadel hostname en UCMDB
        df_match_Hostname=dataFrame_Full[dataFrame_Full['match']]
        df_match_Hostname=df_match_Hostname.assign(Observaciones="validar modelado, coincide hostname pero no codigoServicio")
        df_match_Hostname.rename(columns = {'display_label':ucmdb_col_displayLabel},inplace = True)
        df_match_Hostname.drop('match', axis=1, inplace=True)

        return df_match_Hostname

    def check_discovery_status(self,df,date_name_Column):
        """REcibe un dataframe con fechas y retorna un dataframe con "estado descubrimiento (actualizado, desactualizado, no descubierto)
        Columna recuento de escala dias , con rango de 0-15;15-30;30-60;mas de 60"""
        #1 crear columna temporal con fecha formateada
        df['AccessTime']=df[date_name_Column].apply( 
            lambda stringDate : libs.transform_Date(stringDate)
            )
        #2Creando recuento escala dias
        df['RecuentoDias']=df['AccessTime'].apply(
        lambda last_AccessTime_Date: libs.check_day_stopover(last_AccessTime_Date))
        #3 Creando columna estado descubrimiento:
        df['DiscoveryStatus']=df['RecuentoDias'].apply(
            lambda update_days_range : libs.classify_access_time_status(update_days_range)
            )
        df.drop('AccessTime', axis=1, inplace=True)
        return df
    
    def checkVirtualCenter_status(self,df,ucmdbd_col_virtual_center,ucmdb_col_displayLabel):
        """Valida si el campo virtualcenter esta vacio o no para indicar si falta descubrir el Vcenter o no"""
        observacion_pdt_match="validar no se encontro match en base a hostname o cod servicio en el reporte"
        df[OBSERVACIONES_MATCH_UCMDB_COL]=np.where(((df[ucmdbd_col_virtual_center].str.len()==0) & (df[ucmdb_col_displayLabel].str.len()>0)) & (df[OBSERVACIONES_MATCH_UCMDB_COL].isnull()),"Pdt descubrir virtualcenter",df[OBSERVACIONES_MATCH_UCMDB_COL].apply(
            lambda row:  row+", Pdt descubrir virtualcenter" 
            if not(pd.isnull(row) ) and (row!=observacion_pdt_match) 
            else row
        ))
        return df
    
    
    def match_invTown_vs_invUcmdb(self,tower_inventory_path,tower_host_sheet,tower_col_displayLabel,ucmdb_inventory_path,ucmdb_col_servCode,ucmdb_col_displayLabel):       
        """Realiza match de torre Vs Umcbd en base al codservicio y hostname, retorna df con los match"""
        if(libs.excel_and_sheet_name_is_valid(tower_inventory_path,tower_host_sheet) and (libs.excel_is_valid(ucmdb_inventory_path))):
            print("..Iniciando cruce del inventario de la torre contra el de UCMDB..")
            #1,, leer excel y extraer codigos de servicio de hostname
            df_file_virtualization = pd.read_excel(tower_inventory_path,tower_host_sheet).fillna(value='')
            df_file_ucmdb = pd.read_excel(ucmdb_inventory_path).fillna(value='')
            df_file_ucmdb[ucmdb_col_servCode]=df_file_ucmdb[ucmdb_col_servCode].str.lower()
            print(f"Cantidad Host Inv torre = {len(df_file_virtualization)} | Cantidad registros Inv UCMDB = {len(df_file_ucmdb)}")
            df_file_virtualization=libs.extract_service_code(df_file_virtualization,tower_col_displayLabel)
            df_fileTowerWithIp=self.extract_hostname_ipAddress_from_sheet(tower_inventory_path,TOWER_HOST_IPADDRESS_SHEET)
            #Colocado la ip extraida al df_virtualziacion
            df_file_virtualization=df_file_virtualization.merge(df_fileTowerWithIp,left_on='Host',right_on='Host', how='left').fillna(value='')
            df_file_virtualization.insert(0,'#',np.arange(1,len(df_file_virtualization)+1))#SE añade num consecutivos
            #2 Left join de inv torre vs inv ucmdb en codigo de servicio,
            df_merge=df_file_virtualization.merge(df_file_ucmdb,
            left_on='servCode', right_on=ucmdb_col_servCode, how='left')
            # Valida si el nombre traido por el codigo de servicio corresponde exactamente al de inv torre y los clasifica. en ok, ok validar o pdt
            listTempHostOKMatchvsUcmdb=[]            
            for i in range(len(df_merge)):
                try:
                    name_inv_torre=df_merge.loc[i,tower_col_displayLabel]
                    name_inv_ucmdb=str(df_merge.loc[i,ucmdb_col_displayLabel])
                    if name_inv_ucmdb in name_inv_torre or name_inv_torre in name_inv_ucmdb :
                        df_merge.loc[i,STATUS_MATCH_UCMDB_COL] = "OK"                        
                        listTempHostOKMatchvsUcmdb.append(name_inv_ucmdb)
                    elif (name_inv_ucmdb not in name_inv_torre and name_inv_ucmdb !='nan'): #pdt validar
                        df_merge.loc[i,STATUS_MATCH_UCMDB_COL] = "OK"
                        df_merge.loc[i,OBSERVACIONES_MATCH_UCMDB_COL] = "validar no coincide hostname"
                    else:
                        df_merge.loc[i,STATUS_MATCH_UCMDB_COL] = "pdt en reporte UCMDB"
                        df_merge.loc[i,OBSERVACIONES_MATCH_UCMDB_COL] = "validar no se encontro match en base a hostname o cod servicio en el reporte"
                except:
                        None
                        #i=i #si encuentra error, no hace nada y pasa  a la siguient efila                    

            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_merge = df_merge.sort_values(by=[STATUS_MATCH_UCMDB_COL,OBSERVACIONES_MATCH_UCMDB_COL],na_position='first')
            #5 Validar los que no hicieron match x codigo de servicio, marcados como PDT (diferente a OK). 
            df_PdtMatch=df_merge[df_merge[STATUS_MATCH_UCMDB_COL] !="OK"]
           
            #TODO combinar df:matchip en el df_match_town_vs_ucdmdb, conservando las observaciones de df_matchip
            df_matchIp=self.check_match_status_ipAddress(df_PdtMatch,df_file_ucmdb,TOWER_COL_IPADDRESS,UCMDB_COL_IPADDRESS)
            #6 Une en el Df original los que encontro match x solo ip
            df_merge=pd.concat([df_merge,df_matchIp],axis=0)
            #sobreescribir en el DF del merge los que encontro x hostname (eliminando el duplicado que no encontro algo)
            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_merge = df_merge.sort_values(by=[STATUS_MATCH_UCMDB_COL,'Observaciones en UCMDB'],na_position='first')
            df_merge=df_merge.drop_duplicates(subset=[tower_col_displayLabel])
            df_merge = df_merge.sort_values(by=['#'])
                
            #si no hace match por cod de servicio ni ip, buscar porhostname. 
            df_match_Hostname=self.match_by_Hostname(df_PdtMatch,df_file_ucmdb,ucmdb_col_displayLabel)
            #6 Une en el Df original los que encontro match x solo hostname
            df_merge=pd.concat([df_merge,df_match_Hostname],axis=0)
            #sobreescribir en el DF del merge los que encontro x hostname (eliminando el duplicado que no encontro algo)
            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_merge = df_merge.sort_values(by=[STATUS_MATCH_UCMDB_COL,'Observaciones'],na_position='first')
            df_merge=df_merge.drop_duplicates(subset=[tower_col_displayLabel])
            df_merge = df_merge.sort_values(by=['#'])

            """1 ELIMINAR DATOS DUPLICADOS DE TORRE VS UCMDB EN DONDE YA HAYA ENCONTRADO COINCIDENCIA OK"""
            #(REMOVER SI HAY UNO CON STATUS MATCH OK Y OTRO CON STATUS MATCHPDT, se conserva los)    
            #df_duplicateHostUcmdb=df_merge[df_merge.duplicated(subset=ucmdb_col_displayLabel,keep=False)]
            df_duplicateHostUcmdb=df_merge[df_merge.duplicated(subset=[ucmdb_col_displayLabel,ucmdb_col_servCode],keep=False)]
            df_duplicateHostUcmdb=df_duplicateHostUcmdb[df_duplicateHostUcmdb[ucmdb_col_displayLabel].notnull()]
            #Eliminar registro desdde columna ucmdb_col_displayLabel hasta antes de status match
            #si esta en listTempHostOKMatchvsUcmdb, remover los que no sean OK,"
            index_ucmdb_hostCol=df_merge.columns.get_loc(ucmdb_col_displayLabel)
            index_ucmdb_observaciones=df_merge.columns.get_loc(OBSERVACIONES_MATCH_UCMDB_COL)
            index_ucmdb_num=df_merge.columns.get_loc('#')
            index_duplicates_pdt_delete=[]
            #Retorna el # de los duplicados que no estan OK y ya se encontraron en otra coincidencia como OK "listTempHostOKMatchvsUcmdb"            
            df_duplicateHostUcmdb.apply(                
                lambda row: index_duplicates_pdt_delete.append(row[index_ucmdb_num])
                if (row[index_ucmdb_hostCol] in listTempHostOKMatchvsUcmdb ) and not (pd.isnull(row[index_ucmdb_observaciones]))
                else None,axis=1)                
            if(len(index_duplicates_pdt_delete)>0):
                for index_delete in index_duplicates_pdt_delete:
                    originalRow=df_file_virtualization.loc[df_file_virtualization["#"]==index_delete].copy()
                    originalRow[STATUS_MATCH_UCMDB_COL]="pdt en reporte UCMDB"
                    originalRow[OBSERVACIONES_MATCH_UCMDB_COL]="validar no se encontro match en base a hostname o cod servicio en el reporte"
                    #print(df_merge.loc[df_merge["#"]==index_delete].index)
                    df_merge=df_merge.drop((df_merge.loc[df_merge["#"]==index_delete].index))                  
                    df_merge=pd.concat([df_merge,originalRow],ignore_index=True)
                    #df_merge=df_merge.append(originalRow,ignore_index = True)         
            df_merge = df_merge.sort_values(by=['#'])
            return df_merge
    
    
    def match_invUcmdb_vs_InvTown(self,ucmdb_inventory_path,ucmdb_col_displayLabel,ucmdb_col_servCode,df_match_town_vs_ucmdb):
        """Realiza match de UCMDB Vs torre en base al resultado de torre vs UCMDB"""

        #1)Leer archivo Excel
        #2 formar llave unica en inv ucmdb (displayeLabel_Codigo servicio)
        df_file_ucmdb=pd.read_excel(ucmdb_inventory_path)
        
        df_file_ucmdb['COD_HOSTNAME']=df_file_ucmdb[ucmdb_col_servCode].str.lower()+"_"+df_file_ucmdb[ucmdb_col_displayLabel].str.lower()
        df_match_town_vs_ucmdb['COD_HOSTNAME']=df_match_town_vs_ucmdb[ucmdb_col_servCode].str.lower()+"_"+df_match_town_vs_ucmdb[ucmdb_col_displayLabel].str.lower()
        df_match_town_vs_ucmdb=df_match_town_vs_ucmdb.sort_values(by=[STATUS_MATCH_UCMDB_COL,'Observaciones'],na_position='first')
        #3hacer un leftjoin y de inv UCBDM A dfMatchTownVsUcmdb, y traer el atributo de status match+observaciones
        df_match_ucmdb_vs_town=df_file_ucmdb.merge(df_match_town_vs_ucmdb,left_on='COD_HOSTNAME',right_on='COD_HOSTNAME',how='left', suffixes=('', '_remove')) ## le coloca sufixes "remove" para nombrar columnas duplicadas   
        #Elimina columnas duplicadas
        df_match_ucmdb_vs_town.drop([i for i in df_match_ucmdb_vs_town.columns if 'remove' in i],
               axis=1, inplace=True)
        
        #4)Mirar si se generan duplicados x codigo servicio + hostname, 
            #Si coincide, escoja el que tiene el ultimo acceso actualizAdo
        #5 las celdas que tengan en la columna status match marcadas como pdt en reporte UCMDB , marcarlas como PDT, y las vacias como que no estan en inventario torre
        df_match_ucmdb_vs_town["Status match Torre"]=df_match_ucmdb_vs_town[STATUS_MATCH_UCMDB_COL].apply(            
            lambda match_status : ("No esta en inv torre" if ( str(match_status) =="pdt en reporte UCMDB" or str(match_status) =="nan" ) else str(match_status) )
        )
        return df_match_ucmdb_vs_town
    
    def match_invUcmdbAndTown_vs_ServiceManager(self,serviceManager_path,df_match_ucmdb_vs_town,serviceManager_col_displayLabel,serviceManager_col_servCode):
        """Realiza match de 'UCMDB Vs torre VS SM' en base al resultado de UCMDB vs TORRE"""
        #1)Leer archivo service manager y separar x columnas
        df_service_Manager=pd.read_csv(serviceManager_path,sep=';')
        #2 leer dataframe del resultado match_invUcmdb_vs_InvTown (trae lo que esta en ucmdb y es administrado)
        #3 creando uniqueKey codServ_Hostname
        df_service_Manager['COD_HOSTNAME']=df_service_Manager[serviceManager_col_servCode].str.lower()+"_"+df_service_Manager[serviceManager_col_displayLabel].str.lower()
        #4 Left join de inv UCMDband Town vs Sevicemanager
        df_match_inv_ucmdb_and_town_vs_sm=df_match_ucmdb_vs_town.merge(df_service_Manager,left_on="COD_HOSTNAME",right_on="COD_HOSTNAME",how="left")
        df_match_inv_ucmdb_and_town_vs_sm["Status match SM"]=df_match_inv_ucmdb_and_town_vs_sm[serviceManager_col_displayLabel].apply(
            lambda sm_displayLabel:"OK SM" if str(sm_displayLabel)!="nan" else "PDT SM"
        )
        #4)Mirar si se generan duplicados x codigo servicio + hostname,
        # 5 Formar un df temporal de todos los duplicados x codHostname
        df_duplicate_CodHostname = df_match_inv_ucmdb_and_town_vs_sm[df_match_inv_ucmdb_and_town_vs_sm.duplicated(["COD_HOSTNAME"], keep=False)]
        df_duplicate_CodHostname = df_duplicate_CodHostname.sort_values(by=['Fecha y hora de modificación del sistema','COD_HOSTNAME'],ascending=False,na_position='first') #ORDENA alfabeticamente x status match y observaciones
        df_duplicate_CodHostname=df_duplicate_CodHostname.drop_duplicates(subset=['COD_HOSTNAME'])
        df_duplicate_CodHostname["Observaciones ServiceManager"]="Duplicados en Servicemanager"

        df_match_inv_ucmdb_and_town_vs_sm=pd.concat([df_match_inv_ucmdb_and_town_vs_sm,df_duplicate_CodHostname],axis=0)
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['Observaciones ServiceManager','COD_HOSTNAME'],na_position='last') #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
        df_match_inv_ucmdb_and_town_vs_sm=df_match_inv_ucmdb_and_town_vs_sm.drop_duplicates(subset=['COD_HOSTNAME'])
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['#'],na_position='last')

        return df_match_inv_ucmdb_and_town_vs_sm

    def mainModel(self):
        start_name_files=self.start_name_files
        input_file_path=self.input_file_path

        tower_inventory_path=libs.combineExelfiles(input_file_path,start_name_files,TOWER_SHEET_NAMES)
        #Validar si se añade paso par ala oja que tenga la sips, quede es separada con ,
        ucmdb_inventory_path=self.ucmdb_inventory_path
        serviceManager_path=self.sm_inventory_path

        #TORRE VS UCMDB
        dfMatchTownVsUcmdb=self.match_invTown_vs_invUcmdb(tower_inventory_path,TOWER_HOST_SHEET, TOWER_COL_DISPLAYLABEL,ucmdb_inventory_path,UCMDB_COL_SERVICECODE, UCMDB_COL_DISPLAYLABEL)
        dfMatchTownVsUcmdb=self.check_discovery_status(dfMatchTownVsUcmdb,UCMDB_COL_LAST_ACCESS_TIME)
        dfMatchTownVsUcmdb=self.checkVirtualCenter_status(dfMatchTownVsUcmdb,UCMDB_COL_VIRTUAL_CENTER,UCMDB_COL_DISPLAYLABEL)
        rutaArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"CruceVirtualizacionVsUcmdb.xlsx"
        rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoVirtualizacionTorreVsUCmdb.xlsx"
        dfMatchTownVsUcmdb['Fecha_Reporte']=RUN_DATE
        dfMatchTownVsUcmdb.to_excel(rutaArchivoTownVsUCMDB,index=False)
        libs.append_df_to_excel(dfMatchTownVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistVirtualizacionTorreVsUCmdb")
        print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        

        #UCMDB VS TORRE
        df_match_UcmdbVsTown=self.match_invUcmdb_vs_InvTown(ucmdb_inventory_path,UCMDB_COL_DISPLAYLABEL,UCMDB_COL_SERVICECODE,dfMatchTownVsUcmdb);
        rutaArchivoUCMDBvsTown=OUTPUT_FILE_PATH+"CruceUCMDBvSVirtualizacion.xlsx"
        df_match_UcmdbVsTown.to_excel(rutaArchivoUCMDBvsTown,index=False);
        print(f"El archivo con el cruce UCMDB VS TORRE fue generado en la ruta {rutaArchivoUCMDBvsTown}")        

        #UCMDB&TORRE VS SERVICE MANAGER
        df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(serviceManager_path,df_match_UcmdbVsTown,SERVICE_MANAGER_COL_DISPLAYLABEL,SERVICE_MANAGER_COL_SERVICECODE)
        rutaArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"CruceUCMDByTorreVsServiceManager.xlsx"
        rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistVirtualizacionUCMDBvsTorreVsSM.xlsx"
        df_match_invUcmdbAndTown_vs_SM['Fecha_Reporte']=RUN_DATE
        
        libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistVirtUCMDBvsTorreVsSM")
        df_match_invUcmdbAndTown_vs_SM.to_excel(rutaArchivoUCMDByTorreVsServiceManager,index=False);
        #print(f"El archivo con el cruce UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaArchivoUCMDByTorreVsServiceManager}")
        print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")

        return ["cruce torre vs UCMDB: "+rutaArchivoTownVsUCMDB,"Cruce UCMDB vs Torre: "+rutaArchivoUCMDBvsTown,"Cruce UCMDB adm por torre Vs sM: "+rutaArchivoUCMDByTorreVsServiceManager]


    def testmainv2(self):
        start_name_files = "2023-11-20-06-00-"
        input_file_path = "D:/OneDrive - GLOBAL HITSS/2023/2.Febrero/Auditorias DC/14 Virtualizacion/RF1558894"
        #input_file_path = "D:/OneDrive - GLOBAL HITSS/2022/7.Julio/Auditorias DC/14. Virtualización/"       
        #tower_inventory_path=libs.combineExelfiles(input_file_path,start_name_files,tower_sheet_names)
        tower_inventory_path=r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\14 Virtualizacion\INVENTARIO_COMPLETO\RVTOOLS_COMPLETO\VMWARE\inv virtualizacion.xlsx"
        ucmdb_inventory_path=r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\14 Virtualizacion\inv ucmdb.xlsx"
        serviceManager_path=r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\14 Virtualizacion\export (20).csv"
        test=self.extract_hostname_ipAddress_from_sheet(tower_inventory_path,TOWER_HOST_IPADDRESS_SHEET)

        #TORRE VS UCMDB
        dfMatchTownVsUcmdb=self.match_invTown_vs_invUcmdb(tower_inventory_path,TOWER_HOST_SHEET, TOWER_COL_DISPLAYLABEL,ucmdb_inventory_path,UCMDB_COL_SERVICECODE, UCMDB_COL_DISPLAYLABEL)
        dfMatchTownVsUcmdb=self.check_discovery_status(dfMatchTownVsUcmdb,UCMDB_COL_LAST_ACCESS_TIME)
        dfMatchTownVsUcmdb=self.checkVirtualCenter_status(dfMatchTownVsUcmdb,UCMDB_COL_VIRTUAL_CENTER,UCMDB_COL_DISPLAYLABEL)
        rutaArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"CruceVirtualizacionVsUcmdb.xlsx"
        rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoVirtualizacionTorreVsUCmdb.xlsx"
        dfMatchTownVsUcmdb['Fecha_Reporte']=RUN_DATE

        dfMatchTownVsUcmdb.to_excel(rutaArchivoTownVsUCMDB,index=False)
        libs.append_df_to_excel(dfMatchTownVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistVirtualizacionTorreVsUCmdb")

        print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        

        #UCMDB VS TORRE
        df_match_UcmdbVsTown=self.match_invUcmdb_vs_InvTown(ucmdb_inventory_path,UCMDB_COL_DISPLAYLABEL,UCMDB_COL_SERVICECODE,dfMatchTownVsUcmdb);
        rutaArchivoUCMDBvsTown=OUTPUT_FILE_PATH+"CruceUCMDBvSVirtualizacion.xlsx"
        df_match_UcmdbVsTown.to_excel(rutaArchivoUCMDBvsTown,index=False);
        print(f"El archivo con el cruce UCMDB VS TORRE fue generado en la ruta {rutaArchivoUCMDBvsTown}")        

        #UCMDB&TORRE VS SERVICE MANAGER
        df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(serviceManager_path,df_match_UcmdbVsTown,SERVICE_MANAGER_COL_DISPLAYLABEL,SERVICE_MANAGER_COL_SERVICECODE)
        rutaArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"CruceUCMDByTorreVsServiceManager.xlsx"
        rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistVirtualizacionUCMDBvsTorreVsSM.xlsx"
        df_match_invUcmdbAndTown_vs_SM['Fecha_Reporte']=RUN_DATE
        
        libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistVirtUCMDBvsTorreVsSM")
        df_match_invUcmdbAndTown_vs_SM.to_excel(rutaArchivoUCMDByTorreVsServiceManager,index=False);
        #print(f"El archivo con el cruce UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaArchivoUCMDByTorreVsServiceManager}")
        print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")

        return ["cruce torre vs UCMDB: "+rutaArchivoTownVsUCMDB,"Cruce UCMDB vs Torre: "+rutaArchivoUCMDBvsTown,"Cruce UCMDB adm por torre Vs sM: "+rutaArchivoUCMDByTorreVsServiceManager]

'''
model=Model()
model.testmainv2()

        select
            t.transaction_id
            , t.user_idm
            , t.value
            , u.user_id as user_id_r
            , u.favorite_color
        from
            transactions t
            left join
            users u
            on t.user_id = u.user_id
        ;


        left_df.merge(right_df.rename({'user_id': 'user_id_r'}, axis=1),
                    left_on='user_id', right_on='user_id_r', how='left')
'''

