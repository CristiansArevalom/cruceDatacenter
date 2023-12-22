from datetime import date, datetime
from pyexpat import model
from turtle import right
import libs
import pandas as pd
import numpy as np

STATUS_MATCH_UCMDB_COL='Status match en UCMDB'
OBSERVACIONES_MATCH_UCMDB_COL='Observaciones en UCMDB'
COD_HOSTNAME_COL='COD_HOSTNAME'
TOWER_COL_DISPLAYLABEL='Nombre de host'
TOWER_COL_HOSTNAME='Nombre del sistema'
TOWER_COL_SERVCODE='servCode'
TOWER_IP_ADDRESS='Dirección de gestión'
UCMDB_COL_SERVICECODE='[InfrastructureElement] : Service Code'
UCMDB_COL_DISPLAYLABEL='[InfrastructureElement] : Display Label'
UCMDB_COL_IPADDRESS='[InfrastructureElement] : IpAddress'
UCMDB_COL_LAST_ACCESS_TIME='[InfrastructureElement] : Last Access Time'
SERVICE_MANAGER_COL_DISPLAYLABEL='Nombre para mostrar'
SERVICE_MANAGER_COL_SERVICECODE='Clr Service Code'
SERVICE_MANAGER_COL_SUBTIPO='Subtipo'
SERVICE_MANAGER_COL_ACCESS_TIME='Fecha y hora de modificación del sistema'
OUTPUT_FILE_PATH="D:/OneDrive - GLOBAL HITSS/Automatizmos/ArchivosParaPowerBi/Red/"

RUN_DATE=datetime.now().strftime('%Y/%m/%d %H:%M:%S')

class Model:
    def __init__(self):
        self.tower_inventory_path_router=""
        self.tower_inventory_path_switch=""
        self.ucmdb_inventory_path=""
        self.sm_inventory_path=""
        self.tower_col_displayLabel=TOWER_COL_DISPLAYLABEL
        self.tower_col_servCode=TOWER_COL_SERVCODE
        self.tower_col_hostname=TOWER_COL_HOSTNAME
        self.tower_col_ipAddress=TOWER_IP_ADDRESS
        self.ucmdb_col_servCode=UCMDB_COL_SERVICECODE
        self.ucmdb_col_displayLabel=UCMDB_COL_DISPLAYLABEL
        self.ucmdb_col_LastAccessTime=UCMDB_COL_LAST_ACCESS_TIME
        self.ucmdb_col_ipAddress=UCMDB_COL_IPADDRESS
        self.serviceManager_col_displayLabel=SERVICE_MANAGER_COL_DISPLAYLABEL
        self.serviceManager_col_servCode=SERVICE_MANAGER_COL_SERVICECODE
        self.serviceManager_col_subtipo=SERVICE_MANAGER_COL_SUBTIPO
        self.serviceManager_col_accessTime=SERVICE_MANAGER_COL_ACCESS_TIME
        self.output_file_path=OUTPUT_FILE_PATH
        self.status_match=""
        self.observations_match=""


    def get_tower_inventory_path_router(self):
        return self.tower_inventory_path_router       
    def set_tower_inventory_path_router(self,tower_inventory_path_router):
        self.tower_inventory_path_router=tower_inventory_path_router
        return self.tower_inventory_path_router
    def get_tower_inventory_path_switch(self):
        return self.tower_inventory_path_switch
    def set_tower_inventory_path_switch(self,tower_inventory_path_switch):
        self.tower_inventory_path_switch=tower_inventory_path_switch
        return self.tower_inventory_path_switch
    def get_ucmdb_inventory_path(self):
        return self.ucmdb_inventory_path
    def set_ucmdb_inventory_path(self,ucmdb_inventory_path):
        self.ucmdb_inventory_path=ucmdb_inventory_path
    def get_sm_inventory_path(self):
        return self.sm_inventory_path
    def set_sm_inventory_path(self,sm_inventory_path):
        self.sm_inventory_path=sm_inventory_path

    def check_match_status_ipAddress(self,df_PdtMatch,df_file_ucmdb,Tower_ipAddress,ucmdb_ipaddress):
        """Realiza fullJoin para identificar match x IP y retorna DF con ese match
            ##match_by_String. BASADO EN https://www.geeksforgeeks.org/join-pandas-dataframes-matching-by-substring/
        """
        #1 Remueve del df recibido las columnas que estan en el inv de ucmdb, para dejarlo sin el cruce. 
        ucmdb_colums=df_file_ucmdb.columns.values.tolist()
        df_PdtMatch=df_PdtMatch.drop(columns=ucmdb_colums,axis=1)
        #2 HACE UN Fulljoin [cruce de todos x todos, no es lo mejor, pero por ahora se conserva], crea columna join con solo valores 1
        df_PdtMatch.insert(2,'join',1)
        df_file_ucmdb.insert(2,'join',1)
        dataFrame_Full = df_PdtMatch.merge(df_file_ucmdb, on='join').drop('join', axis=1)
        df_file_ucmdb.drop('join', axis=1, inplace=True) #Se borra la columna creada, join, El axis=1 indica que es columna, 0 es fila
        #dataFrame_Full.rename(columns = {ucmdb_ipaddress:'display_label'},inplace = True) #Se renombra columna del hostname en UCMDB para poder usarlo como atributo del DF
        #3 Mira si la ip inv torre esta contenido entre las ip de Inv UCMDB, si es asi, lo marca como true en columna match. 
        dataFrame_Full['match'] = dataFrame_Full.apply(
            #lambda row: row[ucmdb_ipaddress].find(row[Tower_ipAddress]) if (len(row[Tower_ipAddress])>0 and len(row[ucmdb_ipaddress])>0) else -1, axis=1
            lambda row: self.ip_isin_string(row[Tower_ipAddress],row[ucmdb_ipaddress]) if (len(row[Tower_ipAddress])>0 and len(row[ucmdb_ipaddress])>0) else -1, axis=1
                ).ge(0)
        #4 SE guarda datos en un DF nuevo de solo lo que hizo match, se añade observacion y se vuelve a renombrar columnadel hostname en UCMDB
        df_match_ip=dataFrame_Full[dataFrame_Full['match']].copy()
        #5Validando si hostname o cod servicio coinciden
        df_match_ip[STATUS_MATCH_UCMDB_COL]=df_match_ip.apply(  
            lambda row:(self.check_match_status_serviceCode_Hostname((row[TOWER_COL_HOSTNAME]),
            str(row[self.tower_col_servCode]),str(row[self.ucmdb_col_displayLabel]),str(row[self.ucmdb_col_servCode]))
            [0]),axis=1)
        df_match_ip[OBSERVACIONES_MATCH_UCMDB_COL]=df_match_ip.apply(  
            lambda row:(self.check_match_status_serviceCode_Hostname((row[TOWER_COL_HOSTNAME]),
            str(row[self.tower_col_servCode]),str(row[self.ucmdb_col_displayLabel]),str(row[self.ucmdb_col_servCode]))
            [1]),axis=1)         
        df_match_ip.drop('match', axis=1, inplace=True) 
        return df_match_ip
        #Retorna array[STATUSMATCH,OBSERVACIONES]
    
    #Recibe dataframe + columnas, retorna el datframe que coincide codig o hostname
    def check_match_status_code_hostname(self,df_PdtMatch,df_file_ucmdb,Tower_ipAddress,ucmdb_ipaddress):
        #1 Remueve del df recibido las columnas que estan en el inv de ucmdb, para dejarlo sin el cruce. 
        ucmdb_colums=df_file_ucmdb.columns.values.tolist()
        df_PdtMatch=df_PdtMatch.drop(columns=ucmdb_colums,axis=1)
        #2 HACE UN Fulljoin [cruce de todos x todos, no es lo mejor, pero por ahora se conserva], crea columna join con solo valores 1
        df_PdtMatch.insert(2,'join',1)
        df_file_ucmdb.insert(2,'join',1)
        dataFrame_Full = df_PdtMatch.merge(df_file_ucmdb, on='join').drop('join', axis=1)
        df_file_ucmdb.drop('join', axis=1, inplace=True) #Se borra la columna creada, join, El axis=1 indica que es columna, 0 es fila
        #dataFrame_Full.rename(columns = {ucmdb_ipaddress:'display_label'},inplace = True) #Se renombra columna del hostname en UCMDB para poder usarlo como atributo del DF
        #3 Mira si el serviceCode o hostname esta contenido entre  Inv UCMDB, si es asi, lo marca como true en columna match. 
        

        return -1

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
            status_match="pdt en reporte UCMDB"
            observations_match="No coincide codigo de servicio ni hostname de inv torre en UCMDB"                
        return [status_match,observations_match]

    def ip_isin_string(self,ip,text):
        match=-1
        if(ip+"," in text):
            match=1
        elif(text.endswith(ip)):
            match=1
        return match
    #Retorna array[STATUSMATCH,OBSERVACIONES]   
    def check_match_status_serviceCode_Ip(self,Tower_ipAddress,ucmdb_ipAddress,tower_ServiceCode,ucmdb_ServiceCode):
        matchIp=Tower_ipAddress in ucmdb_ipAddress
        match_serviceCode=tower_ServiceCode in ucmdb_ServiceCode and (tower_ServiceCode!='nan' and ucmdb_ServiceCode!='nan')
        status_match="pdt en reporte UCMDB"
        observations_match="No se logro identificar match"
        if(len(ucmdb_ipAddress)>0 and len(ucmdb_ServiceCode)>0):
            if( matchIp and match_serviceCode ):
                status_match="OK"
                observations_match=""
            elif(not matchIp and match_serviceCode ):
                status_match="pdt en reporte UCMDB"
                observations_match="No coincide Ip de gestión de inv torre en UCMDB"
            elif(matchIp and not match_serviceCode):
                status_match="OK, validar modelado"
                observations_match="No coincide codigo de servicio de inv torre en UCMDB"
            elif(not matchIp and not match_serviceCode):
                status_match="pdt en reporte UCMDB"
                observations_match="No se identifica coincidencia en base al codigo de servicio ni Ip de inv torre en UCMDB"        
        return [status_match,observations_match]
    
    def check_discovery_status(self,df,date_name_Column):
        #limit_Recent_Access_Time = datetime.today()-dt.timedelta(days=RANGE_DAYS_LAST_ACCESS_TIME)
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

#INV TORRE VS INV UCMDB
    def match_invTown_vs_inv_Ucmdb(self):
        towerPathRouterIsValid=libs.csv_is_valid(self.get_tower_inventory_path_router())
        #towerPathSwitchIsValid=libs.csv_is_valid(self.get_tower_inventory_path_switch())
        ucmdbPathIsValid=libs.excel_is_valid(self.get_ucmdb_inventory_path())
        if(towerPathRouterIsValid and  ucmdbPathIsValid):
        #if((towerPathRouterIsValid and towerPathSwitchIsValid) and  ucmdbPathIsValid):
            #1,, leer excel y extraer codigos de servicio de "nombre Host"
            df_file_red_router=pd.read_csv(self.get_tower_inventory_path_router()).fillna(value='')
            df_file_ucmdb=pd.read_excel(self.get_ucmdb_inventory_path()).fillna(value='')
            df_file_ucmdb_copy=df_file_ucmdb.copy()
            df_file_red=df_file_red_router
            df_file_red=libs.extract_service_code(df_file_red,self.tower_col_displayLabel)
            df_file_red[self.tower_col_hostname]=df_file_red[self.tower_col_hostname].str.lower()
            df_file_red['COD_HOSTNAME']=df_file_red[self.tower_col_servCode].str.lower()+"_"+df_file_red[self.tower_col_hostname].str.lower()
            df_file_ucmdb_copy['COD_HOSTNAME']=df_file_ucmdb_copy[self.ucmdb_col_servCode].str.lower()+"_"+df_file_ucmdb[self.ucmdb_col_displayLabel].str.lower()
            df_file_red.drop_duplicates(subset=[self.tower_col_ipAddress],inplace=True)
            df_file_red.insert(0,"#",np.arange(1,len(df_file_red)+1))##SE añade num consecutivos

            #2 left join en base al nombre router
            df_match_town_vs_ucdmdb=df_file_red.merge(df_file_ucmdb,left_on=self.tower_col_hostname
            ,right_on=self.ucmdb_col_displayLabel,how='left')

            df_match_town_vs_ucdmdb[STATUS_MATCH_UCMDB_COL]=df_match_town_vs_ucdmdb.apply(
                lambda row : self.check_match_status_serviceCode_Ip(str(row[self.tower_col_ipAddress]),str(row[self.ucmdb_col_ipAddress]),
                                                                    str(row[self.tower_col_servCode]),str(row[self.ucmdb_col_servCode]))[0] ,axis=1)
            df_match_town_vs_ucdmdb[OBSERVACIONES_MATCH_UCMDB_COL]=df_match_town_vs_ucdmdb.apply(
                lambda row : self.check_match_status_serviceCode_Ip(str(row[self.tower_col_ipAddress]),str(row[self.ucmdb_col_ipAddress]),
                                                                    str(row[self.tower_col_servCode]),str(row[self.ucmdb_col_servCode]))[1] ,axis=1)
            
            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_PdtMatch=df_match_town_vs_ucdmdb[df_match_town_vs_ucdmdb[STATUS_MATCH_UCMDB_COL] !="OK"]
            df_matchIp=self.check_match_status_ipAddress(df_PdtMatch,df_file_ucmdb,self.tower_col_ipAddress,self.ucmdb_col_ipAddress)
            #TODO combinar df:matchip en el df_match_town_vs_ucdmdb, conservando las observaciones de df_matchip
            #6 Une en el Df original los que encontro match x solo ip
            df_match_town_vs_ucdmdb=pd.concat([df_match_town_vs_ucdmdb,df_matchIp],axis=0)
            #sobreescribir en el DF del merge los que encontro x ip (eliminando el duplicado que no encontro algo)
            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_match_town_vs_ucdmdb = df_match_town_vs_ucdmdb.sort_values(by=[STATUS_MATCH_UCMDB_COL,OBSERVACIONES_MATCH_UCMDB_COL],na_position='first')
            df_match_town_vs_ucdmdb=df_match_town_vs_ucdmdb.drop_duplicates(subset=[self.tower_col_ipAddress])
            df_match_town_vs_ucdmdb = df_match_town_vs_ucdmdb.sort_values(by=['#'])
            
            #Validando ahora por check_match_status_serviceCode_Hostname que no hizo mathc IP
            df_PdtMatch=df_match_town_vs_ucdmdb[df_match_town_vs_ucdmdb[STATUS_MATCH_UCMDB_COL] !="OK"]

                        
            
            self.check_discovery_status(df_match_town_vs_ucdmdb,UCMDB_COL_LAST_ACCESS_TIME)
            df_match_town_vs_ucdmdb['Fecha_Reporte']=RUN_DATE
            return df_match_town_vs_ucdmdb

#INV UCMDB VS INV TORRE
    def match_invUcmdb_vs_InvTown(self,df_match_town_vs_ucmdb):
        """Realiza match de UCMDB Vs torre en base al resultado de torre vs UCMDB"""
        #1)Leer archivo Excel
        #2 formar llave unica en inv ucmdb (displayeLabel_Codigo servicio)
        df_file_ucmdb=pd.read_excel(self.ucmdb_inventory_path)
        df_file_ucmdb[COD_HOSTNAME_COL]=df_file_ucmdb[self.ucmdb_col_servCode].str.lower()+"_"+df_file_ucmdb[self.ucmdb_col_displayLabel].str.lower()
        df_match_town_vs_ucmdb[COD_HOSTNAME_COL]=df_match_town_vs_ucmdb[self.ucmdb_col_servCode].str.lower()+"_"+df_match_town_vs_ucmdb[self.ucmdb_col_displayLabel].str.lower()
        df_match_town_vs_ucmdb.sort_values(by=[STATUS_MATCH_UCMDB_COL,OBSERVACIONES_MATCH_UCMDB_COL],na_position='first')
        #3hacer un leftjoin y de inv UCBDM A dfMatchTownVsUcmdb, y traer el atributo de status match+observaciones
        df_match_ucmdb_vs_town=df_file_ucmdb.merge(df_match_town_vs_ucmdb,left_on=COD_HOSTNAME_COL,right_on=COD_HOSTNAME_COL,how='left', suffixes=('', '_remove')) ## le coloca sufixes "remove" para nombrar columnas duplicadas   
        #Elimina columnas duplicadas
        df_match_ucmdb_vs_town.drop([i for i in df_match_ucmdb_vs_town.columns if 'remove' in i],
              axis=1, inplace=True)
        #4)Mirar si se generan duplicados x codigo servicio + hostname, 
            #Si coincide, escoja el que tiene el ultimo acceso actualizAdo
        #5 las celdas que tengan en la columna status match marcadas como pdt en reporte UCMDB , marcarlas como PDT, y las vacias como que no estan en inventario torre
        df_match_ucmdb_vs_town["Status match Torre"]=df_match_ucmdb_vs_town[STATUS_MATCH_UCMDB_COL].apply(            
            lambda match_status : ("No esta en inv torre" if ( str(match_status) =="pdt en reporte UCMDB" or str(match_status) =="nan" ) else str(match_status) )
        )
        #df_match_ucmdb_vs_town.to_excel(OUTPUT_FILE_PATH+"ucmdbVsTorreRed.xlsx")
        #print(f"FIN CRUCE ucmdb vs torre {OUTPUT_FILE_PATH}ucmdbVsTorreRed.xlsx")
        return df_match_ucmdb_vs_town    
#INV UCMDB VS INV TORRE and SM
    def match_invUcmdbAndTown_vs_ServiceManager(self,df_match_ucmdb_vs_town):
        #1)Leer archivo service manager y separar x columnas
        df_service_Manager=pd.read_csv(self.get_sm_inventory_path(),sep=';')
        df_service_Manager[COD_HOSTNAME_COL]=df_service_Manager[self.serviceManager_col_servCode].str.lower()+"_"+df_service_Manager[self.serviceManager_col_displayLabel].str.lower()
        #2 leer dataframe del resultado match_invUcmdb_vs_InvTown (trae lo que esta en ucmdb y es administrado)
        #3 creando uniqueKey codServ_Hostname
        df_match_ucmdb_vs_town[COD_HOSTNAME_COL]=df_match_ucmdb_vs_town[self.ucmdb_col_servCode].str.lower()+"_"+df_match_ucmdb_vs_town[self.ucmdb_col_displayLabel].str.lower()
        #4 Left join de inv UCMDband Town vs Sevicemanager
        df_match_inv_ucmdb_and_town_vs_sm=df_match_ucmdb_vs_town.merge(df_service_Manager,left_on=COD_HOSTNAME_COL,right_on=COD_HOSTNAME_COL,how="left")
        df_match_inv_ucmdb_and_town_vs_sm["Status match SM"]=df_match_inv_ucmdb_and_town_vs_sm[self.serviceManager_col_displayLabel].apply(
            lambda sm_displayLabel:"OK SM" if str(sm_displayLabel)!="nan" else "PDT SM"
        )
        #4)Mirar si se generan duplicados x codigo servicio + hostname, 
        # 5 Formar un df temporal de todos los duplicados x codHostname
        df_duplicate_CodHostname = df_match_inv_ucmdb_and_town_vs_sm[df_match_inv_ucmdb_and_town_vs_sm.duplicated([COD_HOSTNAME_COL], keep=False)]
        df_duplicate_CodHostname = df_duplicate_CodHostname.sort_values(by=[self.serviceManager_col_accessTime,COD_HOSTNAME_COL],ascending=False,na_position='first') #ORDENA alfabeticamente x status match y observaciones
        df_duplicate_CodHostname=df_duplicate_CodHostname.drop_duplicates(subset=[COD_HOSTNAME_COL])
        df_duplicate_CodHostname["Observaciones ServiceManager"]="Duplicados en Servicemanager"
        
        df_match_inv_ucmdb_and_town_vs_sm=pd.concat([df_match_inv_ucmdb_and_town_vs_sm,df_duplicate_CodHostname],axis=0)
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['Observaciones ServiceManager',COD_HOSTNAME_COL],na_position='last') #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
        df_match_inv_ucmdb_and_town_vs_sm=df_match_inv_ucmdb_and_town_vs_sm.drop_duplicates(subset=[COD_HOSTNAME_COL])
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['#'],na_position='last')
        df_match_inv_ucmdb_and_town_vs_sm['Fecha_Reporte']=RUN_DATE

        df_match_inv_ucmdb_and_town_vs_sm.to_excel(OUTPUT_FILE_PATH+"ucmdb&TorrevsSMRed.xlsx")
        print(f"FIN CRUCE ucmdb&torre vs SM {OUTPUT_FILE_PATH}ucmdb&TorrevsSMRed.xlsx")
        return df_match_inv_ucmdb_and_town_vs_sm

    def mainModel(self):
        #TORRE VS UCMDB
        rutaArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"cruceRedvsUcmdb.xlsx"
        df_torreVsUcmdb=self.match_invTown_vs_inv_Ucmdb()
        df_torreVsUcmdb.to_excel(rutaArchivoTownVsUCMDB)
        print(f"FIN CRUCE TORRE VS UCMDB {rutaArchivoTownVsUCMDB}")
        #HISTORICO TORRE VS UCMDB
        rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoRedTorreVsUCmdb.xlsx"
        libs.append_df_to_excel(df_torreVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistRedTorreVsUCmdb")
        print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        


        #UCMDB VS TORRE
        df_match_UcmdbVsTown=self.match_invUcmdb_vs_InvTown(df_torreVsUcmdb)
        rutaArchivoUCMDBvsTown=OUTPUT_FILE_PATH+"ucmdbVsTorreRed.xlsx"
        df_match_UcmdbVsTown.to_excel(rutaArchivoUCMDBvsTown)
        print(f"FIN CRUCE ucmdb vs torre {rutaArchivoUCMDBvsTown}")

        #UCMDB&TORRE VS SERVICE MANAGER
        df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(df_match_UcmdbVsTown)
        rutaArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"CruceUCMDByTorreVsServiceManager.xlsx"
        rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistoricoRedUCMDBvsTorreVsSM.xlsx"
        libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistRedUCMDBvsTorreVsSM")
        print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")
        df_match_invUcmdbAndTown_vs_SM.to_excel(rutaArchivoUCMDByTorreVsServiceManager,index=False);        
        return ["cruce torre vs UCMDB: "+rutaHistoricoArchivoTownVsUCMDB,"Cruce UCMDB vs Torre: "+rutaArchivoUCMDBvsTown,"Cruce UCMDB adm por torre Vs sM: "+rutaHistoricoArchivoUCMDByTorreVsServiceManager]
    
    def testMain(self):
        self.set_tower_inventory_path_switch(r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\13 Red sw Acceso + N5K\Node-Nodes(AllAttributes) (4).csv")
        self.set_tower_inventory_path_router(r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\13 Red sw Acceso + N5K\Node-Nodes(AllAttributes) (4).csv")
        self.set_ucmdb_inventory_path(r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\13 Red sw Acceso + N5K\inv ucmdb.xlsx")
        self.set_sm_inventory_path(r"D:\OneDrive - GLOBAL HITSS\2023\11.Noviembre\Cruces DC\13 Red sw Acceso + N5K\export (20).csv")
        #TORRE VS UCMDB
        df_torreVsUcmdb=self.match_invTown_vs_inv_Ucmdb()
        #UCMDB VS TORRE
        df_match_UcmdbVsTown=self.match_invUcmdb_vs_InvTown(df_torreVsUcmdb)
        #UCMDB&TORRE VS SERVICE MANAGER
        df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(df_match_UcmdbVsTown)
        #HISTORICO TORRE VS UCMDB
        rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoRedTorreVsUCmdb.xlsx"
        libs.append_df_to_excel(df_torreVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistRedTorreVsUCmdb")
        print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        
        #HISTORICO UCMDBvsTorreVsSm
        rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistoricoRedUCMDBvsTorreVsSM.xlsx"
        libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistRedUCMDBvsTorreVsSM")
        print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")

''''
model=Model() 
model.testMain()'''