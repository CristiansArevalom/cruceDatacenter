from datetime import date, datetime
import libs
import pandas as pd
import numpy as np

STATUS_MATCH_UCMDB_COL='Status match en UCMDB'
OBSERVACIONES_MATCH_UCMDB_COL='Observaciones en UCMDB'

TOWER_COL_DISPLAYLABEL='Nombre de host'
TOWER_IP_ADDRESS='Dirección de gestión'
UCMDB_COL_SERVICECODE='[Chassis] : Onyx ServiceCodes'
UCMDB_COL_DISPLAYLABEL='[Chassis] : Display Label'
UCMDB_COL_IPADDRESS='[Chassis] : IpAddress'
UCMDB_COL_LAST_ACCESS_TIME='[Chassis] : Last Access Time'
UCMDB_COL_SNMP_SYSNAME='[Chassis] : SnmpSysName'
SERVICE_MANAGER_COL_DISPLAYLABEL='Nombre para mostrar'
SERVICE_MANAGER_COL_SERVICECODE='Clr Service Code'
SERVICE_MANAGER_COL_SUBTIPO='Subtipo'
SERVICE_MANAGER_COL_ACCESS_TIME='Fecha y hora de modificación del sistema'
OUTPUT_FILE_PATH="D:/OneDrive - GLOBAL HITSS/Automatizmos/ArchivosParaPowerBi/Enclosure/"
RUN_DATE=datetime.now().strftime('%Y/%m/%d %H:%M:%S')
class Model:
    def __init__(self):
        self.tower_inventory_path=""
        self.ucmdb_inventory_path=""
        self.sm_inventory_path=""
        self.tower_col_displayLabel=TOWER_COL_DISPLAYLABEL
        self.tower_col_ipAddres=TOWER_IP_ADDRESS
        self.ucmdb_col_servCode=UCMDB_COL_SERVICECODE
        self.ucmdb_col_displayLabel=UCMDB_COL_DISPLAYLABEL
        self.ucmdb_col_LastAccessTime=UCMDB_COL_LAST_ACCESS_TIME
        self.ucmdb_col_ipAddress=UCMDB_COL_IPADDRESS
        self.ucmdb_col_snmpSysName=UCMDB_COL_SNMP_SYSNAME
        self.serviceManager_col_displayLabel=SERVICE_MANAGER_COL_DISPLAYLABEL
        self.serviceManager_col_servCode=SERVICE_MANAGER_COL_SERVICECODE
        self.serviceManager_col_subtipo=SERVICE_MANAGER_COL_SUBTIPO
        self.serviceManager_col_accessTime=SERVICE_MANAGER_COL_ACCESS_TIME
        self.output_file_path=OUTPUT_FILE_PATH
        self.status_match=""
        self.observations_match=""


    def get_status_match(self):
        return self.status_match
    def set_status_match(self,status_match):
        self.status_match=status_match
    def get_observations_match(self):
        return self.observations_match
    def set_observations_match(self,observations):
        self.observations_match=observations
    def get_tower_inventory_path(self):
        return self.tower_inventory_path
    def set_tower_inventory_path(self,tower_inventory_path):
        self.tower_inventory_path=tower_inventory_path
    def get_ucmdb_inventory_path(self):
        return self.ucmdb_inventory_path
    def set_ucmdb_inventory_path(self,ucmdb_inventory_path):
        self.ucmdb_inventory_path=ucmdb_inventory_path
    def get_sm_inventory_path(self):
        return self.sm_inventory_path
    def set_sm_inventory_path(self,sm_inventory_path):
        self.sm_inventory_path=sm_inventory_path
    def get_tower_col_displayLabel(self):
        return self.tower_col_displayLabel
    def get_tower_col_ipAddres(self):
        return self.tower_col_ipAddres
    def get_ucmdb_col_servCode(self):
        return self.ucmdb_col_servCode
    def get_ucmdb_col_displayLabel(self):
        return self.ucmdb_col_displayLabel
    def get_ucmdb_col_LastAccessTime(self):
        return self.ucmdb_col_LastAccessTime
    def get_ucmdb_col_ipAddress(self):        
        return self.ucmdb_col_ipAddress
    def get_ucmdb_col_snmpSysName(self):
        return self.ucmdb_col_snmpSysName
    def get_serviceManager_col_displayLabel(self):
        return self.serviceManager_col_displayLabel
    def get_serviceManager_col_servCode(self):
        return self.serviceManager_col_servCode
    def get_serviceManager_col_subtipo(self):
        return self.serviceManager_col_subtipo
    def get_serviceManager_col_accessTime(self):
        return self.serviceManager_col_accessTime
    def get_output_file_path(self):
        return self.output_file_path
   
    #Inv torre vs Inv Ucmdb
    def match_invTown_vs_invUcmdb(self):       
        towerPathIsValid=libs.csv_is_valid(self.get_tower_inventory_path())        
        ucmdbPathIsValid=libs.excel_is_valid(self.get_ucmdb_inventory_path())
        if(towerPathIsValid and ucmdbPathIsValid):
            #print("..Iniciando cruce del inventario de la torre contra el de UCMDB..")
            #1,, leer excel y extraer codigos de servicio de hostname
            df_file_enclosure=pd.read_csv(self.get_tower_inventory_path()).fillna(value='')
            df_file_ucmdb_temp=pd.read_excel(self.get_ucmdb_inventory_path())
            df_file_ucmdb=df_file_ucmdb_temp[~df_file_ucmdb_temp[self.get_ucmdb_col_displayLabel()].isnull()].fillna(value='')#Selecciona las celdas en donde displayLabel no es vacio
            print(f"Cantidad Host Inv torre = {len(df_file_enclosure)} | Cantidad registros Inv UCMDB = {len(df_file_ucmdb)}")
            df_file_enclosure=libs.extract_service_code(df_file_enclosure,self.get_tower_col_displayLabel())
            df_file_ucmdb[self.get_ucmdb_col_servCode()]=df_file_ucmdb[self.get_ucmdb_col_servCode()].str.lower() #se paso a minuscula para el regex de extraer codigo servicio
            df_file_enclosure['servCode']=df_file_enclosure['servCode'].str.lower()
            df_file_enclosure.insert(0,"#",np.arange(1,len(df_file_enclosure)+1))##SE añade num consecutivos
            #2 left join en base al nombre del enclosure
            df_match_town_vs_ucdmdb = df_file_enclosure.merge(df_file_ucmdb, left_on=self.get_tower_col_displayLabel(
            ), right_on=self.get_ucmdb_col_displayLabel(), how='left')
            # 3 De los que hagan match, busca si la ip y cod de servicio de inv torre esta en inv ucmdb, si es asi, lo marca como ok
            #When you use df.apply(), each row of your DataFrame will be passed to your lambda function as a pandas Series,The frame's columns will then be the index of the series and you can access values using series[label].
            #df['D'] = (df.apply(lambda x: myfunc(x[colNames[0]], x[colNames[1]]), axis=1)) 

            #df_match_town_vs_ucdmdb["Status match"]=df_match_town_vs_ucdmdb.apply(
            #    lambda row : ("ok" if ( str(row[self.get_tower_col_ipAddres()]) in str(row[self.get_ucmdb_col_ipAddress()])) else "No coincide IP"),axis=1)

            #df_match_town_vs_ucdmdb["Status match IP"]=df_match_town_vs_ucdmdb.apply(
            #    lambda row : ("ok" if ( str(row[self.get_tower_col_ipAddres()]) in str(row[self.get_ucmdb_col_ipAddress()])) else "No coincide IP"),axis=1
            #)            
            df_match_town_vs_ucdmdb["Status match en UCMDB"] = df_match_town_vs_ucdmdb.apply(
                lambda row: self.check_match_status_serviceCode_Ip(str(row[self.get_tower_col_ipAddres()]), str(row[self.get_ucmdb_col_ipAddress()]),
                                                                    str(row['servCode']),str(row[self.get_ucmdb_col_servCode()]))[0] ,axis=1)
            df_match_town_vs_ucdmdb["Observaciones en UCMDB"]=df_match_town_vs_ucdmdb.apply(
                lambda row : self.check_match_status_serviceCode_Ip(str(row[self.get_tower_col_ipAddres()]),str(row[self.get_ucmdb_col_ipAddress()]),
                                                                    str(row['servCode']),str(row[self.get_ucmdb_col_servCode()]))[1] ,axis=1)

            #5 Validar los que no hicieron match x codigo de servicio, marcados como PDT (diferente a OK). 
            df_PdtMatch=df_match_town_vs_ucdmdb[df_match_town_vs_ucdmdb[STATUS_MATCH_UCMDB_COL] !="OK"]
            #TODO combinar df:matchip en el df_match_town_vs_ucdmdb, conservando las observaciones de df_matchip
            df_matchIp=self.check_match_status_ipAddress(df_PdtMatch,df_file_ucmdb,self.get_tower_col_ipAddres(),self.get_ucmdb_col_ipAddress())
            #6 Une en el Df original los que encontro match x solo ip
            df_match_town_vs_ucdmdb=pd.concat([df_match_town_vs_ucdmdb,df_matchIp],axis=0)
            #sobreescribir en el DF del merge los que encontro x hostname (eliminando el duplicado que no encontro algo)
            #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
            df_match_town_vs_ucdmdb = df_match_town_vs_ucdmdb.sort_values(by=[STATUS_MATCH_UCMDB_COL,'Observaciones en UCMDB'],na_position='first')
            df_match_town_vs_ucdmdb=df_match_town_vs_ucdmdb.drop_duplicates(subset=[self.get_tower_col_displayLabel()])
            df_match_town_vs_ucdmdb = df_match_town_vs_ucdmdb.sort_values(by=['#'])
            return df_match_town_vs_ucdmdb
    #Retorna array[STATUSMATCH,OBSERVACIONES]
    def check_match_status_serviceCode_Ip(self,Tower_ipAddress,ucmdb_ipAddress,tower_ServiceCode,ucmdb_ServiceCode):
        matchIp=Tower_ipAddress in ucmdb_ipAddress
        match_serviceCode=tower_ServiceCode in ucmdb_ServiceCode
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
                status_match="pdt en reporte UCMDB"
                observations_match="No coincide codigo de servicio de inv torre en UCMDB"
            elif(not matchIp and not match_serviceCode):
                status_match="pdt en reporte UCMDB"
                observations_match="No coincide codigo de servicio ni Ip de inv torre en UCMDB"        
        return [status_match,observations_match]
    
    #Inv UCMDB vs INV TORRE
    def match_invUmcbd_vs_invTown(self,df_match_town_vs_ucdmdb):
        #1)Leer archivo Excel
        ucmdbPathIsValid=libs.excel_is_valid(self.get_ucmdb_inventory_path())

        if(ucmdbPathIsValid):
            #3 formar llave unica en inv ucmdb (displayeLabel_Codigo servicio)
            df_file_ucmdb_temp=pd.read_excel(self.get_ucmdb_inventory_path())
            df_file_ucmdb=df_file_ucmdb_temp[~df_file_ucmdb_temp[self.get_ucmdb_col_displayLabel()].isnull()].fillna(value='')#Selecciona las celdas en donde displayLabel no es vacio
            df_file_ucmdb['COD_HOSTNAME']=df_file_ucmdb[self.get_ucmdb_col_servCode()].str.lower()+"_"+df_file_ucmdb[self.get_ucmdb_col_displayLabel()].str.lower()
            df_match_town_vs_ucdmdb['COD_HOSTNAME']=df_match_town_vs_ucdmdb[self.get_ucmdb_col_servCode()].str.lower()+"_"+df_match_town_vs_ucdmdb[self.get_ucmdb_col_displayLabel()].str.lower()
            #3hacer un leftjoin y de inv UCBDM A dfMatchTownVsUcmdb, y traer el atributo de status match+observaciones
            df_match_ucmdb_vs_town=df_file_ucmdb.merge(df_match_town_vs_ucdmdb,left_on="COD_HOSTNAME",right_on="COD_HOSTNAME",how='left', suffixes=('', '_remove')) ## le coloca sufixes "remove" para nombrar columnas duplicadas)
            #Elimina columnas duplicadas
            df_match_ucmdb_vs_town.drop([i for i in df_match_ucmdb_vs_town.columns if 'remove' in i],
               axis=1, inplace=True)            
            #1)Cruce inv torre vs ucmdb
            #4)Mirar si se generan duplicados x codigo servicio + hostname, 
            #Si coincide, escoja el que tiene el ultimo acceso actualizAdo
            #5 las celdas que tengan en la columna status match marcadas como pdt en reporte UCMDB , marcarlas como PDT, y las vacias como que no estan en inventario torre
        df_match_ucmdb_vs_town["Status match Torre"]=df_match_ucmdb_vs_town["Status match en UCMDB"].apply(            
            lambda match_status : ("No esta en inv torre" if ( str(match_status) =="pdt en reporte UCMDB" or str(match_status) =="nan" ) else str(match_status) )
        )
        return df_match_ucmdb_vs_town
    #inv UCMDB&torre vs Inv SM
    def match_invUcmdbAndTown_vs_ServiceManager(self,df_match_ucmdb_vs_town):
        #1)Leer archivo service manager y separar x columnas
        df_service_Manager_temp=pd.read_csv(self.get_sm_inventory_path(),sep=';')
        df_service_Manager_temp['COD_HOSTNAME']=df_service_Manager_temp[self.get_serviceManager_col_servCode()].str.lower()+"_"+df_service_Manager_temp[self.get_serviceManager_col_displayLabel()].str.lower()
        df_service_Manager=df_service_Manager_temp[df_service_Manager_temp[self.get_serviceManager_col_subtipo()]=="Enclosure"]
        
        #2 leer dataframe del resultado match_invUcmdb_vs_InvTown (trae lo que esta en ucmdb y es administrado)
        #3 creando uniqueKey codServ_Hostname
        df_match_ucmdb_vs_town['COD_HOSTNAME']=df_match_ucmdb_vs_town[self.get_ucmdb_col_servCode()].str.lower()+"_"+df_match_ucmdb_vs_town[self.get_ucmdb_col_snmpSysName()].str.lower()
        #4 Left join de inv UCMDband Town vs Sevicemanager
        df_match_inv_ucmdb_and_town_vs_sm=df_match_ucmdb_vs_town.merge(df_service_Manager,left_on="COD_HOSTNAME",right_on="COD_HOSTNAME",how="left")
        df_match_inv_ucmdb_and_town_vs_sm["Status match SM"]=df_match_inv_ucmdb_and_town_vs_sm[self.get_serviceManager_col_displayLabel()].apply(
            lambda sm_displayLabel:"OK SM" if str(sm_displayLabel)!="nan" else "PDT SM"
        )
        #4)Mirar si se generan duplicados x codigo servicio + hostname, 
        # 5 Formar un df temporal de todos los duplicados x codHostname
        df_duplicate_CodHostname = df_match_inv_ucmdb_and_town_vs_sm[df_match_inv_ucmdb_and_town_vs_sm.duplicated(["COD_HOSTNAME"], keep=False)]
        df_duplicate_CodHostname = df_duplicate_CodHostname.sort_values(by=[self.get_serviceManager_col_accessTime(),'COD_HOSTNAME'],ascending=False,na_position='first') #ORDENA alfabeticamente x status match y observaciones
        df_duplicate_CodHostname=df_duplicate_CodHostname.drop_duplicates(subset=['COD_HOSTNAME'])
        df_duplicate_CodHostname["Observaciones ServiceManager"]="Duplicados en Servicemanager"
        
        df_match_inv_ucmdb_and_town_vs_sm=pd.concat([df_match_inv_ucmdb_and_town_vs_sm,df_duplicate_CodHostname],axis=0)
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['Observaciones ServiceManager','COD_HOSTNAME'],na_position='last') #ORDENA alfabeticamente x status match y observaciones las que esten vacias primero
        df_match_inv_ucmdb_and_town_vs_sm=df_match_inv_ucmdb_and_town_vs_sm.drop_duplicates(subset=['COD_HOSTNAME'])
        df_match_inv_ucmdb_and_town_vs_sm = df_match_inv_ucmdb_and_town_vs_sm.sort_values(by=['#'],na_position='last')

        return df_match_inv_ucmdb_and_town_vs_sm

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
    
    def ip_isin_string(self,ip,text):
        ips=ip.split(",")
        match=-1
        for ip in ips:
            if(ip+"," in text):
                match=1                
            elif(text.endswith(ip)):
                match=1 
        return match  

    def check_match_status_serviceCode_Hostname(self,tower_col_hostname,tower_col_servCode,ucmdb_col_displayLabel,ucmdb_col_servCode):
        status_match="pdt en reporte UCMDB"
        observations_match="No se logro identificar match"
        matchHostname=tower_col_hostname in ucmdb_col_displayLabel
        match_serviceCode=tower_col_servCode in ucmdb_col_servCode and (tower_col_servCode!='nan' and ucmdb_col_servCode!='nan')
        if(matchHostname and match_serviceCode):
            status_match="OK"
            observations_match=""
        elif(not matchHostname and match_serviceCode):
            status_match="OK, Validar"
            observations_match="No coincide hostname de inv torre en UCMDB"
        elif(matchHostname and not match_serviceCode):
            status_match="pdt en reporte UCMDB"
            observations_match="No coincide codigo de servicio de inv torre en UCMDB"
        elif(not matchHostname and not match_serviceCode):
            status_match="OK, Validar"
            observations_match="No coincide codigo de servicio ni hostname de inv torre en UCMDB"                
        return status_match,observations_match
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
    def mainTest(self):
            self.set_tower_inventory_path(r"D:\OneDrive - GLOBAL HITSS\2023\7 Julio\Auditorias DC\4 Enclosure\Node-Nodes(AllAttributes).csv")
            self.set_ucmdb_inventory_path(r"D:\OneDrive - GLOBAL HITSS\2023\7 Julio\Auditorias DC\4 Enclosure\inv ucmdb.xlsx")
            self.set_sm_inventory_path(r"D:\OneDrive - GLOBAL HITSS\2023\7 Julio\Auditorias DC\4 Enclosure\export (4).csv")
            #tower_inventory_path=r"D:\ECM3200I\Desktop\BasesAutomatismo\Enclosure\Inv Enclosure Torre.csv"
            #ucmdb_inventory_path=r"D:\ECM3200I\Desktop\BasesAutomatismo\Enclosure\inv enclosure UCMDB.xlsx"
            #serviceManager_path=r"D:\ECM3200I\Desktop\BasesAutomatismo\inv virtualizacion service manager.csv"
            #TORRE VS UCMDB
            dfMatchTownVsUcmdb=self.match_invTown_vs_invUcmdb()
            dfMatchTownVsUcmdb=self.check_discovery_status(dfMatchTownVsUcmdb,self.get_ucmdb_col_LastAccessTime())
            rutaArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"CruceEnclosure.xlsx"
            rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoEnclosureTorreVsUCmdb.xlsx"
            dfMatchTownVsUcmdb['Fecha_Reporte']=RUN_DATE
            dfMatchTownVsUcmdb.to_excel(rutaArchivoTownVsUCMDB,index=False)
            libs.append_df_to_excel(dfMatchTownVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistEnclosureTorreVsUCmdb")
            print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        

            #UCMDB VS TORRE
            df_match_UcmdbVsTown=self.match_invUmcbd_vs_invTown(dfMatchTownVsUcmdb)
            rutaArchivoUCMDBvsTown=OUTPUT_FILE_PATH+"CruceUcmdbVsEnclosure.xlsx"
            df_match_UcmdbVsTown.to_excel(rutaArchivoUCMDBvsTown,sheet_name="CruceUcmdbVsEnclosure",index=False)
            #print(f"El archivo con el cruce UCMDB VS TORRE fue generado en la ruta {rutaArchivoUCMDBvsTown}")        

            #UCMDB&TORRE VS SERVICE MANAGER
            df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(df_match_UcmdbVsTown)
            df_match_invUcmdbAndTown_vs_SM['Fecha_Reporte']=RUN_DATE
            rutaArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"CruceUCMDByTorreVsServiceManager.xlsx"
            rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistoricoEnclosureUCMDBvsTorreVsSM.xlsx"
            df_match_invUcmdbAndTown_vs_SM.to_excel(rutaArchivoUCMDByTorreVsServiceManager,sheet_name="HistEnclosureUCMDBvsTorreVsSM",index=False)
            libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistEnclosureUCMDBvsTorreVsSM")

            print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")

            return ["cruce torre vs UCMDB: "+rutaArchivoTownVsUCMDB,"Cruce UCMDB vs Torre: "+rutaArchivoUCMDBvsTown,"Cruce UCMDB adm por torre Vs sM: "+rutaArchivoUCMDByTorreVsServiceManager]

    def mainModel(self):
            #TORRE VS UCMDB
            dfMatchTownVsUcmdb=self.match_invTown_vs_invUcmdb()
            dfMatchTownVsUcmdb=self.check_discovery_status(dfMatchTownVsUcmdb,self.get_ucmdb_col_LastAccessTime())
            rutaArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"CruceEnclosure.xlsx"
            rutaHistoricoArchivoTownVsUCMDB=OUTPUT_FILE_PATH+"HistoricoEnclosureTorreVsUCmdb.xlsx"
            dfMatchTownVsUcmdb['Fecha_Reporte']=RUN_DATE
            dfMatchTownVsUcmdb.to_excel(rutaArchivoTownVsUCMDB,index=False)
            libs.append_df_to_excel(dfMatchTownVsUcmdb,rutaHistoricoArchivoTownVsUCMDB,"HistEnclosureTorreVsUCmdb")
            print(f"El archivo con el historico TORRE VS UCMDB fue generado en la ruta {rutaHistoricoArchivoTownVsUCMDB}")        

            #UCMDB VS TORRE
            df_match_UcmdbVsTown=self.match_invUmcbd_vs_invTown(dfMatchTownVsUcmdb)
            rutaArchivoUCMDBvsTown=OUTPUT_FILE_PATH+"CruceUcmdbVsEnclosure.xlsx"
            df_match_UcmdbVsTown.to_excel(rutaArchivoUCMDBvsTown,sheet_name="CruceUcmdbVsEnclosure",index=False)
            #print(f"El archivo con el cruce UCMDB VS TORRE fue generado en la ruta {rutaArchivoUCMDBvsTown}")        

            #UCMDB&TORRE VS SERVICE MANAGER
            df_match_invUcmdbAndTown_vs_SM=self.match_invUcmdbAndTown_vs_ServiceManager(df_match_UcmdbVsTown)
            df_match_invUcmdbAndTown_vs_SM['Fecha_Reporte']=RUN_DATE
            rutaArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"CruceUCMDByTorreVsServiceManager.xlsx"
            rutaHistoricoArchivoUCMDByTorreVsServiceManager=OUTPUT_FILE_PATH+"HistoricoEnclosureUCMDBvsTorreVsSM.xlsx"
            df_match_invUcmdbAndTown_vs_SM.to_excel(rutaArchivoUCMDByTorreVsServiceManager,sheet_name="HistEnclosureUCMDBvsTorreVsSM",index=False)
            libs.append_df_to_excel(df_match_invUcmdbAndTown_vs_SM,rutaHistoricoArchivoUCMDByTorreVsServiceManager,"HistEnclosureUCMDBvsTorreVsSM")

            print(f"El archivo con el Historico UCMDB&TORRE VS SERVICE MANAGER fue generado en la ruta {rutaHistoricoArchivoUCMDByTorreVsServiceManager}")
            return ["cruce torre vs UCMDB: "+rutaArchivoTownVsUCMDB,"Cruce UCMDB vs Torre: "+rutaArchivoUCMDBvsTown,"Cruce UCMDB adm por torre Vs sM: "+rutaArchivoUCMDByTorreVsServiceManager]

            #dfMatchTownVsUcmdb=self.check_discovery_status(dfMatchTownVsUcmdb,ucmdb_col_LastAccessTime)
            #rutaArchivoTownVsUCMDB=output_file_path+"CruceVirtualizacionDC2.xlsx"
            #dfMatchTownVsUcmdb.to_excel(rutaArchivoTownVsUCMDB,index=False)
            #print(f"El archivo con el cruce TORRE VS UCMDB fue generado en la ruta {rutaArchivoTownVsUCMDB}")


#model= Model()
#model.mainTest()
    