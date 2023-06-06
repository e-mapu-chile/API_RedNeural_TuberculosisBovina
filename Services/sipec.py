# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 10:29:47 2022

@author: pedro
"""

import pandas as pd
import pandas_oracle.tools as pt
from localStoragePy import localStoragePy
from datetime import datetime
import utm


localStorage = localStoragePy('apiANNRiesgoMovimiento', 'json')


def test(a,b):
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")

    ## passing the conn object to the query_to_df
    df = pt.query_to_df("select * from ms_region", conn, 10000)
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        print(row[1],row[2],row[3])
    
    localStorage.setItem('listaNivelContagio', df)
    
    df2 = localStorage.getItem('listaNivelContagio')
    
    print(df)
    print(df2)
    pt.close_connection(conn)
        
def localT():
    df42 = localStorage.getItem('configuracionSimulador')
    
  #  nivelContagioDto.rup = '134114'
    
   # print(nivelContagioDto.rup)
    
    
    print(df42)
    

def obtenerPredios(grupoEspecie):
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select  DISTINCT  "
    sql += " a.id, a.rup, a.ID_REGI, tipo.id as idTEstab, tipo.descripcion , "
    sql += " CE.Id_Rubro, CE.DESC_RUBRO, a.ID_OFSE ,a.COORDENADA_X	,a.COORDENADA_Y	,a.HUSO"
    sql += " FROM "
    sql += " SIPEC.SIP_D_ESTABLECIMIENTOS a "
    sql += " INNER JOIN SIPEC.sip_d_establecimientos_ties tie "
    sql += " ON a.id = tie.id_esta "
    sql += " INNER JOIN  SIPEC.sip_d_tipos_establecimientos tipo "
    sql += " ON tie.id_ties = tipo.id "
    sql += " INNER JOIN  SIPEC.SIP_D_ENCARGADOS_REGIONALES ER "
    sql += " ON a.id_regi = ER.idregion "
    sql += " INNER JOIN SIPEC.SIP_D_ENCARGADOS_SECTORIALES ES "
    sql += " ON a.ID_OFSE = ES.IDSECTORIAL "
    sql += " INNER JOIN MS_REGION_VW R "
    sql += " ON a.id_regi = R.IDREGION "
    sql += " INNER JOIN MS_SECTORIAL_VW OS "
    sql += " ON a.ID_OFSE = OS.IDSECTORIAL "
    sql += " LEFT JOIN SIPEC.SIP_D_CLAS_ASIG_ESTAB CE "
    sql += " ON(a.ID = CE.ID_ESTA AND CE.vigente = 1) "
    sql += " LEFT JOIN SIPEC.sip_d_especies E "
    sql += " ON E.ID = CE.ID_ESPE "
    sql += " left JOIN SIPEC.sip_d_grupos_especies GE "
    sql += "  ON GE.ID = E.ID_GRES "
    sql += "where "
    sql += " GE.ID = "+grupoEspecie+" "
    sql += " and tie.fecha_termino is null "
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11]])
        
          
  #  pt.close_connection(conn)
    return list
    
    
def aplicarNivelContagio(regionId,sectorId,rup,predioLeche,predioCarne,
                         mataderoCarne,controlAnimalVivo,centroRodeo,predioCrianza,predioCompra,
                         feriaCompra):
    
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select  DISTINCT  "
    sql += " a.id, a.rup, a.ID_REGI, tipo.id as idTEstab, tipo.descripcion , "
    sql += " CE.Id_Rubro, CE.DESC_RUBRO, a.ID_OFSE ,a.COORDENADA_X	,a.COORDENADA_Y	,a.HUSO"
    sql += " FROM "
    sql += " SIPEC.SIP_D_ESTABLECIMIENTOS a "
    sql += " INNER JOIN SIPEC.sip_d_establecimientos_ties tie "
    sql += " ON a.id = tie.id_esta "
    sql += " INNER JOIN  SIPEC.sip_d_tipos_establecimientos tipo "
    sql += " ON tie.id_ties = tipo.id "
    sql += " INNER JOIN  SIPEC.SIP_D_ENCARGADOS_REGIONALES ER "
    sql += " ON a.id_regi = ER.idregion "
    sql += " INNER JOIN SIPEC.SIP_D_ENCARGADOS_SECTORIALES ES "
    sql += " ON a.ID_OFSE = ES.IDSECTORIAL "
    sql += " INNER JOIN MS_REGION_VW R "
    sql += " ON a.id_regi = R.IDREGION "
    sql += " INNER JOIN MS_SECTORIAL_VW OS "
    sql += " ON a.ID_OFSE = OS.IDSECTORIAL "
    sql += " LEFT JOIN SIPEC.SIP_D_CLAS_ASIG_ESTAB CE "
    sql += " ON(a.ID = CE.ID_ESTA AND CE.vigente = 1) "
    sql += " LEFT JOIN SIPEC.sip_d_especies E "
    sql += " ON E.ID = CE.ID_ESPE "
    sql += " left JOIN SIPEC.sip_d_grupos_especies GE "
    sql += "  ON GE.ID = E.ID_GRES "
    sql += "where "
    sql += " GE.ID = 1 "
    sql += " and tie.fecha_termino is null "
    
    if regionId != '0':
        sql += "and a.ID_REGI = "+regionId+" "
    if sectorId != '0':
        sql += " and a.ID_OFSE = "+sectorId+" "
    if rup != "":
        sql += "and a.rup = '"+rup+"' "
           
    
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        te = row[4]
        rubro = row[6]
            
        peso = 0
        cod_estab = str(te)+"_"+str(rubro)
        
        
        if cod_estab == "149_2":
            peso = int(mataderoCarne)
        if cod_estab == "168_39":
            if(int(controlAnimalVivo) > peso):
                peso = int(controlAnimalVivo)
        if cod_estab == "150_13":
            if(int(centroRodeo) > peso):
                peso = int(centroRodeo)
        if cod_estab == "1_19":
            if(int(predioCrianza) > peso):
                 peso = int(predioCrianza)
        if cod_estab == "1_12":
            if(int(predioCompra) > peso):
                peso = int(predioCompra)
        if cod_estab == "148_12":#feria
            if(int(feriaCompra) > peso):
                peso = int(feriaCompra)
        if cod_estab == "1_1":
            if(int(predioLeche) > peso):
                 peso = int(predioLeche)
        if cod_estab == "1_2":
            if(int(predioCarne) > peso):
                  peso = int(predioCarne)
        
        list.append([row[1],row[2],peso,row[8],row[9],row[10],row[11],cod_estab])
        
          
  #  pt.close_connection(conn)
    return list

def aplicarPesoEntrada(regionId,sectorId,rup,fechaDesde,cantidadDias):#01/01/2022
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select F.RUP_DES, SUM(CANT_ANIMALES) as TOT_CANT_ANIMALES, COUNT(*) AS CANT_FMA "
    sql += " from(  "
    sql += " select EO.RUP AS RUP_DES, "
    sql += " ( "
    sql += " select COUNT(*) "
    sql += " from SIPEC.SIP_D_DETALLES_MOV_DIIO DFM "
    sql += " where DFM.ID_FOMO = FMA.ID "
    sql += "          ) as CANT_ANIMALES  "
    sql += "         from SIPEC.SIP_D_FORMULARIOS_MOV_DIIO FMA "
    sql += "            inner join SIPEC.SIP_D_ESTABLECIMIENTOS EO on FMA.ID_ESTA_DES = EO.ID "
    sql += "         where FMA.ID_ESFM IN(1,5)  "
    
    if regionId != '0':
        sql += "         and FMA.ID_REGI_DES = "+regionId+" " 
    if sectorId != '0':
        sql += "         and FMA.ID_OFSE_DES = "+sectorId+" "
    if rup != "":
         sql += "         and EO.RUP = '"+rup+"' "
    sql += "        and FMA.FECHA_FORMULARIO between(sysdate - "+cantidadDias+") and TO_DATE('"+fechaDesde+"','dd/mm/yyyy') "
    sql += "        ) F group by F.RUP_DES "
    
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3]] )
        
    pt.close_connection(conn)
    return list    
  
def aplicarPesoSalida(regionId,sectorId,rup,fechaDesde,cantidadDias):#01/01/2022
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select F.RUP_ORI, SUM(CANT_ANIMALES) as TOT_CANT_ANIMALES, COUNT(*) AS CANT_FMA "
    sql += " from(  "
    sql += " select EO.RUP AS RUP_ORI, "
    sql += " ( "
    sql += " select COUNT(*) "
    sql += " from SIPEC.SIP_D_DETALLES_MOV_DIIO DFM "
    sql += " where DFM.ID_FOMO = FMA.ID "
    sql += "          ) as CANT_ANIMALES  "
    sql += "         from SIPEC.SIP_D_FORMULARIOS_MOV_DIIO FMA "
    sql += "            inner join SIPEC.SIP_D_ESTABLECIMIENTOS EO on FMA.ID_ESTA_ORI = EO.ID "
    sql += "         where FMA.ID_ESFM IN(1,5)  "
    
    if regionId != '0':
        sql += "         and FMA.ID_REGI_ORI = "+regionId+" " 
    if sectorId != '0':
        sql += "         and FMA.ID_OFSE_ORI = "+sectorId+" "
    if rup != "":
         sql += "         and EO.RUP = '"+rup+"' "
    sql += "        and FMA.FECHA_FORMULARIO between(sysdate - "+cantidadDias+") and TO_DATE('"+fechaDesde+"','dd/mm/yyyy') "
    sql += "        ) F group by F.RUP_ORI "
    
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3]] )
        
    pt.close_connection(conn)
    return list    
    

def escalaMasaAnimalesF(cantidad,valorAlta,valorMediaAlta,valorMedia,valorBaja):
    if int(cantidad) >= int(valorAlta):
        return 10
    if int(cantidad) >= int(valorMediaAlta) and int(cantidad) < int(valorAlta):
        return 7
    if int(cantidad) >= int(valorMedia) and int(cantidad) < int(valorMediaAlta):
        return 3
    if int(cantidad) <= int(valorBaja):
        return 1
    return 0

#para Brucelosis se ve frecuencia de movimiento (cantidad fma)
def pesoInOutMovimiento(listaNivelContagio,listaPesoEntrada,listaPesoSalida,valorAlta,valorMediaAlta
                        ,valorMedia,valorBaja):
    list = []
   
    for x in listaNivelContagio:
        
        cantidadEntradaAnimales = 0
        for inn in listaPesoEntrada:
            if(inn[0] == x[1]):
                cantidadEntradaAnimales = inn[2]
        
 
        cantidadSalidaAnimales = 0
        for outt in listaPesoSalida:
            if(outt[0] == x[1]):
                cantidadSalidaAnimales = outt[2]
        
  
            
        
        pesoIn = escalaMasaAnimalesF(cantidadEntradaAnimales,valorAlta,valorMediaAlta,
                                     valorMedia,valorBaja)
        pesoOut = escalaMasaAnimalesF(cantidadSalidaAnimales,valorAlta,valorMediaAlta,
                                     valorMedia,valorBaja)
        
        list.append([x[0],x[1],x[2],pesoIn,pesoOut,x[3],x[4],x[5],x[6],x[7]])
    
    return list
        
        

def ObtenerMovimientosIn(region,fechaDesde):
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select TO_DATE(fma.fecha_formulario,'dd/mm/yyyy'),EO.RUP "
    sql += "  ,EO.coordenada_x,EO.coordenada_y,EO.Huso "
    sql += " ,ED.RUP "
    sql += "  ,ED.coordenada_x,ED.coordenada_y,ED.Huso"
    sql += " from SIPEC.SIP_D_FORMULARIOS_MOV_DIIO FMA "
    sql += " inner join SIPEC.SIP_D_ESTABLECIMIENTOS EO on FMA.ID_ESTA_ORI = EO.ID "
    sql += " inner join SIPEC.SIP_D_ESTABLECIMIENTOS ED on FMA.ID_ESTA_DES = ED.ID "
    sql += " where FMA.ID_ESFM IN(1,5)  "
    sql += " and FMA.ID_ESPE = 1 "
    sql += " and FMA.ID_REGI_DES = "+region+" "
    #sql += " and FMA.ID_REGI_ORI = "+region+" "
    sql += " and FMA.FECHA_FORMULARIO between TO_DATE('"+fechaDesde+"','dd/mm/yyyy') and (sysdate)  "
    sql += " order by FMA.FECHA_FORMULARIO asc "
            
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        fecha = str(row[1])[2:10]
        
        #print('20'+fecha)
        latiOri = 0
        longOri = 0
        try:
            if(row[3] != None and row[4] != None and row[5] != None):
                if(int(row[3]) > 0 and int(row[4]) > 0  and int(row[5]) > 0):
                    if (int(row[3]) < 999999 and int(row[4]) < 9999999):
                        coor = utm.to_latlon(int(row[3]),int(row[4]),int(row[5]),'H')
                        latiOri = coor[0]
                        longOri = coor[1]
        except:
            print("error pero siga")
        
        latiDes = 0
        longDes = 0
        try:
            if(row[7] != None and row[8] != None and row[9] != None):
                if(int(row[7]) > 0 and int(row[8]) > 0  and int(row[9]) > 0):
                    if (int(row[7]) < 999999 and int(row[8]) < 9999999):
                        coor = utm.to_latlon(int(row[7]),int(row[8]),int(row[9]),'H')
                        latiDes = coor[0]
                        longDes = coor[1]
        except:
            print("error pero siga2")
                      
        list.append(['20'+fecha,row[2],latiOri,longOri,row[6],latiDes,longDes])
        
    pt.close_connection(conn)
    return list  
    


def ObtenerMovimientosOut(region,fechaDesde):
    list = []
    ## opening conn
    conn = pt.open_connection("""Services/config.yml""")
    sql = "select TO_DATE(fma.fecha_formulario,'dd/mm/yyyy'),EO.RUP "
    sql += "  ,EO.coordenada_x,EO.coordenada_y,EO.Huso "
    sql += " ,ED.RUP "
    sql += "  ,ED.coordenada_x,ED.coordenada_y,ED.Huso"
    sql += " from SIPEC.SIP_D_FORMULARIOS_MOV_DIIO FMA "
    sql += " inner join SIPEC.SIP_D_ESTABLECIMIENTOS EO on FMA.ID_ESTA_ORI = EO.ID "
    sql += " inner join SIPEC.SIP_D_ESTABLECIMIENTOS ED on FMA.ID_ESTA_DES = ED.ID "
    sql += " where FMA.ID_ESFM IN(1,5)  "
    sql += " and FMA.ID_ESPE = 1 "
    #sql += " and FMA.ID_REGI_DES = "+region+" "
    sql += " and FMA.ID_REGI_ORI = "+region+" "
    sql += " and FMA.FECHA_FORMULARIO between TO_DATE('"+fechaDesde+"','dd/mm/yyyy') and (sysdate)  "
    sql += " order by FMA.FECHA_FORMULARIO asc "
            
    df = pt.query_to_df(sql, conn, 10000)
    
    
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        fecha = str(row[1])[2:10]
        
        #print('20'+fecha)
        latiOri = 0
        longOri = 0
        try:
            if(row[3] != None and row[4] != None and row[5] != None):
                if(int(row[3]) > 0 and int(row[4]) > 0  and int(row[5]) > 0):
                    if (int(row[3]) < 999999 and int(row[4]) < 9999999):
                        coor = utm.to_latlon(int(row[3]),int(row[4]),int(row[5]),'H')
                        latiOri = coor[0]
                        longOri = coor[1]
        except:
            print("error pero siga")
        
        latiDes = 0
        longDes = 0
        try:
            if(row[7] != None and row[8] != None and row[9] != None):
                if(int(row[7]) > 0 and int(row[8]) > 0  and int(row[9]) > 0):
                    if (int(row[7]) < 999999 and int(row[8]) < 9999999):
                        coor = utm.to_latlon(int(row[7]),int(row[8]),int(row[9]),'H')
                        latiDes = coor[0]
                        longDes = coor[1]
        except:
            print("error pero siga2")
                      
        list.append(['20'+fecha,row[2],latiOri,longOri,row[6],latiDes,longDes])
        
    pt.close_connection(conn)
    return list  
              
       
      


    









