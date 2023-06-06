# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 11:36:06 2022

@author: pedro
"""

import pyodbc
import pandas as pd

server = '192.168.1.237' 
database = 'sanidadanimal' 
username = 'usr.sanidad_lee' 
password = 'sanidad.lee_2022' 

def testssa():
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # select 26 rows from SQL table to insert in dataframe.
    query = "SELECT top 10 * from protocolo"
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        print(row[1],row[2],row[3])
    
    print(df.head())
    

def resultadosProtocolosRegionOrigen(enfermedad,especie,region,diasAtras,fechaDesde):
    list = []
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # select 26 rows from SQL table to insert in dataframe.
    query = "select case bm.RupOrigen when '0' then bm.rupTomamuestra else bm.rupOrigen end [rup] ,"
    query +=" bm.SectorIdOrigen,bm.ProtocoloId, "
    query +=" (select count(id) from bitacoramuestraanalisis where protocoloid = bm.ProtocoloId "
    query +=" and ContieneEnfermedad = 0) as [negativos], "
    query +=" (select count(id) from bitacoramuestraanalisis ba, PesosEnfermedadTecnica t where ba.protocoloid = bm.ProtocoloId "
    query +=" and ba.ContieneEnfermedad = 1 and ba.enfermedadId = bm.EnfermedadId and "
    query +=" t.tecnicaId = bm.TecnicaId and t.Peso = 0.5) as [positivos_05] , "
    query +=" (select count(id) from bitacoramuestraanalisis ba, PesosEnfermedadTecnica t where ba.protocoloid = bm.ProtocoloId "
    query +=" and ba.ContieneEnfermedad = 1 and ba.enfermedadId = bm.EnfermedadId and "
    query +=" t.tecnicaId = bm.TecnicaId and t.Peso = 1.0) as [positivos_1] "
    query +=" from bitacoramuestraanalisis bm "
    query +=" where bm.muestracerrada = 1 "
    query +=" and bm.EspecieId = "+especie+"  and bm.EnfermedadId = "+enfermedad+" "
    query +=" and bm.FechaCreacion between(select dateadd(day, - "+diasAtras+", getdate())) and convert(datetime, '"+fechaDesde+"', 103) "
    query +=" and bm.RegionIdOrigen = "+region+" "
    query +=" group by bm.RupTomaMuestra, bm.RupOrigen,bm.SectorIdOrigen,bm.ProtocoloId,bm.EnfermedadId,bm.TecnicaId "
    query +=" order by bm.ProtocoloId desc "
    
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3],row[4],row[5],row[6]])
    
    return list
    
def resultadoPruebaCampoBB(enfermedad,especie,diasAtras,fechaDesde):
    list = []
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # select 26 rows from SQL table to insert in dataframe.
    query = " select P.RupOrigen, P.protocoloId, "
    query +="(select count(protocoloID) "
    query +=" from BitacoraResultadoPruebaCampo "
    query +=" where  contieneEnfermedad = 1 "
    query +=" and protocoloId = P.ProtocoloId)  [contiene], "
    query +=" (select count(protocoloID) "
    query +=" from BitacoraResultadoPruebaCampo "
    query +=" where  contieneEnfermedad = 0 "
    query +=" and protocoloId = P.ProtocoloId)  [noContiene] "
    query +=" from BitacoraResultadoPruebaCampo  P "
    query +=" where "
    query +=" P.EspecieId = "+especie+" and P.EnfermedadId = "+enfermedad+" "
    query +=" and convert(datetime, FechaRegistro, 103) between(select dateadd(day, - "+diasAtras+" , getdate())) and convert(datetime, '"+fechaDesde+"', 103) "
    query +=" group by P.RupOrigen, P.protocoloId "
    query +=" order by P.protocoloID desc " 
    
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3],row[4]])
    
    return list

def cantidadProtocolos(especie,enfermedad,diasAtras,fechaDesde):
    list = []
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # select 26 rows from SQL table to insert in dataframe.
    query = " select  count(protocoloId)  [cantidad] ,RupOrigen, especieId, "
    query += " EnfermedadId from BitacoraMuestraAnalisis "
    query += " where EspecieId = 39  and EnfermedadId = 79 "
    query += " and convert(datetime,FechaCreacion,103) between(select dateadd(day, - 760, getdate())) and getdate()  "
    query += " group by RupOrigen,especieId, EnfermedadId "
    query += " union "
    query += " select count(protocoloId) [cantidad],RupOrigen, especieId, EnfermedadId from BitacoraResultadoPruebaCampo "
    query += " where EspecieId = 39  and EnfermedadId = 79 "
    query += " and convert(datetime, FechaRegistro, 103) between(select dateadd(day, - 760, getdate())) and convert(datetime, '"+fechaDesde+"', 103)  "
    query += " group by RupOrigen,especieId, EnfermedadId   " 
    
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[3],row[4]])
    
    return list


def obtenerOficinas():
    list = []
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # select 26 rows from SQL table to insert in dataframe.
    query = "select * from ms_Sectorial " 
    
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[1],row[2],row[7]])
    
    return list

def obtenerResultadosInfluenzaAviar():
    list = []
    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
       # select 26 rows from SQL table to insert in dataframe.
    
    query = " select "
    
    query += " DATEPART(year,bm.FechaCreacion) [anio], "
    
    query += " case  DATEPART(week,bm.FechaCreacion)-1 when 0 then 1 else DATEPART(week,bm.FechaCreacion)-1 end [2022], "
        
    query += " 	  case  DATEPART(week,bm.FechaCreacion) when 0 then 1 else DATEPART(week,bm.FechaCreacion) end [2023], "
    
    query += " bm.FechaCreacion, "
    query += " 	  bm.regionId, "
    query += " bm.Sectorid, "
    query += " p.CoordenadaX, "
    query += " p.CoordenadaY, "
    query += "      case p.Huso when 0 then 19 else p.Huso end , "
    query += " case bm.RupOrigen when '0' then bm.rupTomamuestra else bm.rupOrigen end [rup] , "
    query += "      bm.SectorIdOrigen,bm.ProtocoloId,  "
    query += "      (select count(id) from bitacoramuestraanalisis where protocoloid = bm.ProtocoloId  "
    query += " and ContieneEnfermedad = 0) as [negativos],  "
    query += "      (select count(id) from bitacoramuestraanalisis ba, PesosEnfermedadTecnica t where ba.protocoloid = bm.ProtocoloId  "
    query += " and ba.ContieneEnfermedad = 1) as [positivos] , "
    query += " bm.NombreEstablecimiento "
    query += " from bitacoramuestraanalisis bm , protocolo P "
    query += "      where bm.muestracerrada = 1  "
    query += " 	 and bm.ProtocoloId = P.PK_ProtocoloId "
    query += " and bm.EspecieId in (23,24)  and bm.EnfermedadId = 44 "
    query += "  and bm.ObjetivoId not in(9,10,4,7,15) " 
    query += " and bm.FechaCreacion between convert(datetime, '2022-05-12', 103)  and convert(datetime, '2023-05-12', 103)  "
    query += "      group by bm.FechaCreacion, bm.regionId, "
    query += " 	 bm.Sectorid, p.CoordenadaX, "
    query += " 	 p.CoordenadaY,p.huso,bm.RupTomaMuestra, bm.RupOrigen,bm.SectorIdOrigen,bm.ProtocoloId,bm.EnfermedadId,bm.TecnicaId,  "
    query += " bm.NombreEstablecimiento "
    query += " order by bm.FechaCreacion"
    
    df = pd.read_sql(query, cnxn)
        
    df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in df.iterrows():
        list.append([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15]])
    
    return list