# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 01:03:31 2022

@author: pedro
"""

import numpy as np
import tensorflow as tf
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import utm
from jsonmerge import merge
from functools import wraps


from Services.sipec import aplicarNivelContagio,aplicarPesoEntrada,aplicarPesoSalida,pesoInOutMovimiento ,test,localT,ObtenerMovimientosIn,ObtenerMovimientosOut, obtenerPredios

from Services.ssa import testssa,cantidadProtocolos,resultadosProtocolosRegionOrigen,resultadoPruebaCampoBB, obtenerOficinas, obtenerResultadosInfluenzaAviar

from Services.DTO.dto import dataSetMovimientoVigilancia,sectorialDto,dataSetMovimientos,dataSetRupGrupoEspecie,dataSetResultadosIA


print(tf.__version__)


app = Flask(__name__)

cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

#%%
@app.route("/api/annRiesgoMovTbb/<string:region>", methods=["GET"])
def clasificarRiesgoTbb(region):
    
    
    #Paso 2: Cargar el modelo pre entrenado
    with open('RedesNeurales_Supervisadas/modelo_pred_RiesgoMovimientoVigilancia.json', 'r') as f:  
        model_json = f.read()

    model = tf.keras.models.model_from_json(model_json)

    # cargar los pesos en el modelo
    model.load_weights("RedesNeurales_Supervisadas/modelo_pred_RiesgoMovimientoVigilancia.h5")

    
    df_pred = pd.read_excel("DS/Region"+region+".xlsx")
    df_pred_real = df_pred.drop(['CoordenadaX','CoordenadaY','Huso',' TextoRiesgo','ClasificacionSanitaria',
             'Latitud','Longitud','ProtocoloLab','ProtocoloPc',' NegativosPc',
             'NegativosLab','PositivosLab',' PositivosPc','Id','Oficina',' TextoRiesgo','RUP'],axis=1)
    df_post_pred =  pd.concat([df_pred_real],axis=1)
    X_pred = df_post_pred.drop(' RiesgoMovimiento',axis=1)
    
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    scaler.fit(X_pred)
    X_pred = scaler.transform(X_pred)
    resultado = (model.predict(X_pred) > 0.5).astype("int32") 
    df_lab = pd.DataFrame(resultado, columns = ['1','2','3','4','5','6','7','8','9'])
    
    df_pred['PREDICCION']=np.where(df_lab["1"]==1,1,
                                   np.where(df_lab["2"]==1,2,
                                            np.where(df_lab["3"]==1,3,
                                                    np.where(df_lab["4"]==1,4,
                                                            np.where(df_lab["5"]==1,5,
                                                                    np.where(df_lab["6"]==1,6,
                                                                            np.where(df_lab["7"]==1,7,
                                                                                    np.where(df_lab["8"]==1,8,9))))))))
    
    
    df_pred['PesoContagio'] =   df_pred['Peso Nivel Contagio'] 
    df_pred['NivelVigilancia'] = df_pred['Nivel Vigilancia'] 
    x = df_pred.to_json(orient = 'records')
    yas = json.loads(x)
    #Devolver la predicción al usuario
    return jsonify(yas)

@app.route("/api/annRiesgoMovTbbSContr/<string:region>", methods=["GET"])
def annRiesgoMovTbbSContr(region):
    
    
    #Paso 2: Cargar el modelo pre entrenado
    with open('RedesNeurales_Supervisadas/modelo_pred_ClasificacionRiesgoTBBBovino.json', 'r') as f:  
        model_json = f.read()

    model = tf.keras.models.model_from_json(model_json)

    # cargar los pesos en el modelo
    model.load_weights("RedesNeurales_Supervisadas/modelo_pred_ClasificacionRiesgoTBBBovino.h5")

    
    df_pred = pd.read_excel("DS/Region"+region+".xlsx")
    df_pred_real = df_pred.drop(['CoordenadaX','CoordenadaY','Huso',' TextoRiesgo','ClasificacionSanitaria',
             'Latitud','Longitud','ProtocoloLab','ProtocoloPc',' NegativosPc',
             'NegativosLab','PositivosLab',' PositivosPc','Id','Oficina',' TextoRiesgo','RUP'],axis=1)
    df_post_pred =  pd.concat([df_pred_real],axis=1)
    X_pred = df_post_pred.drop(' RiesgoMovimiento',axis=1)
    
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    scaler.fit(X_pred)
    X_pred = scaler.transform(X_pred)
    resultado = (model.predict(X_pred) > 0.5).astype("int32") 
    df_lab = pd.DataFrame(resultado, columns = ['1','2','3','4','5','6','7','8','9','10'])
    
    df_pred['PREDICCION']=np.where(df_lab["1"]==1,1,
                                   np.where(df_lab["2"]==1,2,
                                            np.where(df_lab["3"]==1,3,
                                                    np.where(df_lab["4"]==1,4,
                                                            np.where(df_lab["5"]==1,5,
                                                                    np.where(df_lab["6"]==1,6,
                                                                            np.where(df_lab["7"]==1,7,
                                                                                     np.where(df_lab["8"]==1,8,
                                                                                    np.where(df_lab["9"]==1,9,10)))))))))
    
    
    df_pred['PesoContagio'] =   df_pred['Peso Nivel Contagio'] 
    df_pred['NivelVigilancia'] = df_pred['Nivel Vigilancia'] 
    x = df_pred.to_json(orient = 'records')
    yas = json.loads(x)
    #Devolver la predicción al usuario
    return jsonify(yas)



@app.route("/api/getPrediosGrupoEspecie", methods=["GET"])
def getPrediosGrupoEspecie():
    args = request.args
    grupoEspecie =  args.get('grupoEspecie')
      
    
    listaPrediosGrupo = obtenerPredios(grupoEspecie)
    
    listar = []
    jsonRet = "["
    flag = 0
    for x in listaPrediosGrupo:
        
        #listaNivelMasaMov =>
        #  id           
        #[ 07.4.01.1493	7	1	PREDIO	19	CRIANZA FAMILIAR	31	265648	6032171	19]
        lati = 0
        long = 0
     
        
        try:
            if(x[8] != None and x[9] != None and x[10] != None):
                if(int(x[8]) > 0 and int(x[9]) > 0  and int(x[10]) > 0):
                    if (int(x[8]) < 999999 and int(x[9]) < 9999999):
                        coor = utm.to_latlon(int(x[8]),int(x[9]),int(x[10]),'H')
                        lati = coor[0]
                        long = coor[1]
        except:
            print("error pero siga")
               
        #listaNivelMasaMov =>
      # Rup ,
        #RegionId,
       # TipoEstablecimientoId,
      ##  TipoEstablecimiento,
     # #  RubroId,
      #  Rubro,
     #   OficinaId,
     #   CoordenadaX ,
     #   CoordenadaY ,
    #    Huso ,
    #    Latitud ,
    #    Longitud
        dataSets = dataSetRupGrupoEspecie(0,str(x[1]),str(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),
                                          str(x[7]),str(x[8]),str(x[9]),str(x[10]),str(lati),
                                              str(long))
        jsonL = dataSets.toJSON()
        if(flag == 0):
            jsonRet += jsonL
            flag = flag +1
        else:
            jsonRet += ","+jsonL
       
        
            
                        
       #print(x[0],x[1],x[5],x[6],x[7],x[8],lati,long,x[2],x[3],x[4],nivelVigilanciaValue,
       #       0,0,0,0,0,0,0,alMenosUnPositivoLab,todosNegativosLab,alMenosUnPositivoPC,
       #       todosNegativosPc,None,"")       
                
        
  

    jsonRet += "]"
    return jsonRet


@app.route("/api/getResultadosIA", methods=["GET"])
def getResultadosIA():
    listaResultadosIA = [] 
    listaResultadosIA = obtenerResultadosInfluenzaAviar()
    print(listaResultadosIA)
    jsonRet = "["
    flag = 0
    for x in listaResultadosIA:
                
        lati = 0
        long = 0
        try:
            if(x[7] != None and x[8] != None and x[9] != None):
                if(int(x[7]) > 0 and int(x[8]) > 0  and int(x[9]) > 0):
                    if (int(x[7]) < 999999 and int(x[8]) < 9999999):
                        coor = utm.to_latlon(int(x[7]),int(x[8]),int(x[9]),'H')
                        lati = coor[0]
                        long = coor[1]
        except:
            print("error pero siga")
            
      #anio,semana22 ,          semana23,FechaCreacion, RegionId, SectorId, CoordenadaX, CoordenadaY,
       #   Huso, RupTomaMuestra, ProtocoloId, Negativos, Positivos,Latitud ,Longitud
        #[1394, 2023, 41, 42, 2023-10-15 00:00:00', 4, 12, 
        #279619.0, 6685126.0, 18, '04.1.02.0500', 12, 232026, 17,0]
        dataSets = dataSetResultadosIA(str(x[1]),str(x[2]),str(x[3]),str(x[4]),
                                       str(x[5]),str(x[6]),str(x[7]),str(x[8]),
                                       str(x[9]),str(x[10]),str(x[12]),
                                      str(x[13]),str(x[14]),str(x[15]),lati,long)
        jsonL = dataSets.toJSON()
        if(flag == 0):
             jsonRet += jsonL
             flag = flag +1
        else:
            jsonRet += ","+jsonL
   
        
    jsonRet += "]"
    return jsonRet

@app.route("/api/getOficinas", methods=["GET"])
def getOficinas():
    listaOf = [] 
    listaOficinas = obtenerOficinas()
    print(listaOficinas)
    jsonRet = "["
    flag = 0
    for x in listaOficinas:
        
        dataSets = sectorialDto(str(x[0]),str(x[1]),x[2])
        jsonL = dataSets.toJSON()
        if(flag == 0):
             jsonRet += jsonL
             flag = flag +1
        else:
            jsonRet += ","+jsonL
   
        
    jsonRet += "]"
    return jsonRet
    
   

@app.route("/api/testa", methods=["GET"])
def testa():
    args = request.args
    especie =  args.get('especie')
    enfermedad =  args.get('enfermedad')
    region = args.get('region')
    sector = args.get('sector')
    rup = args.get('rup')
    fechaDesde = args.get('fechaDesde')
    cantidadDias = args.get('cantidadDias')
    
    predioLeche = args.get('predioLeche')
    predioCarne = args.get('predioCarne')
    mataderoCarne = args.get('mataderoCarne')
    controlAnimalVivo = args.get('controlAnimalVivo')
    centroRodeo = args.get('centroRodeo')
    predioCrianza = args.get('predioCrianza')
    predioCompra = args.get('predioCompra')
    feriaCompra = args.get('feriaCompra')
    
    
    
    valorAlta = args.get('valorAlta')
    valorMediaAlta = args.get('valorMediaAlta')
    valorMedia = args.get('valorMedia')
    valorBaja = args.get('valorBaja')
    valorVigilanciaAlta = args.get('valorVigilanciaAlta')
    valorVigilanciaMedia = args.get('valorVigilanciaMedia')
    valorVigilanciaBaja = args.get('valorVigilanciaBaja')
      
    
    
    listaNivelContagio = aplicarNivelContagio(region, sector, rup,predioLeche,predioCarne,
                             mataderoCarne,controlAnimalVivo,centroRodeo,predioCrianza,predioCompra,
                             feriaCompra)
    listaPesoEntrada = aplicarPesoEntrada(region, sector, rup, fechaDesde, cantidadDias)
    listaPesoSalida = aplicarPesoSalida(region, sector, rup, fechaDesde, cantidadDias)
    
    listaNivelMasaMov = pesoInOutMovimiento(listaNivelContagio, listaPesoEntrada, listaPesoSalida,
                        valorAlta,valorMediaAlta,valorMedia,valorBaja)
    
    listaResultadosLab = resultadosProtocolosRegionOrigen(enfermedad, especie, region, cantidadDias,fechaDesde)
    #listaResultadosLab =>
    # RUP , oficina, protocolo, cantidad negativos, positivos 0.5, positivo 1.0
    #['10.3.03.0100', 59, 125406, 2414, 0, 0]
    listaResultadosPc = resultadoPruebaCampoBB(enfermedad, especie, cantidadDias,fechaDesde)
    #listaResultadosPc =>
    # RUP, protocolo,cantidadPositivo,cantidadNegativos
    # ['07.4.07.0296', 124624, 0, 73]
    ##IR A BUSCAR CANTIDAD DE PROTOCOLOS
    listaCantidadProtocolos = cantidadProtocolos(especie, enfermedad, cantidadDias,fechaDesde)
    #listaCantidadProtocolos =>
    #cantidadProtocolos,RUP,especie,enfermedad
    #[2, '10.1.06.2447', 39, 79]
    
    listar = []
    jsonRet = "["
    flag = 0
    for x in listaNivelMasaMov:
        
        #listaNivelMasaMov =>
        #  id            rup      NC in out ofi X         Y         huso , TE_RUBRO
        #[346053, '10.3.01.1899', 1, 1, 1, 57, '672073', '5504881', '18',TE_RUBRO]
        lati = 0
        long = 0
        cantidadProtocolo = 0
        idProtocoloLab = 0
        idProtocoloPc = 0
        
        cantidadNegativos = 0
        cantidadNegativosPc = 0
        cantidadPositivos05 = 0
        cantidadPositivos10 = 0
        
        alMenosUnPositivoLab = 0
        alMenosUnPositivoPC = 0
        todosNegativosLab = 0
        todosNegativosPc = 0
        
        try:
            if(x[6] != None and x[7] != None and x[8] != None):
                if(int(x[6]) > 0 and int(x[7]) > 0  and int(x[8]) > 0):
                    if (int(x[6]) < 999999 and int(x[7]) < 9999999):
                        coor = utm.to_latlon(int(x[6]),int(x[7]),int(x[8]),'H')
                        lati = coor[0]
                        long = coor[1]
        except:
            print("error pero siga")
            
        
        for ca in listaCantidadProtocolos:
            if(ca[1] == x[1]):
                cantidadProtocolo = ca[0]
                break
        
        #aplicar Nivel Vigilancia
        nivelVigilanciaValue = aplicarNivelVigilancia(cantidadProtocolo, valorVigilanciaAlta, valorVigilanciaMedia, valorVigilanciaBaja)
               
        for rLab in listaResultadosLab:
            if(rLab[0] == x[1]):
                idProtocoloLab = rLab[2]
                cantidadNegativos = rLab[3]
                cantidadPositivos05 = rLab[4]
                cantidadPositivos10 = rLab[5]
                break
                    
        for rPc in listaResultadosPc:
            if(rPc[0] == x[1]):
                idProtocoloPc = rPc[1]
                cantidadNegativosPc = rPc[3]
                cantidadPositivos05 = rPc[2] 
                break
            
            
        if(int(idProtocoloLab) > int(idProtocoloPc)):
            if(int(cantidadPositivos05) > 0):
                alMenosUnPositivoPC = 1
            else:
                if(int(cantidadPositivos10) > 0):
                    alMenosUnPositivoLab = 1
                else:
                    alMenosUnPositivoLab = 0
            if(int(cantidadNegativos) > 0 and int(cantidadPositivos05) < 1 
               and int(cantidadPositivos10) < 1):
                todosNegativosLab = 1
            else:
                todosNegativosLab = 0
        else:
            if(int(idProtocoloPc) > int(idProtocoloLab)):
                if(int(cantidadPositivos05) > 0):
                    alMenosUnPositivoPC = 1
                else:
                    alMenosUnPositivoPC = 0
                if(int(cantidadNegativosPc) > 0 and int(cantidadPositivos05) <1):
                    todosNegativosPc = 1
                else:
                    todosNegativosPc = 0
        
        #listaNivelMasaMov =>
        #  id            rup      NC in out ofi X         Y         huso , TE_RUBRO
        #[346053, '10.3.01.1899', 1, 1, 1, 57, '672073', '5504881', '18',TE_RUBRO]
        dataSets = dataSetMovimientoVigilancia(str(x[0]),str(x[1]),str(x[5]),str(x[6]),str(x[7]),str(x[8]),str(lati),
                                              str(long),str(x[2]),str(x[3]),str(x[4]),str(nivelVigilanciaValue),
                                                  str(idProtocoloPc),str(cantidadNegativosPc),
                                                  str(cantidadPositivos05),str(idProtocoloLab),
                                                  str(cantidadNegativos),str(cantidadPositivos10),
                                                  str(todosNegativosLab),str(alMenosUnPositivoLab),
                                                  str(todosNegativosPc),str(alMenosUnPositivoPC),
                                                  str(x[9]),"S/I","S/I")
        jsonL = dataSets.toJSON()
        if(flag == 0):
            jsonRet += jsonL
            flag = flag +1
        else:
            jsonRet += ","+jsonL
       
        
            
                        
       #print(x[0],x[1],x[5],x[6],x[7],x[8],lati,long,x[2],x[3],x[4],nivelVigilanciaValue,
       #       0,0,0,0,0,0,0,alMenosUnPositivoLab,todosNegativosLab,alMenosUnPositivoPC,
       #       todosNegativosPc,None,"")       
                
        
  

    jsonRet += "]"
    return jsonRet




@app.route("/api/getMovimientosIn", methods=["GET"])
def getMovimientosIn():
    args = request.args
    region = args.get('region')
    fechaDesde = args.get('fechaDesde')
        
    listaMovimientos = ObtenerMovimientosIn(region, fechaDesde)
    print("rsult")
    listar = []
    jsonRet = "["
    flag = 0
    for x in listaMovimientos:
        
        
        dataSets = dataSetMovimientos(str(x[0]),str(x[1]),str(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]))
        jsonL = dataSets.toJSON()
        if(flag == 0):
            jsonRet += jsonL
            flag = flag +1
        else:
            jsonRet += ","+jsonL
       
        
        
  

    jsonRet += "]"
    return jsonRet

@app.route("/api/getMovimientosOut", methods=["GET"])
def getMovimientosOut():
    args = request.args
    region = args.get('region')
    fechaDesde = args.get('fechaDesde')
        
    listaMovimientos = ObtenerMovimientosOut(region, fechaDesde)
    print("rsult")
    listar = []
    jsonRet = "["
    flag = 0
    for x in listaMovimientos:
        
        
        dataSets = dataSetMovimientos(str(x[0]),str(x[1]),str(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]))
        jsonL = dataSets.toJSON()
        if(flag == 0):
            jsonRet += jsonL
            flag = flag +1
        else:
            jsonRet += ","+jsonL
       
        
    jsonRet += "]"
    return jsonRet



def aplicarNivelVigilancia(cantidadPr,valorVigilanciaAlta,valorVigilanciaMedia,valorVigilanciaBaja):
    
    if(int(cantidadPr) >= int(valorVigilanciaAlta)):
        return 3
    if(int(cantidadPr) >= int(valorVigilanciaMedia) and int(cantidadPr) < int(valorVigilanciaAlta)):
        return 2
    if(int(cantidadPr) >= int(valorVigilanciaBaja) and int(cantidadPr) < int(valorVigilanciaMedia)):
        return 1
    
    return 0
    


    
@app.route("/api/annRiesgoMovBrucelosis", methods=["POST"])
def annRiesgoMovBrucelosis():
      
    jsona = json.loads(request.data)
    
    #Paso 2: Cargar el modelo pre entrenado
    with open('RedesNeurales_Supervisadas/modelo_pred_ClasificacionRiesgoTBBBovino.json', 'r') as f:  
        model_json = f.read()

    model = tf.keras.models.model_from_json(model_json)

    # cargar los pesos en el modelo
    model.load_weights("RedesNeurales_Supervisadas/modelo_pred_ClasificacionRiesgoTBBBovino.h5")
       
    #df_pred = pd.read_json(data)
    df_pred = pd.json_normalize(jsona)
    print("convirtio")
    df_pred_real = df_pred.drop(['CoordenadaX','CoordenadaY','Huso',
             'Latitud','Longitud','ProtocoloLab','ProtocoloPc','TextoRiesgoMovimiento',
             'IdRup','OficinaId','Rup','CodigoTERubro','CantidadNegativosLab','CantidadNegativosPc',
             'CantidadPositivosLab','CantidadPositivosPc'],axis=1)
    df_post_pred =  pd.concat([df_pred_real],axis=1)
    X_pred = df_post_pred.drop('RiesgoMovimiento',axis=1)
   
    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    scaler.fit(X_pred)
    X_pred = scaler.transform(X_pred)
    resultado = (model.predict(X_pred) > 0.5).astype("int32") 
    df_lab = pd.DataFrame(resultado, columns = ['1','2','3','4','5','6','7','8','9','10'])
    
    df_pred['PREDICCION']=np.where(df_lab["1"]==1,1,
                                   np.where(df_lab["2"]==1,2,
                                            np.where(df_lab["3"]==1,3,
                                                    np.where(df_lab["4"]==1,4,
                                                            np.where(df_lab["5"]==1,5,
                                                                    np.where(df_lab["6"]==1,6,
                                                                            np.where(df_lab["7"]==1,7,
                                                                                     np.where(df_lab["8"]==1,8,
                                                                                    np.where(df_lab["9"]==1,9,10)))))))))
        
    print(df_pred['PREDICCION'])
    x = df_pred.to_json(orient = 'records')
    yas = json.loads(x)
    #Devolver la predicción al usuario
    return jsonify(yas)
    
   
   

#Iniciar la aplicación de Flask
app.run(port=5000, debug=False)
