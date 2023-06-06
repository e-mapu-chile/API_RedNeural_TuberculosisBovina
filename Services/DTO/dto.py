# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 21:42:43 2022

@author: pedro
"""
import json

class nivelContagioDto:
    def __init__(self,idRup,rup,peso,oficina,coordenadaX,coordenadaY, huso,cod_estab):
        self.idRup = idRup
        self.rup = rup
        self.peso = peso
        self.oficina = oficina
        self.coordenadaX = coordenadaX
        self.coordenadaY = coordenadaY
        self.huso = huso
        self.cod_estab = cod_estab
        

class sectorialDto:
    def __init__(self, oficinaId, regionId, nombre):
        self.oficinaId = oficinaId
        self.regionId = regionId
        self.nombre = nombre
        
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    
   

class dataSetMovimientoVigilancia:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    
    def __init__(self, IdRup,
        Rup ,
        OficinaId,
        CoordenadaX ,
        CoordenadaY ,
        Huso ,
        Latitud ,
        Longitud, 
        PesoContagio ,
        PesoMasaSalida ,
        PesoMasaEntrada, 
        NivelVigilancia, 
        ProtocoloPc ,
        CantidadNegativosPc ,
        CantidadPositivosPc ,
        ProtocoloLab ,
        CantidadNegativosLab ,
        CantidadPositivosLab ,
        TodosNegativosLab ,
        AlMenosUnPositivoLab ,
        TodosNegativosPc ,
        AlMenosUnPositivoPc ,
        CodigoTERubro ,
        TextoRiesgoMovimiento ,
        RiesgoMovimiento ):
        self.IdRup = IdRup 
        self.Rup = Rup 
        self.OficinaId = OficinaId
        self.CoordenadaX = CoordenadaX 
        self.CoordenadaY = CoordenadaY 
        self.Huso = Huso 
        self.Latitud = Latitud 
        self.Longitud = Longitud
        self.PesoContagio = PesoContagio 
        self.PesoMasaSalida = PesoMasaSalida 
        self.PesoMasaEntrada = PesoMasaEntrada
        self.NivelVigilancia = NivelVigilancia
        self.ProtocoloPc = ProtocoloPc 
        self.CantidadNegativosPc = CantidadNegativosPc 
        self.CantidadPositivosPc = CantidadPositivosPc 
        self.ProtocoloLab = ProtocoloLab 
        self.CantidadNegativosLab = CantidadNegativosLab 
        self.CantidadPositivosLab = CantidadPositivosLab 
        self.TodosNegativosLab = TodosNegativosLab 
        self.AlMenosUnPositivoLab = AlMenosUnPositivoLab 
        self.TodosNegativosPc = TodosNegativosPc 
        self.AlMenosUnPositivoPc= AlMenosUnPositivoPc 
        self.CodigoTERubro = CodigoTERubro 
        self.TextoRiesgoMovimiento = TextoRiesgoMovimiento 
        self.RiesgoMovimiento = RiesgoMovimiento 
        
        

class dataSetMovimientos:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    
    def __init__(self, FechaFma,
                 RupOrigen,
                 LatitudOrigen,
                 LongitudOrigen,
                 RupDestino,
                 LatitudDestino,
                 LongitudDestino):
        self.FechaFma = FechaFma 
        self.RupOrigen = RupOrigen 
        self.LatitudOrigen = LatitudOrigen
        self.LongitudOrigen = LongitudOrigen
        self.RupDestino = RupDestino
        self.LatitudDestino = LatitudDestino
        self.LongitudDestino = LongitudDestino
       
       
       
class dataSetRupGrupoEspecie:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    
    def __init__(self, IdRup,
        Rup ,
        RegionId,
        TipoEstablecimientoId,
        TipoEstablecimiento,
        RubroId,
        Rubro,
        OficinaId,
        CoordenadaX ,
        CoordenadaY ,
        Huso ,
        Latitud ,
        Longitud, 
         ):
        self.IdRup = IdRup 
        self.Rup = Rup 
        self.RegionId = RegionId
        self.TipoEstablecimientoId = TipoEstablecimientoId
        self.TipoEstablecimiento = TipoEstablecimiento
        self.RubroId = RubroId
        self.Rubro = Rubro
        self.OficinaId = OficinaId
        self.CoordenadaX = CoordenadaX 
        self.CoordenadaY = CoordenadaY 
        self.Huso = Huso 
        self.Latitud = Latitud 
        self.Longitud = Longitud
       
        
class dataSetResultadosIA:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    
    def __init__(self, anio,
        semana22 ,
        semana23,
        FechaCreacion, 
        RegionId, 
        SectorId, 
        CoordenadaX, 
        CoordenadaY,
        Huso, 
        RupTomaMuestra, 
        ProtocoloId, 
        Negativos, 
        Positivos,
        NombreEstablecimiento,
        Latitud ,
        Longitud
         ):
        self.anio = anio 
        self.semana22 = semana22 
        self.semana23 = semana23
        self.FechaCreacion = FechaCreacion
        self.RegionId =  RegionId
        self.SectorId = SectorId
        self.CoordenadaX = CoordenadaX
        self.CoordenadaY= CoordenadaY
        self.Huso = Huso
        self.RupTomaMuestra = RupTomaMuestra
        self.ProtocoloId = ProtocoloId
        self.Negativos = Negativos
        self.Positivos = Positivos
        self.NombreEstablecimiento = NombreEstablecimiento
        self.Latitud = Latitud 
        self.Longitud = Longitud
        
        
        
    
    