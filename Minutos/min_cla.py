#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 08:27:57 2020

@author: manuel
"""
import psycopg2
import pandas as pd
import glob
import os
#import numpy as np

#credenciales PostgreSQL
connP = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#tomar los archivos con data
#os.getcwd()
#dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20200703/*.txt')
#dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20200803/*.txt')
#dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20200903/*.txt')
# dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20201003/*.txt')
# dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20201103/*.txt')
dir_arc = glob.glob('/home/manuel/Documentos/Archivos_Claro/8.22448996_20201203/*.txt')

f_arc = []
for i in range(len(dir_arc)):
    if os.path.getsize(dir_arc[i]) > 1033:
        f_arc.append(dir_arc[i])

#query PostgreSQL
queryP_in2 ="""
INSERT INTO bi_snap.tmp_minutos_claro(col) VALUES(%s)
"""

#for x in range(len(f_arc)):

for z in f_arc:
    arc2 = pd.read_csv(z)
#    arc2 = pd.read_csv(f_arc[0])
    arc = list(arc2.itertuples(index=False))
    
    #Conexion PostgreSQL
    conexionP = psycopg2.connect(**connP)
    cursorP = conexionP.cursor()
    
#    cursorP.execute(queryP_in2,arc[16])
#    conexionP.commit()
    
    #insercion
    b=0;
    r=0;
    for x in range(len(arc)):
        try:
            cursorP.execute(queryP_in2,arc[x])
            conexionP.commit()
            b += 1
            print(x)
        except:
            conexionP = psycopg2.connect(**connP)
            cursorP = conexionP.cursor()
            r += 1
            
    for x in range(len(arc)):
        #close PostgreSQL
        cursorP.close()
        conexionP.close()        
