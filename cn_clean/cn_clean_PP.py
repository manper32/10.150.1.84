#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 10:50:08 2020

@author: manuel
"""
import psycopg2

query = """
SELECT pg_terminate_backend(pid) FROM pg_stat_activity where datname like '%post%' ;
"""

#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL produccion establecida!')
cursorP_P = conexionP_P.cursor ()
#ejecucion query telefonos PostgreSQL
try:
    cursorP_P.execute(query)
    #cursorP_P.commit()
except:
    pass

cursorP_P.close()
conexionP_P.close()