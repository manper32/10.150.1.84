#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 12:24:29 2020

@author: manuel
"""

import pyodbc
import psycopg2

#credenciales MySQL
connS = {
    'driver' : 'ODBC Driver 17 for SQL Server',
	'host' : '10.150.1.22',
	'user':'sa',
	'password':'analistadb1020',
	'database' : 'Gestion_Bi'}

#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#conexion a SQL server
conexionS = pyodbc.connect(**connS)
#print('\nConexión con el servidor SQLserver establecida!')
cursorS = conexionS.cursor ()
#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL produccion establecida!')
cursorP_P = conexionP_P.cursor ()

#query SQLserver
queryS = """
set dateformat ymd
select			ltrim(rtrim(indice)) deudor_id,	
				cast(telefono as bigint) telefono,
				/*cast(ltrim(ltrim(LEFT(SUBSTRING(telefono, PATINDEX('%[0-9.-]%', telefono), 8000),
				PATINDEX('%[^0-9.-]%', SUBSTRING(telefono, PATINDEX('%[0-9.-]%', telefono), 8000) + 'X') -1)))as bigint) telefono,*/
				--telefono,
				COUNT(*)as marcaciones,
				cast(MAX(horfingest) as datetime) as Fecha_ultima_marcacion
from 
(
	select * 
	from [CarteraPropia].[dbo].[cob_gestion_COB2]
	where val_indicad not in ('MENSAJE', 'NO CONTACTADO','') 
		and telefono!='' 
		and len(ltrim(rtrim(telefono))) > 5
) Con1 
where telefono not like '%[^0-9]%'
group by ltrim(rtrim(indice)),cast(telefono as bigint)/*cast(ltrim(ltrim(LEFT(SUBSTRING(telefono, PATINDEX('%[0-9.-]%', telefono), 8000),
				PATINDEX('%[^0-9.-]%', SUBSTRING(telefono, PATINDEX('%[0-9.-]%', telefono), 8000) + 'X') -1)))as bigint)*/
order by ltrim(rtrim(indice)) desc;
"""

#quey PostgeSQL delete
queryP_del_P = """
truncate table cbpo_propia.telefonos_positivos;
"""

#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_propia.telefonos_positivos(
deudor_id
,telefono
,marcaciones
,fec_ultima_marcacion) VALUES(%s,%s,%s,%s)
"""

#ejecuaciones
cursorS.execute(queryS)
anwr = cursorS.fetchall()

cursorP_P.execute(queryP_del_P)
conexionP_P.commit()

#cursorP_P.execute(queryP_in_P,anwr[62407])
#conexionP_P.commit()

#insercion
b = 0
r = 0
for i in range(len(anwr)):
#for i in range(62404):
    try:
        cursorP_P.execute(queryP_in_P,anwr[i])
        conexionP_P.commit()
        b += 1
    except:
        r += 1
#        print(i)

#print("se pudieron cargar produccion : ",b)
#print("no se pudieron cargar produccion : ",r)

#close SQL server
cursorS.close()
conexionS.close()
for j in range(len(anwr)):
    #close PostgreSQL
    cursorP_P.close()
    conexionP_P.close()