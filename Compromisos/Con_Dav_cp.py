#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 16:12:51 2020

@author: manuel
"""

import pyodbc
import psycopg2
from datetime import date
from datetime import timedelta


today = date.today()
yesterday = date.today() - timedelta(days = 1)
ls = today.strftime('%Y-%m-%d')
li = yesterday.strftime('%Y-%m-%d')
#ls = '2020-08-31'
#li = '2020-09-01'
#print(pyodbc.drivers())

#credenciales MySQL
connS = {
    'driver' : 'ODBC Driver 17 for SQL Server',
	'host' : '10.150.1.81',
	'user':'sa',
	'password':'Analistadb1020',
	'database' : 'Davivienda'}

#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#query SQLserver
queryS = """
declare @li date = '"""+ li +"""'
declare @ls date = '"""+ ls +"""'

select distinct
rtrim(ltrim(indice)) deudor_id,
rtrim(ltrim(credito)) obligacion_id,
rtrim(ltrim(monpac)) valor,
cast(rtrim(ltrim(fecges)) as date) fecha_compromiso,
cast(rtrim(ltrim(feccom)) as date) fecha_pago,
rtrim(ltrim(asesor)) asesor
from davivienda.dbo.cob_gestrim
where val_indicad like '%COMPR%'
--and codemp like '%COPR%'
and ltrim(rtrim(fecges)) between @li and @ls
or val_indicad like '%NEGOC%'
--and codemp like '%COPR%'
and ltrim(rtrim(fecges)) between @li and @ls
order by rtrim(ltrim(indice));
"""

queryP_del_P = """
delete from cbpo_davivienda.compromisos 
where fecha_compromiso >= '"""+ li +"' and asesor not like '%WOLK%';"

#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_davivienda.compromisos(
deudor_id
,obligacion_id
,valor
,fecha_compromiso
,fecha_pago
,asesor) VALUES(%s,%s,%s,%s,%s,%s)
"""
#conexion a SQLserver
conexionS = pyodbc.connect(**connS)
cursorS = conexionS.cursor()
#conexion a PSQL
conexionP_P = psycopg2.connect(**connP_P)
cursorP_P = conexionP_P.cursor()

#ejecuciones
cursorS.execute(queryS)
anwr = cursorS.fetchall()

cursorP_P.execute(queryP_del_P)
conexionP_P.commit()

#cursorP_P.execute(queryP_in_P,anwr[3])
#conexionP_P.commit()

#insercion
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_P.execute(queryP_in_P,anwr[i])
        conexionP_P.commit()
        b += 1
    except:
        r += 1
#        print(i)

#close SQL server
cursorS.close()
conexionS.close()
for j in range(len(anwr)):
    #close PostgreSQL
    cursorP_P.close()
    conexionP_P.close()
