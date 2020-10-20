#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 15:33:04 2020

@author: manuel
"""
import mysql.connector 
import psycopg2
from datetime import date
from datetime import timedelta

#credenciales MySQL
connM = {
	'host' : '10.200.50.4',
	'user':'bi',
	'password':'team.dev.2020!',
	'database' : 'cobrando'}

#credenciales PostgreSQL
connP = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}


today = date.today()
yesterday = date.today() - timedelta(days = 1)
ls = today.strftime('%Y-%m-%d')
li = yesterday.strftime('%Y-%m-%d')

#query MySQL
queryM = """
SELECT deudor_id
	,'' abligacion
	,cast(replace(replace(Valor,'$',''),'.','')as unsigned) Valor
	,case 	when length(manage_date) = 15 
			then str_to_date(replace(manage_date,' ',' 0'),'%d.%m.%Y %H%i%s')
			when length(manage_date) = 16
			then str_to_date(manage_date,'%d.%m.%Y %H%i%s')
			else str_to_date(manage_date,'%d.%m.%Y %H.%i.%s')
	end manage_date
	,str_to_date(commit_date ,'%d.%m.%Y')commit_date
	,'WOLK-CBOT'
from cobrando.manage_pop
where status in ('CP','NG')
and case 	when length(manage_date) = 15 
			then str_to_date(replace(manage_date,' ',' 0'),'%d.%m.%Y %H%i%s')
			when length(manage_date) = 16
			then str_to_date(manage_date,'%d.%m.%Y %H%i%s')
			else str_to_date(manage_date,'%d.%m.%Y %H.%i.%s')
	end >= '"""+ li +"';"

#query PostgreSQL
queryP_in ="""
INSERT INTO cbpo_popular.compromisos(
deudor_id	
,obligacion_id	
,valor	
,fecha_compromiso	
,fecha_pago	
,asesor) VALUES(%s,%s,%s,%s,%s,%s)
"""

queryP_del_P = """
delete from cbpo_popular.compromisos
where fecha_compromiso >= '"""+ li +"'and asesor = 'WOLK-CBOT';"

#Conexion MySQL
conexionM = mysql.connector.connect(**connM)
cursorM = conexionM.cursor ()

#Conexion PostgreSQL
conexionP = psycopg2.connect(**connP)
cursorP = conexionP.cursor ()

#ejecuciones
cursorM.execute(queryM)
anwr = cursorM.fetchall()

cursorP.execute(queryP_del_P)
conexionP.commit()

#cursorP.execute(queryP_in,anwr[0])
#conexionP.commit()
if len(anwr) > 0:
    #insercion
    b = 0
    r = 0
    for i in range(len(anwr)):
    #for i in range(617):
        try:
            cursorP.execute(queryP_in,anwr[i])
            conexionP.commit()
            b += 1
        except:
            r += 1


for j in range(len(anwr)):
    #close PostgreSQL
    cursorP.close()
    conexionP.close()
    #close SQL server
    cursorM.close()
    conexionM.close()