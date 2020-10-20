#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 19 14:01:20 2020

@author: manuel
"""
import pandas as pd
import mysql.connector 
import psycopg2

#credenciales MySQL120
connM1 = {
	'host' : '10.150.1.120',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales MySQL122
connM2 = {
	'host' : '10.150.1.122',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales MySQL123
connM3 = {
	'host' : '10.150.1.123',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

##credenciales MySQL206
#connM4 = {
#	'host' : '10.150.1.206',
#	'user':'desarrollo',
#	'password':'soportE*8994',
#	'database' : 'asterisk'}

#credenciales PostgreSQL
connP = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#query MySQL
queryM = """
select DISTINCT
	regexp_replace(phone_number, '[^0-9]', '')phone
	,call_date
	,status
from asterisk.vicidial_log
where regexp_replace(phone_number, '[^0-9]', '') != ''
and cast(call_date as date) = date_add(current_date(), INTERVAL -1 DAY)
and status != 'NEW'
order by regexp_replace(phone_number, '[^0-9]', '');
"""

#query MySQL
queryP = """
delete from bi_snap.telefonos
where ctid in (
select
	min(ctid) id
from bi_snap.telefonos
group by telefono,fecha ,estado
having count(*)>1
order by count(*));
"""

#SELECT 
#	regexp_replace(phone_number, '[^0-9]', '')phone
#	,modify_date 
#	,status
#FROM asterisk.vicidial_list
#WHERE modify_date IS not NULL
#and regexp_replace(phone_number, '[^0-9]', '') != ''
#and modify_date >= '2020/08/01'
#and status != 'NEW';
#"""

#query PostgreSQL
queryP_in ="""
INSERT INTO bi_snap.telefonos(
telefono
,fecha
,estado) VALUES(%s,%s,%s)
"""

#Conexion MySQL20
conexionM1 = mysql.connector.connect(**connM1)
cursorM1 = conexionM1.cursor()

#Conexion MySQL22
conexionM2 = mysql.connector.connect(**connM2)
cursorM2 = conexionM2.cursor()

#Conexion MySQL23
conexionM3 = mysql.connector.connect(**connM3)
cursorM3 = conexionM3.cursor()

##Conexion MySQL206
#conexionM4 = mysql.connector.connect(**connM4)
#cursorM4 = conexionM4.cursor()

#Conexion PostgreSQL
conexionP = psycopg2.connect(**connP)
cursorP = conexionP.cursor ()

#ejecuciones SQL20
cursorM1.execute(queryM)
anwr = cursorM1.fetchall()

#ejecuciones SQL22
cursorM2.execute(queryM)
anwr2 = cursorM2.fetchall()

#ejecuciones SQL23
cursorM3.execute(queryM)
anwr3 = cursorM3.fetchall()

#ejecuciones SQL206
#cursorM4.execute(queryM)
#anwr4 = cursorM4.fetchall()

anwr = pd.DataFrame(anwr)
anwr2 = pd.DataFrame(anwr2)
anwr3 = pd.DataFrame(anwr3)
fn = pd.DataFrame()

for n in [anwr,anwr2,anwr3]:
	fn = fn.append(n)
    
#fn = fn.rename(columns={0:'date',
#                        1:'phone',
#                        2:'status'})
##fn = fn.drop_duplicates()
#fn3 = fn.drop_duplicates(subset='phone', keep="last")
fn2 = [tuple(x) for x in fn.values]


#cursorP.execute(queryP_in,fn2[0])
#conexionP.commit()

#vicidial
#insercion
b=0;
r=0;
for x in range(len(fn2)):
	try:
    		cursorP.execute(queryP_in,fn2[x])
    		conexionP.commit()
    		b += 1
#    		print(x)
	except:
            r += 1
#            print(x)
            
cursorP.execute(queryP)
conexionP.commit()

#cursorP.execute(queryP_in,anwr4[0])
#conexionP.commit()

##app llamadas
##insercion
#b=0;
#r=0;
#for x in range(1,len(anwr4)):
#	try:
#    		cursorP.execute(queryP_in,anwr4[x])
#    		conexionP.commit()
#    		b += 1
#    		print(x)
#	except:
#            r += 1
##            print(x)        
        

#close Mysql120
cursorM1.close()
conexionM1.close()
#close Mysql122
cursorM2.close()
conexionM2.close()
#close Mysql123
cursorM3.close()
conexionM3.close()
##close Mysql206
#cursorM4.close()
#conexionM4.close()

for x in range(len(fn)):
    #close PostgreSQL
    cursorP.close()
    conexionP.close()