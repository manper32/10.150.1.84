#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 11:38:33 2020

@author: manuel
"""

import mysql.connector 
import psycopg2

#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#credenciales MySQL llamadas
connM = {
	'host' : '10.150.1.206',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

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

#query MySQL
queryM = """
SELECT distinct
	regexp_replace(vendor_lead_code , '[^0-9]', '')phone
	,modify_date 
	,status
FROM asterisk.vicidial_list
WHERE modify_date IS not NULL
and vendor_lead_code not in ('3106669238','3108575751','93023562782')
and regexp_replace(vendor_lead_code, '[^0-9]', '') != ''
and cast(modify_date as date) = DATE_ADD(CURRENT_DATE() ,INTERVAL -1 DAY)
and status != 'NEW'
and list_id = 20200811001;
"""

#query PostgreSQL
queryP_in ="""
INSERT INTO bi_snap.telefonos(
telefono
,fecha
,estado) VALUES(%s,%s,%s)
"""

#conexion a PostgreSQL produccion
conexionP = psycopg2.connect(**connP_P)
cursorP = conexionP.cursor ()

#Conexion MySQL 206
conexionM = mysql.connector.connect(**connM)
cursorM = conexionM.cursor ()

#ejecuciones mySQL  206
cursorM.execute(queryM)
anwr = cursorM.fetchall()

if anwr != []:
    #insercion
    b=0;
    r=0;
    for i in range(len(anwr)):
    	try:
    		cursorP.execute(queryP_in,anwr[i])
    		conexionP.commit()
    		b += 1
#    		print(i)
    	except:
    		r += 1
            
    cursorP.execute(queryP)
    conexionP.commit()
else:
    pass

#close Mysqls
cursorM.close()
conexionM.close()
for x in range(len(anwr)):
    #close PostgreSQL
    cursorP.close()
    conexionP.close()