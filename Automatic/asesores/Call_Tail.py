#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  5 16:13:19 2020

@author: manuel
"""

import pandas as pd
import mysql.connector 
import psycopg2
from datetime import datetime
from datetime import timedelta


today = datetime.now()
ls = today.strftime('%Y/%m/%d %H:%M:%S')
li = (today - timedelta(hours = 1)).strftime('%Y/%m/%d %H:%M:%S')
#ls = '2020/10/01'
#li = '2020/10/01'

#credenciales MySQL120
connM1 = {
	'host' : '10.152.1.120',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales MySQL122
connM2 = {
	'host' : '10.152.1.122',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales MySQL123
connM3 = {
	'host' : '10.152.1.123',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales PostgreSQL
connP = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#query MySQL
queryP_0 = """
select max(event_time)
from bi_snap.gestion_det 
where server_ip = '10.152.1.120';
"""
#query MySQL
queryP_2 = """
select max(event_time)
from bi_snap.gestion_det 
where server_ip = '10.152.1.122';
"""
#query MySQL
queryP_3 = """
select max(event_time)
from bi_snap.gestion_det 
where server_ip = '10.152.1.123';
"""

#query MySQL
queryM = """
SELECT
	DISTINCT val.agent_log_id,
	val.`user`,
	val.event_time,
	val.lead_id,
	val.status,
	val.user_group,
	val.campaign_id, 
	val.comments,
	val.uniqueid,
	val.pause_type,
	case
		when vl.phone_number is null
		and vcl.phone_number is null then ucl.phone_number
		when vl.phone_number is null
		and vcl.phone_number is not null then vcl.phone_number
		else vl.phone_number
	end phone_number,
	CASE 	
		when val.comments is null then vl2.vendor_lead_code
		else null end Cedula
    ,val.server_ip
	FROM asterisk.vicidial_agent_log val
left join asterisk.vicidial_log vl on
	vl.uniqueid = val.uniqueid
	and vl.lead_id = val.lead_id
	and vl.user = val.user
left join asterisk.vicidial_closer_log vcl on
	vcl.lead_id = val.lead_id
left join asterisk.user_call_log ucl on
	ucl.lead_id = val.lead_id
left join asterisk.vicidial_list vl2  on
	val.lead_id = vl2.lead_id
where
	val.lead_id is not null
	and val.status is not null
	and val.event_time > '%s'
	and val.event_time <= DATE_ADD(NOW() ,INTERVAL -15 minute);
"""

#query PostgreSQL
queryP_in ="""
INSERT INTO bi_snap.gestion_det(
agent_log_id
,usuario
,event_time
,lead_id
,status
,user_group
,campaign_id
,comments
,uniqueid
,pause_type
,phone_number
,Cedula
,server_ip)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

#Conexion PostgreSQL
conexionP = psycopg2.connect(**connP)
cursorP = conexionP.cursor()

#fechas
#SQL20
cursorP.execute(queryP_0)
fecha0 = cursorP.fetchall()

#SQL22
cursorP.execute(queryP_2)
fecha2 = cursorP.fetchall()

#SQL23
cursorP.execute(queryP_3)
fecha3 = cursorP.fetchall()

#ejecuciones SQL20
cursorM1.execute(queryM % fecha0[0][0].strftime('%Y/%m/%d %H:%M:%S'))
anwr = cursorM1.fetchall()

#ejecuciones SQL22
cursorM2.execute(queryM % fecha2[0][0].strftime('%Y/%m/%d %H:%M:%S'))
anwr2 = cursorM2.fetchall()

#ejecuciones SQL23
cursorM3.execute(queryM % fecha3[0][0].strftime('%Y/%m/%d %H:%M:%S'))
anwr3 = cursorM3.fetchall()

anwr = pd.DataFrame(anwr)
anwr2 = pd.DataFrame(anwr2)
anwr3 = pd.DataFrame(anwr3)
fn = pd.DataFrame()

for n in [anwr,anwr2,anwr3]:
	fn = fn.append(n)
    
fn2 = [tuple(x) for x in fn.values]

#cursorP.execute(queryP_in,fn2[0])
#conexionP.commit()

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
            
#close Mysql120
cursorM1.close()
conexionM1.close()
#close Mysql122
cursorM2.close()
conexionM2.close()
#close Mysql123
cursorM3.close()
conexionM3.close()

for x in range(len(fn)):
    #close PostgreSQL
    cursorP.close()
    conexionP.close()