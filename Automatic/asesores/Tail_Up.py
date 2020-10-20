#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 09:17:18 2020

@author: manuel
"""

import psycopg2
from datetime import date
#from datetime import timedelta


today = date.today()
dt = today.strftime('%Y-%m-%d')

#print(pyodbc.drivers())
#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#credenciales PostgreSQL cloud
connP_C = {
	'host' : '10.200.50.5',
	'port' : '5432',
	'user':'cord_bi',
	'password':'pepito2019',
	'database' : 'BIWOLKVOX'}

#query SQLserver
queryP_P = """
select 	
	gd.event_time::date
	,gd.usuario 
	,cl.campaing_name
	,u.unit_name
	,c.customer_name
	,case when gd."comments" is null then coalesce(gd.agent_log_id::varchar,'0000') else coalesce(t3.deudor_id::varchar,'0000') end
--	t3.deudor_id
	,count(*)
	,now()--select *
from bi_snap.gestion_det gd
left join bi_snap.campaing_list AS cl on gd.campaign_id = cl.campaing_name
left join bi_snap.units AS u ON u.unit_id = cl.unit_id
left join bi_snap.customers AS c ON c.customer_id = u.customer_id
left join (select
	t2.telefono 
	,t2.deudor_id 
from(
select 
	t1.telefono
	,t1.deudor_id
	,row_number ()
	 			OVER (PARTITION  BY	t1.telefono
				ORDER BY	trim(t1.deudor_id),
								t1.telefono desc) AS ROWNUMBER
from(select distinct 
	telefono
	,deudor_id
from cbpo_bogota.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_carteraok.telefonos
union
select distinct 
	telefono
	,deudor_id 
from  cbpo_claro.telefonos 
union
select distinct 
	telefono
	,deudor_id
from cbpo_codensa.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_colpatria.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_davivienda.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_falabella.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_popular.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_maf.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_progresa.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_propia.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_qnt.telefonos
union
select distinct 
	telefono
	,deudor_id
from cbpo_santander.telefonos) t1
where telefono not in (1000000,1000001,1001001,1010101,1000002,1000010,1000100,1000195,1001010)
and t1.telefono > 1000000
order by telefono)t2
where t2.rownumber = 1)t3
on trim(gd.phone_number)::int8 = t3.telefono
where trim(gd.phone_number) != 'anonymous'
and gd.event_time::date in (current_date,current_date -1)
group by 
	gd.event_time::date
	,gd.usuario 
	,cl.campaing_name
	,u.unit_name
	,c.customer_name
	,case when gd."comments" is null then coalesce(gd.agent_log_id::varchar,'0000') else coalesce(t3.deudor_id::varchar,'0000') end
order by 
	gd.event_time::date desc
	,gd.usuario;
"""

#query insert PostgreSQL cloud
queryP_in_C ="""
INSERT INTO dashboard.gestor_clientes_det(
event_time
,user_id 
,campaing_name 
,unit_name 
,customer_name
,lead_code
,llamadas 
,load_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
"""

#query insert PostgreSQL cloud
queryP_del_C ="""
delete from dashboard.gestor_clientes_det
where event_time::date in (current_date,current_date -1);
"""

#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
cursorP_P = conexionP_P.cursor ()
#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
cursorP_C = conexionP_C.cursor ()

#ejecuaciones
cursorP_P.execute(queryP_P)
anwr = cursorP_P.fetchall()

cursorP_C.execute(queryP_del_C)
conexionP_C.commit()

#cursorP_C.execute(queryP_in_C,anwr[639])
#conexionP_C.commit()

#insercion
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_C.execute(queryP_in_C,anwr[i])
        conexionP_C.commit()
        b += 1
#        print(i)
    except:
        r += 1
#        print(i)
        
#close PostgreSQL
cursorP_P.close()
conexionP_P.close()
#close PostgreSQL
cursorP_C.close()
conexionP_C.close()