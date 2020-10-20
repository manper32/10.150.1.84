import mysql.connector 
import psycopg2

#credenciales MySQL
connM = {
	'host' : '10.150.1.122',
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

#Conexion MySQL
conexionM = mysql.connector.connect(**connM)
#print('\nConexión con el servidor MySQL establecida!')
cursorM = conexionM.cursor ()

#Conexion PostgreSQL
conexionP = psycopg2.connect(**connP)
#print('\nConexión con el servidor PostgreSQL establecida!')
cursorP = conexionP.cursor ()

#query MySQL
queryM = """
	SELECT
	cast(val.agent_log_id as integer) agent_log_id,
	val.`user` user_id,
	vu.full_name,
	val.server_ip,
	date(val.event_time) as fec,
	time(val.event_time) as tim,
	cast(val.lead_id as integer) lead_id,
	val.campaign_id,
	cast(val.pause_sec as integer) pause_sec,
	cast(val.wait_sec as integer) wait_sec,
	cast(val.talk_sec as integer) talk_sec,
	cast(val.dispo_sec as integer) dispo_sec,
	val.status,
	val.user_group,
	val.sub_status,
	val.dead_sec,
	val.uniqueid,
	val.pause_type,
	SYSDATE() fecha_cargue
FROM
	asterisk.vicidial_agent_log as val
inner join asterisk.vicidial_users as vu on
	vu.`user` = val.`user`
where
	date(val.event_time) = date_sub(curdate(),interval 1 day);
"""

queryP_in ="""
INSERT INTO bi_snap.call_log(
agent_log_id
,user_id
,full_name
,server_ip
,fec
,tim
,lead_id
,campaign_id
,pause_sec
,wait_sec
,talk_sec
,dispo_sec
,status
,user_group
,sub_status
,dead_sec
,uniqueid
,pause_type
,fecha_cargue) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
queryP_del = """
delete from bi_snap.call_log
where fec = now()::DATE-1
and server_ip = '10.152.1.122';
"""
cursorM.execute(queryM)
anwr = cursorM.fetchall()

cursorP.execute(queryP_del)
conexionP.commit()

b=0;
r=0;
for i in range(len(anwr)):
	try:
		cursorP.execute(queryP_in,anwr[i])
		conexionP.commit()
		b += 1
#		print(i)
	except:
		r += 1

#print("se pudieron cargar : ",b)
#print("no se pudieron cargar : ",r)
#close Mysql
cursorM.close()
conexionM.close()
#close PostgreSQL
cursorP.close()
conexionP.close()