import psycopg2

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

#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL establecida!')
cursorP_P = conexionP_P.cursor ()

#conexion a PostgreSQL cloud
#conexionP_C = psycopg2.connect(**connP_C)
#print('\nConexión con el servidor PostgreSQL establecida!')
#cursorP_C = conexionP_C.cursor ()

queryP_data = """
select distinct 
t1.user_id
,t1.full_name
,t1.fec 
,t4.customer_name 
,t3.unit_name
,t3.tipo_cartera
,t1.lead_id 
,t1.status
,t5.contactability
,t5.effectiveness
,t2.unit_id 
,t1.agent_log_id 
,t1.pause_sec
,t1.wait_sec
,t1.talk_sec
,t1.dispo_sec
,t1.dead_sec
,t1.sub_status
,t1.server_ip
from bi_snap.call_log t1
left join bi_snap.campaing_list t2
on t2.campaing_name = t1.campaign_id 
left join bi_snap.units t3 
on t2.unit_id = t3.unit_id
left join bi_snap.customers t4 
on t3.customer_id = t4.customer_id
left join bi_snap.management_indicators t5  
on t5.indicator_cod = t1.status
FULL OUTER join bi_snap.prod_m t6 
on t1.agent_log_id = t6.agent_log_id
where t4.customer_id <> 13
and t6.agent_log_id is null ;
"""
#query delete PostgreSQL produccion
queryP_delP ="""
delete from bi_snap.prod_m 
where fec in (now()::DATE,now()::DATE-1)
and customer_name not like '%VENTAS%';
"""
#query delete PostgreSQL cloud
#queryP_delC ="""
#delete from dashboard.prod_m 
#where fec = now()::DATE
#and customer_name not like '%VENTAS%';
#"""

#query insert PostgreSQL produccion
queryP_inP ="""
INSERT INTO bi_snap.prod_m(
user_id
,full_name
,fec
,customer_name
,unit_name
,tipo_cartera
,lead_id
,status
,contactability
,effectiveness
,unit_id
,agent_log_id
,pause_sec
,wait_sec
,talk_sec
,dispo_sec
,dead_sec
,sub_status
,server_ip) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
#query insert PostgreSQL produccion
queryP_inC ="""
INSERT INTO dashboard.prod_m(
user_id
,full_name
,fec
,customer_name
,unit_name
,tipo_cartera
,lead_id
,status
,contactability
,effectiveness
,unit_id
,agent_log_id
,pause_sec
,wait_sec
,talk_sec
,dispo_sec
,dead_sec
,sub_status
,server_ip) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

cursorP_P.execute(queryP_delP)
conexionP_P.commit()

#cursorP_C.execute(queryP_delC)
#conexionP_C.commit()

cursorP_P.execute(queryP_data)
anwr = cursorP_P.fetchall()

#cursorP.execute(queryP_in,anwr[0])
#conexionP.commit()

#insercion produccion
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_P.execute(queryP_inP,anwr[i])
        conexionP_P.commit()
        b += 1
    except:
        r += 1
#        print(i)

#print("se pudieron cargar en produccion : ",b)
#print("no se pudieron cargar en produccion : ",r)

#insercion cloud
#b = 0
#r = 0
#for i in range(len(anwr)):
#    try:
#        cursorP_C.execute(queryP_inC,anwr[i])
#        conexionP_C.commit()
#        b += 1
#    except:
#        r += 1
#        print(i)

#print("se pudieron cargar en cloud : ",b)
#print("no se pudieron cargar en cloud : ",r)
for x in range(len(anwr)):
    #close PostgreSQL produccion
    cursorP_P.close()
    conexionP_P.close()
    #close PostgreSQL cloud
#    cursorP_C.close()
#    conexionP_C.close()