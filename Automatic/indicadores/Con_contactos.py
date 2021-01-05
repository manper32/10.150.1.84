import psycopg2
from datetime import date
from datetime import timedelta

# cartera
today = date.today()-timedelta(days=1)
dt = today.strftime('%Y-%m-%d')

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

#credenciales PostgreSQL test
connP_T = {
	'host' : '10.150.1.77',
	'port' : '5432',
	'user':'bi',
	'password':'juanitoMeToco2020',
	'database' : 'login'}

#query indicador prodiccion
queryP_data = """
select	unit_id
		,user_id
		,full_name 
		,unit_name
		,customer_name 
		,fec 
		,sum (case when (contactability = '1' and status  not in 
			('ADC','DISPO','DONEM','INCALL','XFER')) 
			then 1 else 0 end) contacts 
		,sum(case when status in ('CP','NG')
			then 1 else 0 end) commitment_port
		,sum(case when status in ('AC','AG','INT')
			then 1 else 0 end) commitment_sales
		,sum(case when status  not in 
			('ADC','DISPO','DONEM','INCALL','XFER')
			then 1 else 0 end)  calls
		,sum(pause_sec) pause_sec
		,sum(case when (sub_status = '1') 
			then pause_sec else 0 end) break
		,sum(case when (sub_status = '2') 
			then pause_sec else 0 end) active_pause
		,sum(case when (sub_status = '3') 
			then pause_sec else 0 end) restroom			
		,sum(case when (sub_status = '5') 
			then pause_sec else 0 end) failure
		,sum(case when (sub_status = '7') 
			then pause_sec else 0 end) cons_coord
		,sum(case when (sub_status = '9') 
			then pause_sec else 0 end) lunch
		,sum(case when (sub_status = '10') 
			then pause_sec else 0 end) whatsapp
		,sum(case when (sub_status = '11') 
			then pause_sec else 0 end) front
		,sum(case when (sub_status = '12') 
			then pause_sec else 0 end) folder
		,sum(case when (sub_status = '4') 
			then pause_sec else 0 end) training
		,sum(case when (sub_status = '6') 
			then pause_sec else 0 end) feedback
		,sum(case when (sub_status = '8') 
			then pause_sec else 0 end) marking
		,sum(case when (sub_status = 'NXDIAL') 
			then pause_sec else 0 end) NXDIAL
		,sum(case when (sub_status = 'CO') 
			then pause_sec else 0 end) CO
		,sum(case when (sub_status = 'LOGIN') 
			then pause_sec else 0 end) pause
		,sum(case when (sub_status = 'LAGGED') 
			then pause_sec else 0 end) delay
		,sum(case when (sub_status is null) 
			then pause_sec else 0 end) typing
		,sum(wait_sec ) wait_sec
		,sum(talk_sec  ) talk_sec
		,sum(dispo_sec ) dispo_sec
		,sum(dead_sec  ) dead_sec
from (select distinct 
cl.user_id
,cl.full_name
,cl.fec
,c.customer_name
,pl.productivity AS unit_name
,pl.tipo_cartera 
,cl.lead_id
,cl.status
,mi.contactability
,mi.effectiveness
,ul.productivity_id as unit_id
,cl.agent_log_id
,cl.pause_sec
,cl.wait_sec
,cl.talk_sec
,cl.dispo_sec
,cl.dead_sec
,cl.sub_status
,cl.server_ip
from bi_snap.call_log AS cl
LEFT JOIN bi_snap.users_log AS ul ON ul.extension_id::TEXT = cl.user_id::TEXT 
AND cl.fec >= ul.start_date 
AND cl.fec <= ul.end_date 
LEFT JOIN bi_snap.productivities_list AS pl ON pl.productivity_id = ul.productivity_id
LEFT JOIN bi_snap.customers AS c ON c.customer_id = pl.customer_id 
left join bi_snap.management_indicators AS mi on mi.indicator_cod = cl.status
/*FULL OUTER join bi_snap.prod_m AS al on cl.agent_log_id = al.agent_log_id*/
where c.customer_id <> 13
and fec in (now()::DATE,now()::DATE-1)
/*and al.agent_log_id is null */)t1	
--bi_snap.prod_m
where 	unit_id is not null
		and fec in (now()::DATE,now()::DATE-1)
group by	user_id
			,fec
			,unit_name
			,unit_id
			,customer_name 
			,full_name 
order by 	fec desc,
			sum(case when status  not in 
			('ADC','DISPO','DONEM','INCALL','XFER')
			then 1 else 0 end) desc;
"""

#query delete PostgreSQL cloud
queryP_delC ="""
delete from {0}.indicators
where fec >= '"""+ dt +"';"

#query delete PostgreSQL cloud 0 CALLS
queryP_delC0 ="""
delete from {0}.indicators
where calls = 0;
"""

#query insert PostgreSQL cloud
queryP_inC ="""
INSERT INTO {0}.indicators(
unit_id
,user_id
,full_name
,unit_name
,customer_name
,fec
,contacts
,commitment_port
,commitment_sales
,calls
,pause_sec
,break
,active_pause
,restroom
,failure
,cons_coord
,lunch
,whatsapp
,front
,folder
,training
,feedback
,marking
,nxdial
,co
,pause
,delay
,typing
,wait_sec
,talk_sec
,dispo_sec
,dead_sec) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
cursorP_P = conexionP_P.cursor ()

#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
cursorP_C = conexionP_C.cursor ()

#conexion a PostgreSQL test
conexionP_T = psycopg2.connect(**connP_T)
cursorP_T = conexionP_T.cursor ()

cursorP_C.execute(queryP_delC.format('dashboard'))
conexionP_C.commit()
cursorP_T.execute(queryP_delC.format('public'))
conexionP_T.commit()

cursorP_P.execute(queryP_data)
anwr = cursorP_P.fetchall()

#cursorP_C.execute(queryP_inC,anwr[0])
#conexionP_C.commit()

#insercion cloud
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_C.execute(queryP_inC.format('dashboard'),anwr[i])
        conexionP_C.commit()
        b += 1
    except:
        r += 1
#        print(i)

#insercion cloud
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_T.execute(queryP_inC.format('public'),anwr[i])
        conexionP_T.commit()
        b += 1
    except:
        r += 1
#        print(i)

cursorP_C.execute(queryP_delC0.format('dashboard'))
conexionP_C.commit()
cursorP_T.execute(queryP_delC0.format('public'))
conexionP_T.commit()

for x in range(len(anwr)):
    #close PostgreSQL produccion
    cursorP_P.close()
    conexionP_P.close()
    #close PostgreSQL cloud
    cursorP_C.close()
    conexionP_C.close()