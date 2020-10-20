import psycopg2
from datetime import date
from datetime import timedelta


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

#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL establecida!')
cursorP_P = conexionP_P.cursor ()

#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
#print('\nConexión con el servidor PostgreSQL establecida!')
cursorP_C = conexionP_C.cursor ()

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
from	bi_snap.prod_m
where 	unit_id is not null
		and fec in (now()::DATE,now()::DATE-1)
group by	user_id
			,fec
			,unit_name
			,unit_id
			,customer_name 
			,full_name 
order by 	count(*) desc;
"""

#query delete PostgreSQL cloud
queryP_delC ="""
delete from dashboard.indicators
where fec >= '"""+ dt +"';"

#query delete PostgreSQL cloud 0 CALLS
queryP_delC0 ="""
delete from dashboard.indicators
where calls = 0;
"""

#query insert PostgreSQL cloud
queryP_inC ="""
INSERT INTO dashboard.indicators(
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
,dead_sec) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

cursorP_C.execute(queryP_delC)
conexionP_C.commit()

cursorP_P.execute(queryP_data)
anwr = cursorP_P.fetchall()

#cursorP_C.execute(queryP_inC,anwr[0])
#conexionP_C.commit()

#insercion cloud
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_C.execute(queryP_inC,anwr[i])
        conexionP_C.commit()
        b += 1
    except:
        r += 1
#        print(i)

#print("se pudieron cargar en cloud : ",b)
#print("no se pudieron cargar en cloud : ",r)

cursorP_C.execute(queryP_delC0)
conexionP_C.commit()

for x in range(len(anwr)):
    #close PostgreSQL produccion
    cursorP_P.close()
    conexionP_P.close()
    #close PostgreSQL cloud
    cursorP_C.close()
    conexionP_C.close()