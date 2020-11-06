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

#credenciales MySQL206
connM4 = {
	'host' : '10.150.1.206',
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
queryM = """
SELECT DISTINCT 
    vl.call_date,
    vl.phone_number,
    case when LENGTH(vl.phone_number) = 10 then 'Movil' else 'Fijo' end as tipo_telefono,
    vl.status,
    vl.user,
    vl.campaign_id
/*    ,FROM_UNIXTIME(vl.end_epoch)
    ,FROM_UNIXTIME(vl.start_epoch)*/
	,case 	when vl.end_epoch is null or vl.start_epoch is null then 0 
			else TIMESTAMPDIFF(SECOND,FROM_UNIXTIME(vl.start_epoch),FROM_UNIXTIME(vl.end_epoch)) end dur
FROM
    asterisk.vicidial_log AS vl
WHERE
	cast(vl.call_date as date) = date_add(current_date(), INTERVAL -1 DAY)
/*	cast(vl.call_date as date) BETWEEN '2020-09-03' and date_add(current_date(), INTERVAL -1 DAY)*/
 	and LENGTH(vl.phone_number) <= 10
order by 
call_date desc,phone_number
"""
#where 	cast(vl.call_date as date) = date_add(current_date(), INTERVAL -1 DAY)
#query MySQL
queryM1 = """
SELECT DISTINCT
        vl.call_date,
        case when vl.campaign_id not like '%GESCA%' and t2.vendor_lead_code is null 
	    then vl.phone_number
		when vl.campaign_id like '%GESCA%' 
	    then vl.phone_number
	    else t2.vendor_lead_code end tel,
	    case when length(case when vl.campaign_id not like '%GESCA%' and t2.vendor_lead_code is null 
		    then vl.phone_number
			when vl.campaign_id like '%GESCA%' 
		    then vl.phone_number
		    else t2.vendor_lead_code end) >= 10 
		and SUBSTRING((case when vl.campaign_id not like '%GESCA%' and t2.vendor_lead_code is null 
		    then vl.phone_number
			when vl.campaign_id like '%GESCA%' 
		    then vl.phone_number
		    else t2.vendor_lead_code end),1,2) like '%3%'
		then 'Movil'
		else 'Fijo' end tipo,
    vl.status,
    vl.user,
    vl.campaign_id
	,case 	when vl.end_epoch is null or vl.start_epoch is null then 0 
			else TIMESTAMPDIFF(SECOND,FROM_UNIXTIME(vl.start_epoch),FROM_UNIXTIME(vl.end_epoch)) end dur
FROM
    asterisk.vicidial_log AS vl
left join 	asterisk.vicidial_list t2
on  		vl.phone_number = t2.phone_number
and 		vl.lead_id = t2.lead_id
WHERE
	cast(vl.call_date as date) = date_add(current_date(), INTERVAL -1 DAY)
	/*cast(vl.call_date as date) BETWEEN '2020-09-03' and date_add(current_date(), INTERVAL -1 DAY)*/
/*and vl.campaign_id like '%GESCALL%'*/
order by 
vl.call_date desc,vl.phone_number;
"""

#query PostgreSQL
queryP_in ="""
INSERT INTO bi_snap.minutos_vicidial(
fecha
,telefono
,tipo_telefono
,status
,usuario
,campaign
,duracion) VALUES(%s,%s,%s,%s,%s,%s,%s)
"""

#query PostgreSQL
queryP ="""
begin;

truncate table bi_snap.minutos_sec;

insert into bi_snap.minutos_sec
select 	t4.customer_name
		,t3.unit_name 
		,t1.fecha::date date_sec
		,t1.tipo_telefono
		,sum(case 	when t1.status = 'AA'and t1.duracion = 0
					then 1 
					else t1.duracion
			end) seconds --select *
from 	bi_snap.minutos_vicidial t1
left join bi_snap.campaing_list t2
on t1.campaign = t2.campaing_name--select * from bi_snap.customers
left join bi_snap.units t3
on t2.unit_id = t3.unit_id
left join bi_snap.customers t4
on t3.customer_id = t4.customer_id 
where 	fecha >= '2020/09/03 00:00:00'
and upper(campaign) not like '%IN%'
and campaign != ''
and t4.customer_name is not null
group by t4.customer_name ,t3.unit_name ,t1.fecha::date,t1.tipo_telefono 
order by t1.fecha::date desc,t3.unit_name;

end;
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

#Conexion MySQL206
conexionM4 = mysql.connector.connect(**connM4)
cursorM4 = conexionM4.cursor()

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
cursorM4.execute(queryM1)
anwr4 = cursorM4.fetchall()

anwr = pd.DataFrame(anwr)
anwr2 = pd.DataFrame(anwr2)
anwr3 = pd.DataFrame(anwr3)
anwr4 = pd.DataFrame(anwr4)
fn = pd.DataFrame()
for n in [anwr,anwr2,anwr3,anwr4]:
	fn = fn.append(n)
#fn2 = anwr4
fn2 = [tuple(x) for x in fn.values]

#cursorP.execute(queryP_in,fn2[0])
#conexionP.commit()

#insercion
b=0;
r=0;
for i in range(len(fn2)):
    	try:
            cursorP.execute(queryP_in,fn2[i])
            conexionP.commit()
            b += 1
#            print(b)
    	except:
            r += 1
#            print(r)
            
cursorP.execute(queryP)
conexionP.commit()

for x in range(len(anwr)+len(anwr2)+len(anwr3)+len(anwr4)):
    #close Mysql
    cursorM1.close()
    conexionM1.close()
    #close Mysql
    cursorM2.close()
    conexionM2.close()
    #close Mysql
    cursorM3.close()
    conexionM3.close()
    #close PostgreSQL
    cursorP.close()
    conexionP.close()