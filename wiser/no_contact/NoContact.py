import mysql.connector
import psycopg2
import pandas as pd

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

#credenciales MySQL124
connM5 = {
	'host' : '10.152.1.124',
	'user':'desarrollo',
	'password':'soportE*8994',
	'database' : 'asterisk'}

#credenciales MySQL209
connM6 = {
	'host' : '10.150.1.209',
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

connT = {
    	'host' : '10.150.1.77',
    	'port' : '5432',
    	'user':'bi',
    	'password':'juanitoMeToco2020',
    	'database' : 'login'}

#query MySQL
query1 = """
select
-1 tarea_id,
b.call_date gestion_fecha,
'VDAD' usuario_id,
a.vendor_lead_code deudor_id,
0 asignacion_id,
b.phone_number telefono,
'Vicidial' canal,
{0} id_tipificacion
FROM vicidial_list a
	,vicidial_log b 
WHERE a.list_id=b.list_id 
and a.lead_id=b.lead_id 
and CAST(b.call_date as date) = CURRENT_DATE()
and b.user='VDAD' 
and b.campaign_id in({1}) 
and b.phone_number<>'';
"""

query2 ="""
select distinct schema_name 
from public.cliente
where schema_name is not null
and schema_name = 'cbpo_propia_wiser';
"""
query3 ="""
select id
from {0}.tipificaciones_herramientas
where herramienta = 'NOCONTACT';
"""
query4 ="""
select campaing_name 
from {0}.campaing_list
where campaing_type is true;
"""

query5 = """
insert into {0}.gestiones
(tarea_id,gestion_fecha,usuario_id,deudor_id,asignacion_id,telefono,canal,id_tipificacion)
values
(%s,%s,%s,%s,%s,%s,%s,%s);
"""

query6 = """
delete
from {0}.gestiones
where gestion_fecha::date = current_date
and usuario_id = 'VDAD';
"""

conn = [connM1,connM2,connM3,connM4,connM6]#,connM5]

for i in pd.read_sql(query2,psycopg2.connect(**connT))['schema_name']:
    conexionP = psycopg2.connect(**connP)
    cursorP = conexionP.cursor()
    cursorP.execute(query6.format(i))
    conexionP.commit()
    cursorP.close()
    conexionP.close()
    for j in conn:
                
        tipi = pd.read_sql(query3.format(i),psycopg2.connect(**connP)).iloc[0,0]
        camp = pd.read_sql(query4.format(i),psycopg2.connect(**connP))
        
        a = str()
        for n in range(len(camp)+1):
            if n > 0 and n < len(camp):
                a = a + "','" + camp.iloc[n,0]
            elif n == 0:
                a = a + "'" + camp.iloc[n,0]
            elif n == len(camp):
                a = a + "'"
        
        conexion = mysql.connector.connect(**j)
        cursor = conexion.cursor()
        cursor.execute(query1.format(tipi,a))
        anwr = cursor.fetchall()
        cursor.close()
        conexion.close()
        
        if anwr != []:
            #insercion
            b=0
            r=0
            for k in range(len(anwr)):
               	try:
                    conexionP = psycopg2.connect(**connP)
                    cursorP = conexionP.cursor ()
                    cursorP.execute(query5.format(i),anwr[k])
                    conexionP.commit()
                    cursorP.close()
                    conexionP.close()
                    b += 1
                    # print(b)
               	except:
                    r += 1
                    print(r)