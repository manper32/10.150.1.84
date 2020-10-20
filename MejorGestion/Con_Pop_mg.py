#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 16:30:07 2020

@author: manuel
"""

import pyodbc
import psycopg2
from datetime import date
#from datetime import timedelta


today = date.today()
ls = today.strftime('%Y/%m/%d')
li = today.strftime('%Y/%m/') + '01'
#print(pyodbc.drivers())

#credenciales MySQL
connS = {
    'driver' : 'ODBC Driver 17 for SQL Server',
	'host' : '10.150.1.22',
	'user':'sa',
	'password':'analistadb1020',
	'database' : 'Gestion_Bi'}

#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#conexion a SQL server
conexionS = pyodbc.connect(**connS)
#print('\nConexión con el servidor SQLserver establecida!')
cursorS = conexionS.cursor ()
#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL produccion establecida!')
cursorP_P = conexionP_P.cursor ()

#li = "2020/07/01"
#ls = "2020/07/31"

#query SQLserver
queryS = """
set dateformat ymd
declare @li date = '"""+ li +"""'
declare @ls date = '"""+ ls +"""'

select distinct
t1.indice deudor_id,
month(t1.horfingest) mes,
year(t1.horfingest) anio,
t1.indicad indicador,
t2.repeticion,
t3.llamadas,
t3.sms,
t3.correos,
t3.gescall,
t3.whatsapp,
t3.no_contacto,
t1.horfingest fecha_gestion,
t3.visitas,
cast(replace(t1.telefono,',','')as bigint),
t1.asesor,
convert(nvarchar(10),t4.fecha_primer_gestion,3) fecha_primer_gestion,
convert(nvarchar(10),t4.fecha_ultima_gestion,3) fecha_ultima_gestion,
t5.fecha_ultimo_alo,
t6.fecha_primer_alo------
/*upper(t1.desccod01) descdod1,
upper(t1.desccod02) descdod2*/
from(select
row_number ()
	 OVER (PARTITION  BY	rtrim(ltrim(a.indice))
			ORDER BY	rtrim(ltrim(a.indice)),
						b.prioridad,
						a.horfingest desc) AS ROWNUMBER,
ltrim(rtrim(a.indice)) as indice,
a.horfingest,
b.indicad,
b.prioridad,
a.telefono,
a.asesor,
a.feccom,
a.monpac,
a.fila,
b.diagges,
b.desccod01,
b.desccod02,
b.indice indi,
a.codemp codep
from banco_popular.[dbo].[cob_gestrim] a,
banco_popular.[dbo].[cob_validacodt] b
where	a.val_indice=b.indice
and  a.horfingest between @li and @ls
and a.fila=1 
and a.indice <> '') t1
left join (	select LTRIM(RTRIM(c.indice)) indice
					,d.prioridad
					,count(*) repeticion
			from	banco_popular.[dbo].[cob_gestrim] c,
					banco_popular.[dbo].[cob_validacodt] d 
			where	c.val_indice=d.indice
					and c.horfingest between @li and @ls
								and c.fila=1 
								and c.indice <> ''
			group by LTRIM(RTRIM(c.indice)) 
					,d.prioridad) t2
on	t2.indice = t1.indice
	and t2.prioridad = t1.prioridad 
left join (select	LTRIM(RTRIM(indice)) indice
					,sum(case when ltrim(rtrim(asesor)) not in ('BACKEMAIL','BACKGESCAL','BACKSMS','CALLELITE','BACKCARTA','VIRTUAL') 
						then 1 else 0 end) llamadas
					,sum(case when ltrim(rtrim(asesor)) in ('BACKSMS') 
						then 1 else 0 end) sms
					,sum(case when ltrim(rtrim(asesor)) in ('BACKEMAIL') 
						then 1 else 0 end) correos
					,sum(case when ltrim(rtrim(asesor)) in ('BACKGESCAL') 
						then 1 else 0 end) gescall
					,null whatsapp
					,sum(case when ltrim(rtrim(asesor)) in ('CALLELITE') 
						then 1 else 0 end) no_contacto
					,sum(case when ltrim(rtrim(asesor)) in ('VIRTUAL') 
						then 1 else 0 end) visitas
			from banco_popular.[dbo].[cob_gestrim]
			where horfingest between @li and @ls
			and fila=1 
			and indice <> ''
			group by LTRIM(RTRIM(indice)))t3
on t1.indice = t3.indice 
left join (	select indice,
				max(horfingest) fecha_ultima_gestion,
				min(horfingest) fecha_primer_gestion
			from banco_popular.dbo.cob_gestrim
			where horfingest between @li and @ls
			group by indice)t4
on t1.indice = t4.indice
left join (	select	ltrim(rtrim(indice)) indice ,
					max(horfingest) fecha_ultimo_alo
			from	banco_popular.dbo.cob_gestrim
			where	cast(horfingest as date) between @li and @ls
					and ltrim(rtrim(val_indicad)) not in ('MENSAJE','NO CONTACTADO','')
			group by ltrim(rtrim(indice)))t5
on t1.indice = t5.indice
left join (select ltrim(rtrim(indice)) indice ,
					min(horfingest) fecha_primer_alo
			from	banco_popular.dbo.cob_gestrim
			where	cast(horfingest as date) between @li and @ls
					and ltrim(rtrim(val_indicad)) not in ('MENSAJE','NO CONTACTADO','')
			group by ltrim(rtrim(indice)))t6
on t1.indice = t6.indice
where 
t1.horfingest between @li and @ls
and t1.fila=1 
and t1.indice<>''
and t1.ROWNUMBER = 1;
"""

#query delete PostgreSQL produccion
queryP_del_P = """
delete from cbpo_popular.mejor_gestion
where fecha_gestion::date >= '"""+ li +"';"


#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_popular.mejor_gestion(
deudor_id
,mes
,anio
,indicador
,repeticion
,llamadas
,sms
,correos
,gescall
,whatsapp
,no_contacto
,fecha_gestion
,visitas
,phone
,asesor
,fecha_primer_gestion
,fecha_ultima_gestion
,ultimo_alo
,primer_alo) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

#ejecuaciones
cursorS.execute(queryS)
anwr = cursorS.fetchall()

cursorP_P.execute(queryP_del_P)
conexionP_P.commit()

#cursorP_P.execute(queryP_in_P,anwr[0])
#conexionP_P.commit()

#insercion
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_P.execute(queryP_in_P,anwr[i])
        conexionP_P.commit()
        b += 1
    except:
        r += 1
#        print(i)

#print("se pudieron cargar produccion : ",b)
#print("no se pudieron cargar produccion : ",r)


#close SQL server
cursorS.close()
conexionS.close()

for x in range(len(anwr)):
    #close PostgreSQL
    cursorP_P.close()
    conexionP_P.close()
