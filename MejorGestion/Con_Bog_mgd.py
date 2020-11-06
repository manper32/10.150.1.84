#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 12:35:38 2020

@author: manuel
"""

import pyodbc
import psycopg2
from datetime import date
#from datetime import timedelta


today = date.today()
ls = today.strftime('%Y/%m/%d')
#li = today.strftime('%Y/%m/') + '01'
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

#li = "2020/06/01"
#ls = "2020/06/30"

#query SQLserver
queryS = """
set dateformat ymd
declare @li date = '"""+ ls +"""'

select 
t1.indice deudor_id,
day(t1.horfingest) mes,
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
t1.asesor
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
a.codemp
from banco_bogota.[dbo].[cob_gestrim] a,
banco_bogota.[dbo].[cob_validacodt] b
where	a.val_indice=b.indice
and  a.horfingest = @li
and a.fila=1 
and a.indice <> '') t1
left join (	select LTRIM(RTRIM(c.indice)) indice
					,d.prioridad
					,count(*) repeticion
			from	banco_bogota.[dbo].[cob_gestrim] c,
					banco_bogota.[dbo].[cob_validacodt] d 
			where	c.val_indice=d.indice
					and c.horfingest = @li
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
			from banco_bogota.[dbo].[cob_gestrim]
			where horfingest = @li
			and fila=1 
			and indice <> ''
			group by LTRIM(RTRIM(indice)))t3
on t1.indice = t3.indice 
left join (	select indice,
				max(horfingest) fecha_ultima_gestion,
				min(horfingest) fecha_primer_gestion
			from banco_bogota.dbo.cob_gestrim
			where horfingest = @li
			group by indice)t4
on t1.indice = t4.indice
where 
t1.horfingest = @li
and t1.fila=1 
and t1.indice<>''
and t1.ROWNUMBER = 1;
"""

#query delete PostgreSQL produccion
queryP_del_P = """
truncate table cbpo_bogota.mejor_gestion_dia ;
"""


#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_bogota.mejor_gestion_dia(
deudor_id
,dia
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
,asesor) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
#conexion a SQL server
conexionS = pyodbc.connect(**connS)
cursorS = conexionS.cursor ()
#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
cursorP_P = conexionP_P.cursor ()

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

#close SQL server
cursorS.close()
conexionS.close()

for x in range(len(anwr)):
    #close PostgreSQL
    cursorP_P.close()
    conexionP_P.close()
