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

#conexion a SQL server
conexionS = pyodbc.connect(**connS)
#print('\nConexión con el servidor SQLserver establecida!')
cursorS = conexionS.cursor ()
#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL produccion establecida!')
cursorP_P = conexionP_P.cursor ()

#li = "2020/06/01"
#ls = "2020/06/30"

#query SQLserver
queryS = """
set dateformat ymd
declare @li date = '"""+ ls +"""'

select distinct
t1.indice deudor_id,
month(t1.horfingest) mes,
t1.indicador,
t2.repeticion,
t3.llamadas,
t3.sms,
t3.correos,
t3.gescall,
t3.whatsapp,
t3.no_contacto,
t1.horfingest fecha_gestion,
t3.visitas,
cast(LEFT(SUBSTRING(t1.telefono, PATINDEX('%[0-9.-]%', t1.telefono), 8000),
PATINDEX('%[^0-9.-]%', SUBSTRING(t1.telefono, PATINDEX('%[0-9.-]%', t1.telefono), 8000) + 'X') -1) as bigint) telefono,
t1.asesor
from(SELECT	row_number ()
				OVER (PARTITION  BY	rtrim(ltrim(a.indice))
				ORDER BY	rtrim(ltrim(a.indice)),
							b.prioridad,
							a.horfingest desc) AS ROWNUMBER
				,rtrim(a.indice) as indice
				,a.horfingest 
				,b.indicador 
				,a.prioridad
				,a.telefono
				,a.asesor
				,a.feccom 
				,a.monpac 
				,b.descod 
				,a.fecges
				,a.codemp
				FROM [CarteraPropia].[dbo].[cob_gestion_COB2] a
	,[CarteraPropia].[dbo].[cob_contacto] b 
where a.cod2=b.cod 
	and a.codemp = 'COB2' 
	and cast(a.horfingest as date) = @li
	and a.indice <> '')t1
left join (select LTRIM(RTRIM(c.indice)) indice
					,d.prioridad
					,count(*) repeticion
			from	[CarteraPropia].[dbo].[cob_gestion_COB2] c
					,[CarteraPropia].[dbo].[cob_contacto] d 
			where c.cod2 = d.cod 
				and c.codemp = 'COB2' 
				and  cast(c.horfingest as date) = @li
				and c.indice <> ''
			group by LTRIM(RTRIM(c.indice)) 
					,d.prioridad)t2
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
		from [CarteraPropia].[dbo].[cob_gestion_COB2]
		where codemp = 'COB2' 
		and  cast(horfingest as date) = @li
		and indice <> ''
		group by LTRIM(RTRIM(indice)))t3
on t1.indice = t3.indice
left join (	select indice,
				max(horfingest) fecha_ultima_gestion,
				min(horfingest) fecha_primer_gestion
			from [CarteraPropia].[dbo].[cob_gestion_COB2]
			where cast(horfingest as date) = @li
			group by indice)t4
on t1.indice = t4.indice
where	t1.codemp = 'COB2' 
		and  cast(horfingest as date) = @li
		and t1.indice <> ''
		and t1.ROWNUMBER = 1;
"""

#query delete PostgreSQL produccion
queryP_del_P = """
truncate table cbpo_propia.mejor_gestion_dia ;
"""


#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_propia.mejor_gestion_dia(
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
