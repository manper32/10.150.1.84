#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 17:50:12 2020

@author: manuel
"""

import pyodbc
import psycopg2
from datetime import datetime
#from datetime import timedelta


# today = date.today()
# dt = today.strftime('%Y-%m-%d')

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

#credenciales PostgreSQL cloud
connP_C = {
	'host' : '10.200.50.5',
	'port' : '5432',
	'user':'cord_bi',
	'password':'pepito2019',
	'database' : 'BIWOLKVOX'}

#fecha SQLserver
querySF = """
SELECT
max(c.fecha_recepcion_lote)
FROM outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE a.solicitud=b.solicitud
and b.lote=c.lote;
"""
#fecha PSQL
queryPF = """
select max(fecha)
from dashboard.avan_leg_cob
where fecha <= current_date ;
"""
#query SQLserver
queryS = """
select
fecha
,sum(c_asignacion)c_asignacion
,sum(c_gestion)c_gestion 
,SUM(c_lote)c_lote
,sum(c_nd)c_lote
from(
select
coalesce(cast (fecha_asignacion as date),cast(fecha_gestion as date),cast(fecha_lote as date)) fecha
,coalesce(c_asignacion,0) c_asignacion
,coalesce(c_gestion,0) c_gestion
,coalesce(c_lote,0) c_lote
,coalesce(c_nd,0) c_nd
from(
select 
case	when cast(fecha_recepcion_lote as date) = '1900-01-01'
			then null
			else cast(fecha_recepcion_lote as date) 
end fecha_lote
,count(case	when cast(fecha_recepcion_lote as date) = '1900-01-01'
			then null
			else cast(fecha_recepcion_lote as date) 
end) c_lote
FROM outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{0}'
and a.solicitud=b.solicitud
and b.lote=c.lote
group by case	when cast(fecha_recepcion_lote as date) = '1900-01-01'
			then null
			else cast(fecha_recepcion_lote as date) 
end) t1
full outer join (
select 
case	when cast(fechaasignacion as date) = '1900-01-01'
		then null
		else cast(fechaasignacion as date) 
end fecha_asignacion
,count(case	when cast(fechaasignacion as date) = '1900-01-01'
		then null
		else cast(fechaasignacion as date) 
end) c_asignacion
from outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{1}'
and a.solicitud=b.solicitud
and b.lote=c.lote
group by case	when cast(fechaasignacion as date) = '1900-01-01'
		then null
		else cast(fechaasignacion as date) 
end) t2
on cast(t1.fecha_lote as date) = cast(t2.fecha_asignacion as date)
full outer join(
select 
case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date) 
end fecha_gestion
,count(case	when cast(Fecha_Gestion as date) = '1900-01-01'
			then null
			else cast(Fecha_Gestion as date) 
	end) c_gestion
from outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{2}'
and a.solicitud=b.solicitud
and b.lote=c.lote
and ltrim(rtrim(legalizado)) = 'SI'
or a.solicitud=b.solicitud
and b.lote=c.lote
and ltrim(rtrim(legalizado)) is null
group by case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date) 
end) t3
on cast(t1.fecha_lote as date) = cast(t3.fecha_gestion as date)
full outer join(
select 
case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date) 
end fecha_nd
,count(case	when cast(Fecha_Gestion as date) = '1900-01-01'
			then null
			else cast(Fecha_Gestion as date) 
	end) c_nd
from outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{3}'
and a.solicitud=b.solicitud
and b.lote=c.lote
and ltrim(rtrim(legalizado)) in ('ND','NO')
group by case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date)
end)t4
on cast(t1.fecha_lote as date) = cast(t4.fecha_nd as date))t5
where fecha > '{4}'
group by t5.fecha
order by t5.fecha desc;
"""

queryS1 = """
select 
t1.consultor
,t1. canalventa
,cast(t1.Fecha_Gestion as date)
,t1.c_cons
,t4.c_nd
from(
select
upper(Consultor) Consultor
,a.canalventa
,cast(fecha_recepcion_lote as date) FECHA_LOTE
,Fecha_Gestion
,count(*) c_cons
from outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{0}'
and a.solicitud=b.solicitud
and b.lote=c.lote
group by 
upper(Consultor)
,cast(fecha_recepcion_lote as date)
,Fecha_Gestion
,a.Canalventa)t1
left join(
select 
case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date) 
end fecha_nd
,Consultor
,a.Canalventa
,count(case	when cast(Fecha_Gestion as date) = '1900-01-01'
			then null
			else cast(Fecha_Gestion as date) 
	end) c_nd
from outsourcing.dbo.Checklist a,
	outsourcing.dbo.cob_checklist_lote b,
	outsourcing.dbo.cob_lotes c
WHERE fecha_recepcion_lote > '{1}'
and a.solicitud=b.solicitud
and b.lote=c.lote
and ltrim(rtrim(legalizado)) in ('ND','NO')
group by case	when cast(Fecha_Gestion as date) = '1900-01-01'
		then null
		else cast(Fecha_Gestion as date)
end
,Consultor
,a.Canalventa)t4
on cast(t1.Fecha_Gestion as date) = t4.fecha_nd
and t1.Consultor = t4.Consultor
and t1.Canalventa = t4.Canalventa
where cast(t1.Fecha_Gestion as date) > '{2}'
order by 
t1.Fecha_Gestion desc
,t1.Consultor;
"""
# #query delete PostgreSQL produccion
# queryP_del_P = """
# delete from bi_snap.gestor_clientes
# where fecha_cargue = '{}';
# """

# #query delete PostgreSQL produccion
# queryP_del_C = """
# delete from dashboard.gestor_clientes
# where fecha_cargue = '{}';
# """

#query insert PostgreSQL produccion
queryP_in_C ="""
INSERT INTO dashboard.avan_leg_cob(
fecha
,c_asignacion
,c_gestion
,c_lote
,c_nd) VALUES(%s,%s,%s,%s,%s)
"""

#query insert PostgreSQL cloud
queryP_in_C1 ="""
INSERT INTO dashboard.avan_leg_cli(
consultor
,canal
,fecha
,c_reg
,c_nd) VALUES(%s,%s,%s,%s,%s)
"""
#conexion a SQL server
conexionS = pyodbc.connect(**connS)
cursorS = conexionS.cursor()
#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
cursorP_C = conexionP_C.cursor()

#fechaS
cursorS.execute(querySF)
dt = cursorS.fetchall()
# fechaP
cursorP_C.execute(queryPF)
dtP = cursorP_C.fetchall()

if datetime.strptime(dt[0][0],'%Y/%m/%d').date() > dtP[0][0]:
    

    #ejecuaciones
    cursorS.execute(queryS.format(datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')))
    anwr = cursorS.fetchall()
    
    cursorS.execute(queryS1.format(datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')
                                  ,datetime.strftime(dtP[0][0],'%Y/%m/%d')))
    anwr1 = cursorS.fetchall()
    
    # cursorP_P.execute(queryP_del_P)
    # conexionP_P.commit()
    
    # cursorP_C.execute(queryP_del_C)
    # conexionP_C.commit()
    
    # cursorP_C.execute(queryP_in_C,anwr[37])
    # conexionP_C.commit()
    
    #insercion
    b = 0
    r = 0
    for i in range(len(anwr)):
        try:
            cursorP_C.execute(queryP_in_C,anwr[i])
            conexionP_C.commit()
            b += 1
            print(i)
        except:
            r += 1
    #        print(i)
    
    #insercion
    b = 0
    r = 0
    for i in range(len(anwr1)):
        try:
            cursorP_C.execute(queryP_in_C1,anwr1[i])
            conexionP_C.commit()
            b += 1
            print(i)
        except:
            r += 1
    # cursorP_C.execute(queryP_in_C1,anwr1[22826])
    # conexionP_C.commit()
    
else:
    pass

#close SQL server
cursorS.close()
conexionS.close()
#close PostgreSQL
cursorP_C.close()
conexionP_C.close()