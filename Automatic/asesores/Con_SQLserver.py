import pyodbc
import psycopg2
from datetime import date
#from datetime import timedelta


today = date.today()
dt = today.strftime('%Y-%m-%d')

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

#conexion a SQL server
conexionS = pyodbc.connect(**connS)
#print('\nConexión con el servidor SQLserver establecida!')
cursorS = conexionS.cursor ()
#conexion a PostgreSQL produccion
conexionP_P = psycopg2.connect(**connP_P)
#print('\nConexión con el servidor PostgreSQL produccion establecida!')
cursorP_P = conexionP_P.cursor ()
#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
#print('\nConexión con el servidor PostgreSQL cloud establecida!')
cursorP_C = conexionP_C.cursor ()

#query SQLserver
queryS = """
SELECT Usuario,FechaGestion,GestionesDia,Id,Extension,FechaSubida as fecha_cargue--Usuario,COUNT(*)--
FROM [Gestion_Bi].[dbo].[GestionPorDia]
where FechaSubida = convert(nvarchar(10),getdate(),3)
order by FechaGestion desc;
"""
#query delete PostgreSQL produccion
queryP_del_P = """
delete from bi_snap.gestor_clientes
where fecha_cargue = '"""+ dt +"';"

#query delete PostgreSQL produccion
queryP_del_C = """
delete from dashboard.gestor_clientes
where fecha_cargue = '"""+ dt +"';"

#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO bi_snap.gestor_clientes(
usuario
,fecha_gestion
,clientes
,id
,ext
,fecha_cargue) VALUES(%s,%s,%s,%s,%s,%s)
"""

#query insert PostgreSQL cloud
queryP_in_C ="""
INSERT INTO dashboard.gestor_clientes(
usuario
,fecha_gestion
,clientes
,id
,ext
,fecha_cargue) VALUES(%s,%s,%s,%s,%s,%s)
"""
#ejecuaciones
cursorS.execute(queryS)
anwr = cursorS.fetchall()

cursorP_P.execute(queryP_del_P)
conexionP_P.commit()

cursorP_C.execute(queryP_del_C)
conexionP_C.commit()

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

#insercion
b = 0
r = 0
for i in range(len(anwr)):
    try:
        cursorP_C.execute(queryP_in_C,anwr[i])
        conexionP_C.commit()
        b += 1
    except:
        r += 1
#        print(i)

#print("se pudieron cargar cloud: ",b)
#print("no se pudieron cargar cloud: ",r)


#close SQL server
cursorS.close()
conexionS.close()
#close PostgreSQL
cursorP_P.close()
conexionP_P.close()
#close PostgreSQL
cursorP_C.close()
conexionP_C.close()