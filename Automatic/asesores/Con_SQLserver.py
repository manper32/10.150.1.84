import pyodbc
import psycopg2
from datetime import date
#from datetime import timedelta


today = date.today()
dt = today.strftime('%Y-%m-%d')
# dt = '2020-09-01'

#print(pyodbc.drivers())

#credenciales MySQL
connS = {
    'driver' : 'ODBC Driver 17 for SQL Server',
	'host' : '10.150.1.22',
	'user':'sa',
	'password':'analistadb1020',
	'database' : 'Gestion_Bi'}

#credenciales PostgreSQL cloud
connP_C = {
	'host' : '10.200.50.5',
	'port' : '5432',
	'user':'cord_bi',
	'password':'pepito2019',
	'database' : 'BIWOLKVOX'}

#conexion a SQL server
conexionS = pyodbc.connect(**connS)
cursorS = conexionS.cursor ()
#conexion a PostgreSQL cloud
conexionP_C = psycopg2.connect(**connP_C)
cursorP_C = conexionP_C.cursor ()

#query SQLserver
queryS = """
SELECT 
Usuario,
FechaGestion,
GestionesDia,
Id,
Extension,
FechaSubida as fecha_cargue,
CantCompromiso,
CantContacto 
FROM [Gestion_Bi].[dbo].[GestionPorDia]
where FechaSubida = convert(nvarchar(10),getdate(),3)
order by FechaGestion desc;
"""

#query delete PostgreSQL produccion
queryP_del_C = """
delete from dashboard.gestor_clientes
where fecha_cargue = '"""+ dt +"';"

#query insert PostgreSQL cloud
queryP_in_C ="""
INSERT INTO dashboard.gestor_clientes(
usuario
,fecha_gestion
,clientes
,id
,ext
,fecha_cargue
,compromisos
,contactos) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
"""
#ejecuaciones
cursorS.execute(queryS)
anwr = cursorS.fetchall()

cursorP_C.execute(queryP_del_C)
conexionP_C.commit()

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

#close SQL server
cursorS.close()
conexionS.close()
#close PostgreSQL
cursorP_C.close()
conexionP_C.close()