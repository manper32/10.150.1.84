import pandas as pd
import psycopg2

def GestionesWolkManage():
    connP = {
    	'host' : '10.150.1.77',
    	'port' : '5432',
    	'user':'bi',
    	'password':'juanitoMeToco2020',
    	'database' : 'login'}
    connP_P = {
    	'host' : '10.150.1.74',
    	'port' : '5432',
    	'user':'postgres',
    	'password':'cobrando.bi.2020',
    	'database' : 'postgres'}
    
    query1 = """
    select distinct schema_name 
    from public.cliente
    where schema_name is not null
    and schema_name = 'cbpo_propia_wiser';
    """
    query2 = """
    begin;
    
    insert into {0}.gestiones
    (tarea_id,gestion_fecha,usuario_id,deudor_id,asignacion_id,telefono,canal,id_tipificacion,descripcion)
    (select
    0 tarea_id,
    t1.manage_date gestion_fecha,
    'CHATBOT' usuario_id,
    t1.deudor_id::varchar,
    coalesce(t3.asignacion_id,1) asignacion_id,
    right(t1.phone::varchar,10) telefono,
    'CHATBOT' canal,
    t2.tipificacion_id,
    t1.reason_nopay descripcion
    from {0}.wolk_manage t1
    left join {0}.wolk_status t2
    on case when t1.status = '' then 'SG' else t1.status end = t2.status
    left join (	select t4.asignacion_id
    			from (	select asignacion_id
    							,row_number()
			 					OVER (PARTITION  BY	asignacion_id
								ORDER BY	asignacion_id desc) as ROWNUMBER
    					from {0}.asignaciones
	    				where estado is true)t4
	    		where t4.rownumber = 1)t3
    on t1.deudor_id is not null
    full outer join {0}.gestiones t4
    on t1.deudor_id::varchar = t4.deudor_id
    and t1.manage_date = t4.gestion_fecha
    and t4.canal = 'CHATBOT'
    where t4.deudor_id is null);
    
    end;
    """
    
    #conexion a PostgreSQL produccion
    for i in pd.read_sql(query1,psycopg2.connect(**connP))['schema_name']:
        conexionP = psycopg2.connect(**connP_P)
        cursorP = conexionP.cursor()
        cursorP.execute(query2.format(i))
        conexionP.commit()
        #close PostgreSQL
        cursorP.close()
        conexionP.close()
        
GestionesWolkManage()