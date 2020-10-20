from datetime import datetime
from datetime import timedelta
from datetime import date
import pandas as pd
import requests
import psycopg2
import json

yesterday = (date.today() - timedelta(days = 1)).strftime("%Y%m%d000000")
today = datetime.now().strftime("%Y%m%d000000")
#credenciales PostgreSQL produccion
connP_P = {
	'host' : '10.150.1.74',
	'port' : '5432',
	'user':'postgres',
	'password':'cobrando.bi.2020',
	'database' : 'postgres'}

#query insert PostgreSQL produccion
queryP_in_P ="""
INSERT INTO cbpo_propia.wolk_chats(
chat_id,
channel,
chat_date,
user_name,
user_email,
user_phone,
user_chat_chars,
agent_id,
agent_name,
agent_chat_chars,
chat_duration,
cod_act,
comment,
id_customer,
agent_skill,
user_id,
sentiment)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

#query insert PostgreSQL produccion
queryP_in_P1 ="""
INSERT INTO cbpo_propia.wolk_conv(
chat_id,
channel,
from_msg,
from_,
to_,
time,
msg,
sentiment)VALUES(%s,%s,%s,%s,%s,%s,%s,%s);
"""

#query insert PostgreSQL produccion
queryP_in_P2 ="""
INSERT INTO cbpo_propia.wolk_cbot(
id_chat,
channel,
routing_point,
date,
cust_name,
cust_email,
cust_phone,
cust_query,
chatbot_answer)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

#cobrando-propia
urlchat =   'http://35.184.156.46/ipdialbox/api_reports.php?token=7b69645f6469737472697d2d3230323030393135313331303130&report=chats&'\
            +'date_ini='+yesterday+'&date_end='+today
urlconv =   'http://35.184.156.46/ipdialbox/api_reports.php?token=7b69645f6469737472697d2d3230323030393135313331303130&report=chats_conversations&'\
            +'date_ini='+yesterday+'&date_end='+today
urlcbot =   'http://35.184.156.46/ipdialbox/api_reports.php?token=7b69645f6469737472697d2d3230323030393135313331303130&report=chats_bots&'\
            +'date_ini='+yesterday+'&date_end='+today

#conexion a PostgreSQL produccion
conexionP = psycopg2.connect(**connP_P)
cursorP = conexionP.cursor()

chat = json.loads(requests.get(urlchat).content)
conv = json.loads(requests.get(urlconv).content)
cbot = json.loads(requests.get(urlcbot).content)
#.encode("latin1")

try:
    chat1 = pd.DataFrame([dic.values() for dic in chat], columns=chat[0].keys())
    chat1['user_phone'] = chat1['user_phone'].replace("",0,inplace=False)
    chat1 = list(chat1.itertuples(index=False))
    ###insercion
    b = 0
    r = 0
    for i in range(len(chat1)):
        try:
            cursorP.execute(queryP_in_P,chat1[i])
            conexionP.commit()
            b += 1
        except:
            r += 1
    #        print(i)
    #        break
except:
    pass

try:
    conv1 = pd.DataFrame([dic.values() for dic in conv], columns=conv[0].keys())
    conv1 = list(conv1.itertuples(index=False))
    ##insercion
    b = 0
    r = 0
    for i in range(len(conv1)):
        try:
            cursorP.execute(queryP_in_P1,conv1[i])
            conexionP.commit()
            b += 1
        except:
            r += 1
    #        print(i)
    #        break
except:
    pass

try:
    cbot1 = pd.DataFrame([dic.values() for dic in cbot], columns=cbot[0].keys())
    cbot1 = list(cbot1.itertuples(index=False))
    #insercion
    b = 0
    r = 0
    for i in range(len(cbot1)):
        try:
            cursorP.execute(queryP_in_P2,cbot1[i])
            conexionP.commit()
            b += 1
        except:
            r += 1
    #        print(i)
    #        break
            #conexion a PostgreSQL produccion
            conexionP = psycopg2.connect(**connP_P)
            cursorP = conexionP.cursor ()
except:
    pass

try:
    for i in range(len(chat1)+len(conv1)+len(cbot1)):
    #for i in range(len(cbot1)):
        #close PostgreSQL
        cursorP.close()
        conexionP.close()
except:
    pass