import paho.mqtt.client as mqtt
import time
import pymysql

conn=pymysql.connect(host='localhost', user='root', password='root', db='mqttdb', charset='utf8')
curs=conn.cursor(pymysql.cursors.DictCursor)
broker_address="broker.hivemq.com"

#반환없는 데이터베이스 명령
def sql_N_return(sql):
	curs.execute(sql)
	conn.commit()

#반환있는 데이터베이스 명령(fecth one)
def sql_return(sql):
	curs.execute(sql)
	conn.commit()
	fetch_data = curs.fetchone()
	return fetch_data

#데이터베이스 쿼리 참/거짓 리턴
def sql_boolean_return(sql):
	if(curs.execute(sql)): return 1
	else: return 0

#MQTT 구독/발행 함수 재정의
def on_messsage(client, userdata, message):
	print("[MQTT_INFO] Message received from publisher", str(message.payload.decode("utf-8")))
	print("[MQTT_INFO] Message topic=", message.topic)
	#받은 메시지를 DB에 저장\
	sql_tmp=f"select * from sub_info where topic='{message.topic}'"
	if(sql_boolean_return(sql_tmp)==0):
		sql_tmp="insert into sub_info values('{}', '{}', 1)".format(message.topic, message.payload.decode('utf-8'))
		sql_N_return(sql_tmp)
	else:
		count=sql_return(sql_tmp)
		count=count['count']
		count=int(count)+1
		sql_tmp=f"update sub_info set count={count} where topic='{message.topic}'"
		sql_N_return(sql_tmp)
		sql_tmp=f"update sub_info set msg='{message.payload.decode('utf-8')}' where topic='{message.topic}'"
		sql_N_return(sql_tmp)

def on_log(client, userdata, level, buf):
	print("[MQTT_IOG] LOG [{}] ".format(buf))

def sub_function(topic):
	client = mqtt.Client()
	client.on_message=on_messsage
	client.on_log=on_log
	client.connect(broker_address)
	print("[MQTT_INFO] Conneting to broker")
	client.loop_start()
	client.subscribe(topic)
	print("[MQTT_INFO] Subscribe topic and listening")
	time.sleep(10)
	print("[MQTT_INFO] Disconnect to broker")
	client.loop_stop()

def sub_start(topic):
	sub_function(topic)

