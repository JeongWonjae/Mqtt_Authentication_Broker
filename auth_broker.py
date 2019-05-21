from socket import *
import random
import threading
import pymysql
import paho.mqtt.client as mqtt
import sys
import time

'''
t[0]= user, publisher
t[1~]= client, subscriber
'''

#Define
broker_url="iot.eclipse.org"
broker_port=1883
host='127.0.0.1'
port=4444
bufsize=1024
utf="utf-8"
t=[]
index=0

#Connect Mysql
conn=pymysql.connect(host='localhost', user='root', password='root', db='mqttdb', charset='utf8')
curs=conn.cursor(pymysql.cursors.DictCursor)

#Mqtt connected
client=mqtt.Client()
client.connect(broker_url, broker_port)

#Multi thread socket Class
class MultiSocket(threading.Thread):

    def __init__(self,socket):
        super().__init__()
        self.server_socket=socket

    def run(self):

        global index
        self.client_socket, addr=self.server_socket.accept() #connect to server
        print(f"[INFO] Access Client(Sub) {addr[0]}:{addr[1]}")
        index=index+1

        create_thread(self.server_socket) #recursive
        t=threading.Thread(target=self.c_recv)
        t.daemon=True
        t.start()

    def c_recv(self):

        print("[INFO] Message from {}".format(self.client_socket))
        while True:
            recv=self.client_socket.recv(1024)

            try:
                #등록요청 받으면
                if 'REGIST' in recv.decode(utf):
                    send_response="[SERV_INFO] Entry your information\n> INFO IP,SYSTEM_INFO,ID,PW,TOPIC"
                    self.c_send(send_response)

                #등록정보 받으면
                elif 'INFO' in recv.decode(utf):
                    recv_info=(recv.decode(utf)).replace("INFO", "")
                    client_info=((recv_info).split(","))
                    print("[INFO] Save client inforamtion {}/{}/{}".format(client_info[0], client_info[1], client_info[2]))
                    # client_info 0~4 -> IP, System_info, ID, PW, TOPIC
                    send_pub="[SERV_INFO] Request from Subscriber IP={} / System={} / ID={}\nAre you Accept? e.g)ACK ID> ".format(client_info[0], client_info[1], client_info[2])
                    t[0].c_send(send_pub) #t[0]=User(Publisher)

                    sql = "insert into client_info values('{}','{}','{}','{}','{}', 'NO')".format(client_info[0], client_info[1], client_info[2], client_info[3], client_info[4])
                    curs.execute(sql)
                    conn.commit()

                #허용 되면 토큰 생성
                elif 'ACK' in recv.decode(utf):
                    ack_id=recv.decode(utf)
                    ack_id=ack_id.replace("ACK", "")
                    ack_id=ack_id.replace(" ", "")
                    sql = "update client_info set accept='YES' where id='{}'".format(ack_id)
                    curs.execute(sql)
                    conn.commit()

                    print(t[0])
                    print("[INFO] Make token for {} and publisher".format(ack_id))
                    token=str(random.getrandbits(32))
                    seed=str(random.getrandbits(8))
                    send_tokenseed=f"[SERV_INFO] token/seed {token}/{seed}"
                    print("[INFO] Issue token to {} and publisher".format(ack_id))
                    t[0].c_send(send_tokenseed)
                    t[1].c_send(send_tokenseed)
                    send_accept_msg="[SERV_INFO] Start your pub/sub"
                    t[0].c_send(send_accept_msg)
                    t[1].c_send(send_accept_msg)

                #거절
                elif 'NAK' in recv.decode(utf):
                    send_client="[SERV_INFO] Connect reject by publisher"
                    t[1].c_send(send_client)

                elif 'SUB' in recv.decode(utf):
                    sql="select topic from client_info where accept='YES'"
                    curs.execute(sql)
                    conn.commit()
                    topic=curs.fetchone()
                    #채널 저장하고
                    #mqtt 서버에 접속
                    client.subscribe(topic['topic'], qos=0)

                elif 'PUB' in recv.decode(utf):
                    pub_info=recv.decode(utf)
                    pub_info=pub_info.replace(" ","")
                    pub_info=pub_info.split(",")
                    #topic=pub_info[0], msg=pub_info[1]
                    client.publish(topic=pub_info[0], payload=pub_info[1])

                else:
                    self.c_send("[SERV_INFO] Wrong Command")

            except Exception as e: print(e)

    def c_send(self, data):
        self.client_socket.send(data.encode(utf))

#Make Client Socket
def create_thread(server_socket):
    global index
    t.append(MultiSocket(server_socket))
    t[index].daemon=True
    t[index].start()

#Make Server Socket
server_socket=socket(AF_INET, SOCK_STREAM)
print("[INFO] Server started!")
server_socket.bind((host, port))
server_socket.listen(1)
print("[INFO] Waiting Client...")
create_thread(server_socket)

#------------------Main-----------------------#

while True:
    control=input()
    if control=='exit': break
    elif control=='check':
        for i in t:
            print(i)
    elif control=='dbcls':
        sql="delete from client_info"
        curs.execute(sql)
        conn.commit()

#End Client Socket
for j in t:
    try: j.client_socket.close()
    except Exception as e: print(e)

#End Server Socket
server_socket.close()

#---------------------------------------------#