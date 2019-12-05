from socket import *
import random
import threading
import pymysql
import paho.mqtt.client as mqtt
import mqtt_sub as subF
import time
import token_seed as ts

'''
t[0]= user, publisher
t[1~]= client, subscriber
'''

#정의
broker_port=1883
host='127.0.0.1'
port=4444
bufsize=1024
utf="utf-8"
t=[]
index=0

#데이터베이스 드라이버 호출
conn=pymysql.connect(host='localhost', user='root', password='root', db='mqttdb', charset='utf8')
curs=conn.cursor(pymysql.cursors.DictCursor)

#멀티 스레드 소켓 클래스
class MultiSocket(threading.Thread):

    def __init__(self,socket):
        super().__init__()
        self.server_socket=socket

    def run(self):

        global index
        self.client_socket, addr=self.server_socket.accept() #연결 수락
        print(f"[INFO] Access Client(Sub) {addr[0]}:{addr[1]}")
        index=index+1

        create_thread(self.server_socket) #재귀
        t=threading.Thread(target=self.c_recv)
        t.daemon=True
        t.start()

    def c_recv(self):

        #print("[INFO] Message from {}".format(self.client_socket))
        while True:
            recv=self.client_socket.recv(1024)

            try:
                #등록요청 받으면
                if 'REGIST' in recv.decode(utf):
                    #REGIST 문자열 발견 시 정보를 요구함
                    send_response="[SERV_INFO] Request for information"
                    self.c_send(send_response)

                #등록정보 받으면
                elif 'INFO' in recv.decode(utf):
                    #INFO 문자열 발견 시 (,)을 기준으로 정보를 나누어서 데이터베이스에 저장함
                    recv_info=(recv.decode(utf)).replace("INFO", "")
                    client_info=((recv_info).split(", ")) #(, )으로 구분
                    client_info[0]=client_info[0].replace(" ", "") #주소 앞 공백제거
                    print("[INFO] Save client inforamtion {}/{}/{}".format(client_info[0], client_info[1], client_info[2]))
                    # client_info 0~4 -> IP, System_info, ID, PW, TOPIC

                    #저장한 정보의 구독자를 신뢰할 것인지 발행자에게 물음
                    send_pub="[SERV_INFO] Request from Subscriber IP={} / System={} / ID={}".format(client_info[0], client_info[1], client_info[2])
                    t[0].c_send(send_pub) #t[0]=User(Publisher)

                    sql = "insert into client_info values('{}','{}','{}','{}','{}', 'NO')".format(client_info[0], client_info[1], client_info[2], client_info[3], client_info[4])
                    curs.execute(sql)
                    conn.commit()

                #허용 되면 토큰 생성
                elif 'ACK' in recv.decode(utf):
                    #발행자에게 해당 구독자의 ID정보를 신뢰승인 받으면 데이터베이스에 'Accept'컬럼을 'YES'로 수정함
                    ack_id=recv.decode(utf)
                    ack_id=ack_id.replace("ACK ", "")
                    ack_id=ack_id.replace(" ", "")
                    sql = "update client_info set accept='YES' where id='{}'".format(ack_id)
                    curs.execute(sql)
                    conn.commit()

                    #승인 받았기 때문에 바로 토큰/시드를 생성해서 저장하고 양쪽에 전달함
                    print("[INFO] Make token for {} and publisher".format(ack_id))
                    #토큰과 시드 생성
                    token=ts.make_token()
                    seed=ts.make_seed()
                    send_tokenseed=f"[SERV_INFO] Your token/seed {token}/{seed}"
                    print("[INFO] Issue token to {} and publisher".format(ack_id))

                    #!원래는 올바른 구독자에게만 토큰을 전달해야함
                    t[0].c_send(send_tokenseed) #t[0]은 발행자
                    t[1].c_send(send_tokenseed) #t[1]은 구독자

                    send_accept_msg="[SERV_INFO] Start your pub/sub"
                    t[0].c_send(send_accept_msg)
                    t[1].c_send(send_accept_msg)

                    sql="insert into token_info values('{}', {}, {}, {})".format(ack_id, token, seed, 1)
                    curs.execute(sql)
                    conn.commit()
                    sql="insert into token_info values('PUBLISHER', {}, {}, {})".format(token, seed, 1)
                    curs.execute(sql)
                    conn.commit()

                #거절
                elif 'NAK' in recv.decode(utf):
                    send_sub="[SERV_INFO] Connect reject by publisher"
                    t[1].c_send(send_sub) #구독자에게 거절

                #구독
                elif 'SUB' in recv.decode(utf): #구독자로부터
                    sub_info=recv.decode(utf)
                    sub_info=sub_info.replace("SUB ", "")
                    sub_info=sub_info.split(" ")
                    #0은 토큰, 1은 아이디
                    sub_token=sub_info[0]

                    # 브로커가 저장해둔 토큰/시드 계산
                    sql = "select * from token_info where id='{}'".format(sub_info[1])
                    broker_token = sql_return(sql)
                    broker_new_token = ts.calculator(str(broker_token['token']), str(broker_token['seed']))

                    allowed_client=sql_return("select * from client_info where accept='YES'")
                    if 'YES' in allowed_client['accept']: #허용이 된 구독자인지 확인
                        print("[INFO] Compare sub_token with my_token")
                        if broker_new_token==sub_token: #새로운 토큰과 수신 받은 토큰을 비교
                            print("[INFO] Token values are equal. Subscribe approved")
                            print("[INFO] Token was {}".format(sub_token))

                            send_sub="[SERV_INFO] Token was corrected. Start Subscribe"
                            t[1].c_send(send_sub)

                            topic=allowed_client['topic'] #토픽을 빼옴
                            # 구독자에게 발행되는 메시지를 전달하는 쓰레드 함수 미리 호출
                            send_sub_msg = threading.Thread(target=print_sub_info)
                            send_sub_msg.daemon = True
                            send_sub_msg.start()
                            subF.sub_start(topic)  # 구독 함수 호출, 메시지를 받으면 DB에 저장

                            # 구독자에게 구독 종료를 알림
                            send_sub = "[SERV_INFO] Disconnect to broker"
                            t[1].c_send(send_sub)

                            sql = "update token_info set token={} where id='{}'".format(broker_new_token, sub_info[1])
                            sql_N_return(sql)
                        else: # 토큰이 다름
                            print("[INFO] Token values are different. Subscribe denied")
                            send_sub = "[SERV_INFO] Token was wrong. Subscribe denied"
                            self.c_send(send_sub)

                #발행
                elif 'PUB' in recv.decode(utf): #발행자로부터
                    broker_url = "broker.hivemq.com"
                    pub_info=recv.decode(utf)
                    pub_info=pub_info.replace("PUB ", "")
                    pub_info=pub_info.split(" ")
                    pub_token=pub_info[2]
                    pub_token=pub_token.replace("@", "")
                    #토픽은 0, 메시지는 1, 토큰은 2

                    #브로커가 저장해둔 토큰/시드 계산
                    sql="select * from token_info where id='PUBLISHER'"
                    broker_token=sql_return(sql)
                    broker_new_token=ts.calculator(str(broker_token['token']), str(broker_token['seed']))

                    print("[INFO] Compare pub_token with my_token")
                    if(broker_new_token==pub_token): #토큰 비교
                        print("[INFO] Token values are equal. Publish approved")
                        print("[INFO] Token was {}".format(pub_token))

                        # 메인 MQTT 연결, 구독 메시지 발행
                        client = mqtt.Client()
                        client.connect(broker_url)
                        client.publish(pub_info[0], pub_info[1])

                        send_pub = "[SERV_INFO] Success message forwording"
                        self.c_send(send_pub)

                        # 인증 브로커 데이터베이스에 새로운 토큰 갱신
                        sql="update token_info set token={} where id='PUBLISHER'".format(broker_new_token)
                        sql_N_return(sql)
                    else:
                        print("[INFO] Token values are different. Publish denied")
                        send_pub="[SERV_INFO] Token was wrong. Publish denied"
                        self.c_send(send_pub)

                else:
                    self.c_send("[SERV_INFO] Wrong Command")

            except Exception as e: print("[ERROR] ", e)

    def c_send(self, data):
        self.client_socket.send(data.encode(utf))

#Make Client Socket
def create_thread(server_socket):
    global index
    t.append(MultiSocket(server_socket))
    t[index].daemon=True
    t[index].start()

#반환없는 데이터베이스 명령
def sql_N_return(sql):
    sql=sql
    curs.execute(sql)
    conn.commit()

#반환있는 데이터베이스 명령(fecth one)
def sql_return(sql):
    sql=sql
    curs.execute(sql)
    conn.commit()
    fetch_data = curs.fetchone()
    return fetch_data

#데이터베이스 쿼리 참/거짓 리턴
def sql_boolean_return(sql):
    if(curs.execute(sql)): return 1
    else: return 0

def db_clear():
    sql_N_return("delete from client_info")
    sql_N_return("delete from token_info")
    sql_N_return("delete from sub_info")

def print_sub_info():
    start_time = time.time()
    while (time.time()-start_time<=10):
        sql = "select * from sub_info"
        time.sleep(1)
        try:
            fetch_data = sql_return(sql)
            send_sub = "[SERV_INFO] Current Status : {}".format(fetch_data['msg'])
            t[1].c_send(send_sub)
        except:
            pass

#인증 브로커 소켓 생성
print("[INFO] Server started")
server_socket=socket(AF_INET, SOCK_STREAM)
server_socket.bind((host, port))
print("[INFO] Waiting Client")
server_socket.listen(1)
create_thread(server_socket)
print("[INFO] Database clear")
db_clear()

#------------------메인-----------------------#

while True:
    try:
        control=input()
        if control=='exit': break
        elif control=='check' or control=='CHECK': #연결된 소켓 확인
            for i in t:
                print(i)
        elif control=='dbcls' or control=='DBCLS': #데이터베이스 초기화
            sql_N_return("delete from client_info")
            sql_N_return("delete from token_info")
            sql_N_return("delete from sub_info")
        elif control=='exit' or control=='EXIT':
            break
    except Exception as e:
        pass

#클라이언트 소켓을 모두 닫음
for j in t:
    try: j.client_socket.close()
    except Exception as e: print(e)

#인증 브로커 소켓을 닫음
server_socket.close()

#---------------------------------------------#

'''
#데이터베이스 구조
table: client_info
+--------+-------------+------+-----+---------+-------+
| Field  | Type        | Null | Key | Default | Extra |
+--------+-------------+------+-----+---------+-------+
| ip     | varchar(30) | NO   |     | NULL    |       |
| info   | varchar(40) | NO   |     | NULL    |       |
| id     | varchar(20) | NO   | PRI | NULL    |       |
| pw     | varchar(20) | NO   |     | NULL    |       |
| topic  | varchar(20) | NO   |     | NULL    |       |
| accept | varchar(5)  | YES  |     | NULL    |       |
+--------+-------------+------+-----+---------+-------+

table: sub_info
+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| topic | varchar(30) | NO   |     | NULL    |       |
| msg   | varchar(60) | NO   |     | NULL    |       |
| count | int(11)     | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+

table: token_info
+-------+-------------+------+-----+---------+-------+
| Field | Type        | Null | Key | Default | Extra |
+-------+-------------+------+-----+---------+-------+
| id    | varchar(20) | NO   | PRI | NULL    |       |
| token | bigint(20)  | NO   |     | NULL    |       |
| seed  | int(11)     | NO   |     | NULL    |       |
| count | int(11)     | YES  |     | NULL    |       |
+-------+-------------+------+-----+---------+-------+
'''
