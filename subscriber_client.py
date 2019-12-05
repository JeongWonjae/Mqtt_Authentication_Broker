from socket import *
import threading
import pymysql
import control_db as dbF
import token_seed as ts

utf="utf-8"
host='127.0.0.1'
port=4444
id="USER1"

#데이터베이스 드라이버 호출
conn=pymysql.connect(host='localhost', user='root', password='root', db='mqttdb', charset='utf8')
curs=conn.cursor(pymysql.cursors.DictCursor)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect((host, port))

def RE_SUB(msg):
    sql = "select * from sub_db"
    token_seed = dbF.sql_return(sql)  # 기존 토큰/시드를 가져옴

    new_token = ts.calculator(str(token_seed['token']), str(token_seed['seed']))  # 새로운 토큰 생성
    msg += " {} {}".format(new_token, id)
    client_socket.send(msg.encode(utf))  # 새로운 토큰 전달

    sql = "update sub_db set token={}".format(new_token)  # 새로운 토큰 저장
    dbF.sql_N_return(sql)

def recv_data():
    while True:
        data = client_socket.recv(1024)
        print(data.decode(utf))

        #브로커의 정보요청
        if 'Request for information' in data.decode(utf):
            msg = "INFO 127.0.0.1, CAR DOOR, USER1, 1234, CAR/DOOR"
            client_socket.send(msg.encode(utf))
            print("[SYSTEM_INFO] Passed the information to the Auth Broker.")

        #토큰을 전달받음
        if 'Your token/seed' in data.decode(utf):
            recv_ts=data.decode(utf)
            recv_ts=recv_ts.replace("Your token/seed ", "")
            recv_ts=recv_ts.replace("[SERV_INFO] ", "")
            recv_ts=recv_ts.strip()
            recv_ts_array=recv_ts.split("/") #0은 토큰, 1은 시드

            #데이터베이스에 토큰과 시드를 저장
            sql="update sub_db set token={}".format(int(recv_ts_array[0]))
            dbF.sql_N_return(sql)
            sql="update sub_db set seed={}".format(int(recv_ts_array[1]))
            dbF.sql_N_return(sql)

        #구독이 종료되었음, 다시 구독
        if 'Disconnect' in data.decode(utf):
            msg="SUB"
            RE_SUB(msg)

t = threading.Thread(target=recv_data)
t.daemon = True
t.start()

while True:
    msg = input()
    if msg == "exit": break
    elif "SUB" in msg : RE_SUB(msg)
    else : client_socket.send(msg.encode(utf))
    '''
    elif msg가 sub이면 데이터베이스에서 토큰에 시드값 연산한 결과를 같이 전달함
    이 후 브로커에게 disconnect 메시지가 오면 다시 sub를 자동으로 하는데 sub와 토큰을 같이 전달
    '''

#데이터베이스 구조
'''
table: sub_db
+-------+------------+------+-----+---------+-------+
| Field | Type       | Null | Key | Default | Extra |
+-------+------------+------+-----+---------+-------+
| token | bigint(20) | NO   |     | NULL    |       |
| seed  | int(11)    | NO   |     | NULL    |       |
+-------+------------+------+-----+---------+-------+
'''