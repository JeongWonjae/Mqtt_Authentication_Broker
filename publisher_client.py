from socket import *
import threading
import control_db as dbF
import token_seed as ts

utf="utf-8"
client_socket = socket(AF_INET, SOCK_STREAM)
host='127.0.0.1'
port=4444
client_socket.connect((host, port))

def recv_data():
    while True:

        data = client_socket.recv(1024)
        print(data.decode(utf))

        #토큰을 전달받음
        if 'Your token/seed' in data.decode(utf):
            recv_ts=data.decode(utf)
            recv_ts=recv_ts.replace("Your token/seed ", "")
            recv_ts=recv_ts.replace("[SERV_INFO] ", "")
            recv_ts=recv_ts.strip()
            recv_ts_array=recv_ts.split("/") #0은 토큰, 1은 시드

            #데이터베이스에 토큰과 시드를 저장
            sql="update pub_db set token={}".format(int(recv_ts_array[0]))
            dbF.sql_N_return(sql)
            sql="update pub_db set seed={}".format(int(recv_ts_array[1]))
            dbF.sql_N_return(sql)


t = threading.Thread(target=recv_data)
t.daemon = True
t.start()

while True:
    msg = input()

    if msg == "exit": break
    elif "PUB" in msg :
        sql = "select * from pub_db"
        token_seed = dbF.sql_return(sql)  # 기존 토큰/시드를 가져옴

        new_token = ts.calculator(str(token_seed['token']), str(token_seed['seed']))  # 새로운 토큰 생성
        msg += " @{}".format(new_token)
        client_socket.send(msg.encode(utf))  # 새로운 토큰 전달

        sql = "update pub_db set token={}".format(new_token)  # 새로운 토큰 저장
        dbF.sql_N_return(sql)

    else: client_socket.send(msg.encode(utf))

#데이터베이스 구조
'''
table: pub_db
+-------+------------+------+-----+---------+-------+
| Field | Type       | Null | Key | Default | Extra |
+-------+------------+------+-----+---------+-------+
| token | bigint(20) | NO   |     | NULL    |       |
| seed  | int(11)    | NO   |     | NULL    |       |
+-------+------------+------+-----+---------+-------+
'''