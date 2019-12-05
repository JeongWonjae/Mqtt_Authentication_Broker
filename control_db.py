import pymysql

conn=pymysql.connect(host='localhost', user='root', password='root', db='mqttdb', charset='utf8')
curs=conn.cursor(pymysql.cursors.DictCursor)

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