import string
import random

#토큰 생성
def make_token():
    token_size=10 #토큰 길이
    string_pool=string.digits #숫자, 대소문자 (대소문자 : string.ascii_letters / 대문자 : string.ascii_uppercase)

    token = ""
    for i in range(token_size):
        token += random.choice(string_pool)  # 문자열 중 하나를 선택
    return token

#시드 생성
def make_seed():
    seed_size=5 #시드 길이
    string_pool=string.digits #숫자, 대소문자 (대소문자 : string.ascii_letters / 대문자 : string.ascii_uppercase)

    seed = ""
    for j in range(seed_size):
        seed += random.choice(string_pool)
    return seed

#토큰/시드 계산
def calculator(token, seed):

    token_calculated=str(int(int(token)/int(seed))) #토큰을 시드로 나눔
    if (len(token_calculated)==4): token_calculated+="0"
    if (len(token_calculated)==3): token_calculated+="00"

    result=["0","0","0","0","0","0","0","0","0","0"]
    for i in range(0, 10): #나눈 결과를 원래 토큰의 홀 수 번째에 삽입함
        if(i%2==0):
            result[i]=token[i]
        elif(i%2==1):
            result[i]=token_calculated[int(i/2)]

    result_char="" #새로운 토큰이 생성됨
    for i in range(0, 10):
        result_char+=result[i]

    return result_char

#testing

'''a=0
while(a<10):
    print("{}번째 테스팅".format(a+1))
    token=str(make_token())
    seed=str(make_seed())
    print("token : ", token)
    print("seed : ", seed)
    new_token=calculator(token, seed)
    print("new token : ", new_token)
    print("new token len : ", len(new_token))
    print("\n")
    a+=1'''



