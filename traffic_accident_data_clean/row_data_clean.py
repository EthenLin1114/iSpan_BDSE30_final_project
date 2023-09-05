from datetime import datetime
import re
import pandas as pd
import pymysql
import os

# 時間欄位的清整方法
def format_time(number):
    float_number = float(number)
    int_number = int(float_number) 
    hours = int_number // 10000
    minutes = (int_number // 100) % 100
    seconds = int_number % 100
    return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)

# 日期欄位的清整方法
def format_date(number):
    date_string = number.strip() # 將整數轉換為字串
    date_data = datetime.strptime(date_string, '%Y%m%d').date()
    return date_data

# 死亡人數切分方法
def spilt_dead_num(Casualties):
    dead_num = r"死亡(\d+)"
    match = re.search(dead_num, Casualties)
    return match.group(1)

# 受傷人數切分方法
def spilt_injuried_num(Casualties):
    injuried_num = r"受傷(\d+)"
    match = re.search(injuried_num, Casualties)
    return match.group(1)

# 縣市名稱切分方法
def spilt_city(df):
    df['縣市名稱'] = df['發生地點'].apply(lambda x: x[0:3])

# 時間欄位清整
def row_data_time_clean(df):
    df['發生時間'] = df['發生時間'].astype(str).str.replace(':', '')
    df['發生時間'] = df['發生時間'].apply(lambda x: x.split('.')[0])
    df['發生時間'] = df['發生時間'].apply(lambda x: x.zfill(6))
    df['發生時間'] = df['發生時間'].str[0:2]
    df['發生時間'] = df['發生時間'].astype(int)+1
    df['發生時間'] = df['發生時間'].apply(lambda x: f"{x}:00:00")

# 日期欄位清整
def row_data_date_clean(df):
    df['發生日期'] = df['發生日期'].astype(str).str.replace('-', '')
    df['發生日期'] = df['發生日期'].apply(lambda x: x.split('.')[0])
    df['發生日期'] = df['發生日期'].apply(lambda x: format_date(x))

def row_data_added_deaths_and_injuries(df):
    # 創建一个空的列
    new_column = pd.Series(dtype='int')

    # 在指定位置插入空的列
    df.insert(31, '死亡人數', new_column)
    df.insert(32, '受傷人數', new_column)
    

    # 將死亡人數與受傷人數切分
    df['死亡人數'] = df['死亡受傷人數'].apply(lambda x: spilt_dead_num(x))
    df['受傷人數'] = df['死亡受傷人數'].apply(lambda x: spilt_injuried_num(x))
    # 將死亡受傷人數一整列的資料刪除
    df = df.drop(["死亡受傷人數"], axis=1)
    df.insert(4, '縣市名稱', new_column)

if "__name__" == "__main__":
    host = 'localhost'
    user = 'root'
    password = 'Passw0rd!'
    database = 'teamone_fix'
    port = 3306

    # 建立mysql連線
    conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
 
    # 指定要寫入的資料庫表名稱
    table_name = 'ACCIDENT'

    
    source_path = "./全國交通事故資料/107年傷亡道路交通事故資料"

    # 讀取source_path裡的所有檔案
    for filename in os.listdir(source_path):

        # 讀取csv檔並將發生年度、發生月份欄位的資料刪除
        df = pd.read_csv("{}/{}".format(source_path,filename),encoding='UTF-8-sig', header=0).drop(["發生年度","發生月份"], axis=1)
        
        # 將最下面兩行說明刪除
        df = df.dropna(axis=0,subset=['發生日期'])
        row_data_time_clean(df)
        row_data_date_clean(df)
        spilt_city(df)

        # 一行一行寫入資料庫
        for row in df.itertuples(index=False):
            values = ', '.join([f"'{value}'" if pd.notnull(value) else 'NULL' for value in row])
            query = f"INSERT INTO {table_name} VALUES ({values})"
            with conn.cursor() as cursor:
                cursor.execute(query)

        # 將上面for迴圈的內容commit到資料庫    
        conn.commit()

    # 關閉資料庫連線
    conn.close()