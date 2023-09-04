import pymysql
import pandas as pd
import numpy as np
import os
import time
import warnings
warnings.filterwarnings("ignore") 
import re
from datetime import datetime

host = 'localhost'
user = 'root'
password = 'Passw0rd!'
database = 'teamone_fix'
port = 3306


# 建立mysql連線
conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port)

# 指定要寫入的資料表名稱
table_name = 'awc_concat'


source_path = "C:/Users/student/Main_P/DEVIDE/result/"

for filename in os.listdir(source_path):
    if not filename.endswith('.csv'):
        continue
    df = pd.read_csv(f'{source_path}{filename}',encoding='UTF-8', header=0, index_col=False)
    
    # 將 'WHOLE_TIME' 欄位轉換為時間字串格式
    df['WHOLE_TIME'] = pd.to_timedelta(df['WHOLE_TIME']).astype(str).str[-8:]
    
    #處理00:00:00
    df['WHOLE_TIME'] = df['WHOLE_TIME'].replace('00:00:00', '24:00:00')
    
    #時間與天氣資料對齊
    df['WHOLE_TIME'] = df['WHOLE_TIME'].astype(str).str[0:2]
    
    #處理非人類年齡
    df.loc[~df['OBJ_GENDER'].isin(['男', '女']), 'OBJ_AGE'] = np.nan
    df['OBJ_AGE'].fillna('其他', inplace=True)
    
    #速限補空值(用道路類別平均速限四捨五入)
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'].isin(['市區道路', '其他','縣道','專用道路','鄉道','省道'])), 'SPEED_LIMIT'] = '50'
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'] == '村里道路'), 'SPEED_LIMIT'] = '40'
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'] == '國道'), 'SPEED_LIMIT'] = '90'
    
    #類別補空值
    df['VEHICLE_MAIN'].fillna('無', inplace=True)
    df['VEHICLE_SUB'].fillna('無', inplace=True)
    df['PROTECTION'].fillna('不明', inplace=True)
    df['C_PDT_USAGE'].fillna('不明', inplace=True)
    df['OBJ_CDN_MAIN'].fillna('其他', inplace=True)
    df['OBJ_CDN_SUB'].fillna('其他', inplace=True)
    df['CRASH_MAIN'].fillna('無', inplace=True)
    df['CRASH_SUB'].fillna('無', inplace=True)
    df['CRASH_OTHER_MAIN'].fillna('無', inplace=True)
    df['CRASH_OTHER_SUB'].fillna('無', inplace=True)
    df['CAUSE_MAIN_DETAIL'].fillna('其他', inplace=True)
    df['CAUSE_SUB_DETAIL'].fillna('不明', inplace=True)
    df['HAR'].fillna('是', inplace=True)

    # 對 'OBJ_AGE' 欄位的數值清整
    df['OBJ_AGE'].astype(int)
    bins_age = [0, 18, 40, 65, np.inf]
    labels_age = ['少年', '青年', '中年', '老年']
    df['OBJ_AGE_CATEGORICAL'] = pd.cut(df['OBJ_AGE'].astype(int), bins=bins_age, labels=labels_age, right=False, include_lowest=True)
    df.loc[df['OBJ_AGE'] < 0, 'OBJ_AGE'] = np.nan
    df['OBJ_AGE'] = df['OBJ_AGE_CATEGORICAL']
    df = df.drop(columns=['OBJ_AGE_CATEGORICAL'])

    # 對 'SPEED_LIMIT' 欄位的數值清整

    df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'] = df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'].astype(int) * 10
    df.loc[(df['SPEED_LIMIT'] >= 30) & (df['SPEED_LIMIT'] < 110), 'SPEED_LIMIT'] = (df['SPEED_LIMIT'] // 10) * 10
    df.loc[(df['SPEED_LIMIT'] > 110) & (df['SPEED_LIMIT'] < 200), 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] % 100) // 10) * 10
    df.loc[df['SPEED_LIMIT'] > 199, 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] // 100)*10).astype(int)
    df.loc[(df['SPEED_LIMIT'] >= 10) & (df['SPEED_LIMIT'] < 30) & ~df['SPEED_LIMIT'].isin([10, 15, 20, 25]), 'SPEED_LIMIT'] = None
    df.loc[df['SPEED_LIMIT'] == 0, 'SPEED_LIMIT'] = None
    
    
  
    # 一行一行寫入資料庫並判斷是否為空值是的話填入null
    for row in df.itertuples(index=False):
        values = ', '.join([f"'{value}'" if pd.notnull(value) else 'NULL' for value in row])
        query = f"INSERT INTO {table_name} VALUES ({values})"
        try:
            with conn.cursor() as cursor:
    
                cursor.execute(query)
    
    
        except pymysql.MySQLError as e:
            print(f"row: {row}: {str(e)}\n")
        
        conn.commit()
    print(f"Finished processing row {row}")

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
    from datetime import datetime
    date_string = number.strip() # 將整數轉換為字串
    date_data = datetime.strptime(date_string, '%Y%m%d').date()
    return date_data

# 死亡人數切分方法
def spilt_dead_num(Casualties):
    match = re.search(dead_num, Casualties)
    return match.group(1)

# 受傷人數切分方法
def spilt_injuried_num(Casualties):
    match = re.search(injuried_num, Casualties)
    return match.group(1)

# 縣市名稱切分方法
def spilt_city(address):
    return address[0:3]
    
# 指定要寫入的資料庫表名稱
table_name = 'ACCIDENT'
dead_num = r"死亡(\d+)"
injuried_num = r"受傷(\d+)"

source_path = "./全國交通事故資料/107年傷亡道路交通事故資料"

# 讀取source_path裡的所有檔案
for filename in os.listdir(source_path):

    # 讀取csv檔並將發生年度、發生月份欄位的資料刪除
    df = pd.read_csv("{}/{}".format(source_path,filename),encoding='UTF-8-sig', header=0).drop(["發生年度","發生月份"], axis=1)

    # 將最下面兩行說明刪除
    df = df.dropna(axis=0,subset=['發生日期'])

    # 時間欄位清整
    df['發生時間'] = df['發生時間'].astype(str).str.replace(':', '')
    df['發生時間'] = df['發生時間'].apply(lambda x: x.split('.')[0])
    df['發生時間'] = df['發生時間'].apply(lambda x: x.zfill(6))
    df['發生時間'] = df['發生時間'].str[0:2]
    df['發生時間'] = df['發生時間'].astype(int)+1
    df['發生時間'] = df['發生時間'].apply(lambda x: f"{x}:00:00")

    # 日期欄位清整
    df['發生日期'] = df['發生日期'].astype(str).str.replace('-', '')
    df['發生日期'] = df['發生日期'].apply(lambda x: x.split('.')[0])
    df['發生日期'] = df['發生日期'].apply(lambda x: format_date(x))

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
    # 切分縣市名稱
    df['縣市名稱'] = df['發生地點'].apply(lambda x: spilt_city(x))

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