import pymysql
import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")
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

def time_data_clean(df):
    # 將 'WHOLE_TIME' 欄位轉換為時間字串格式
    df['WHOLE_TIME'] = pd.to_timedelta(df['WHOLE_TIME']).astype(str).str[-8:]
    
    #處理00:00:00
    df['WHOLE_TIME'] = df['WHOLE_TIME'].replace('00:00:00', '24:00:00')
    
    #時間與天氣資料對齊
    df['WHOLE_TIME'] = df['WHOLE_TIME'].astype(str).str[0:2]

def age_data_clean(df):
    #處理非人類年齡
    df.loc[df['OBJ_GENDER'].isin(['男', '女']), 'OBJ_AGE'] = np.nan
    df['OBJ_AGE'].fillna('其他', inplace=True)
    # 對 'OBJ_AGE' 欄位的數值清整
    df['OBJ_AGE'].astype(int)
    bins_age = [0, 18, 40, 65, np.inf]
    labels_age = ['少年', '青年', '中年', '老年']
    df['OBJ_AGE_CATEGORICAL'] = pd.cut(df['OBJ_AGE'].astype(int), bins=bins_age, labels=labels_age, right=False, include_lowest=True)
    df.loc[df['OBJ_AGE'] < 0, 'OBJ_AGE'] = np.nan
    df['OBJ_AGE'] = df['OBJ_AGE_CATEGORICAL']
    df = df.drop(columns=['OBJ_AGE_CATEGORICAL'])

def speed_limit_data_clean(df):
    #速限補空值(用道路類別平均速限四捨五入)
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'].isin(['市區道路', '其他','縣道','專用道路','鄉道','省道'])), 'SPEED_LIMIT'] = '50'
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'] == '村里道路'), 'SPEED_LIMIT'] = '40'
    df.loc[(df['SPEED_LIMIT'].isna()) & (df['ROAD_TYPE_MAIN'] == '國道'), 'SPEED_LIMIT'] = '90'
    # 數值清整
    df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'] = df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'].astype(int) * 10
    df.loc[(df['SPEED_LIMIT'] >= 30) & (df['SPEED_LIMIT'] < 110), 'SPEED_LIMIT'] = (df['SPEED_LIMIT'] // 10) * 10
    df.loc[(df['SPEED_LIMIT'] > 110) & (df['SPEED_LIMIT'] < 200), 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] % 100) // 10) * 10
    df.loc[df['SPEED_LIMIT'] > 199, 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] // 100)*10).astype(int)
    df.loc[(df['SPEED_LIMIT'] >= 10) & (df['SPEED_LIMIT'] < 30) & ~df['SPEED_LIMIT'].isin([10, 15, 20, 25]), 'SPEED_LIMIT'] = None
    df.loc[df['SPEED_LIMIT'] == 0, 'SPEED_LIMIT'] = None

def category_data_modify_missing_value(df):
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

if "__name__" == "__main__":

    for filename in os.listdir(source_path):
        if not filename.endswith('.csv'):
            continue
        df = pd.read_csv(f'{source_path}{filename}',encoding='UTF-8', header=0, index_col=False)
        
        time_data_clean(df)
        age_data_clean(df)
        speed_limit_data_clean(df)
        category_data_modify_missing_value(df)
    
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
    # 關閉資料庫連線
    conn.close()