# 檔案名稱取出日期值並加入檔案欄位中
import csv
import os
import re
import pymysql
import time

# 資料庫必要資訊(此為要寫入自己的MySQL)
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "請輸入密碼",
    "database": "請輸入指定的database"
}
# 連接到MySQL資料庫
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 原始檔案所在目錄
source_path = "請輸入檔案所在位置"

# 取檔名裡觀測站ID的正規表達式
patternId = r"^(.*?)-"

# 取檔名裡年月日的正規表達式
patternWholeDate = r"(\d{4}-\d{2}-\d{2})\."

# 將目錄裡的檔案利用迴圈依序做處理
start_time = time.time()
for filename in os.listdir(source_path):

    # 利用正規表達式取出檔名的觀測站ID
    match = re.search(patternId, filename)
    match_station_ID = match.group(1)

    # 利用正規表達式取出檔名的年月日
    match = re.search(patternWholeDate, filename)
    match_whole_date = match.group()

    # 將檔案讀取
    with open(os.path.join(source_path, filename), "r", newline="", encoding="utf-8") as file:

        # 將讀取的csv去除掉前兩行存在rows變數裡
        reader = csv.reader(file)
        rows = list(reader)
        rows = rows[2:]

        # 將觀測站的ID塞到第一列
        for row in rows[0:]:
            row.insert(0, f"{match_station_ID}")

        # 將日期塞到第二列
        for row in rows[0:]:
            row.insert(1, f"{match_whole_date}")

        # 將修改後的資料塞到weater的table裡，請先在MySQL建立自己的table
        try:
            for row in rows[0:]:
                cursor.execute(
                    "INSERT INTO weather(Station_ID , whole_date, ObsTime, StnPres, SeaPres, Temperature, Td_dew_point, RH, WS, WD, WSGust,WDGust, Precp, PrecpHour, SunShine, GloblRad, Visb, UVI, Cloud_Amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
            # 每30000筆commit一次
            if row  % 30000 == 0: 
                conn.commit()
        # 將有錯誤的部分寫入一個txt檔
        except pymysql.MySQLError as e:
            with open('error_log.txt', 'a') as err_log:
                err_log.write(f"Error inserting row {row}: {str(e)}\n")

        finally:
            # 這部分的代碼無論是否有錯誤都會執行
            conn.commit()   # 最後剩餘的commit 
            print(f"Finished processing row {row}")
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"The code took {elapsed_time} seconds to run.")

cursor.close()
conn.close()
