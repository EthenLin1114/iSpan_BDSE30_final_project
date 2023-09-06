# 使用ETL來將我們專題的資料做清整後倒入MySQL或寫回csv
- added_station_id_and_date_to_csv.py:
    - 將csv檔名上的station id 和日期加入該csv檔裡

- row_data_clean.py:
    - 地址中的縣市拆分出來
    - 死亡和受傷人數拆開
    - 時間的格式改為XX:XX:XX
    - 日期的格式改為XXXX-XX-XX

- added_station_id_and_camera_id_to_data.py:
    - 利用geopy.distance的geodesic取每筆事故資料最近距離的天氣測站和科技執法


- final_data_clean.py:
    - 時間00:00:00改為24:00:00
    - 年齡從數值型資料改為類別型資料
    - 速限值的清整
    - 類別型資料填空值

## 套件安裝
pip install geopy
pip install pandas
pip install numpy
pip install pymysql