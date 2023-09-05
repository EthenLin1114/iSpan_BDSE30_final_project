import pandas as pd
from geopy.distance import geodesic
import re
from datetime import datetime

def data_preprocessing(df_a, df_c):
    # 將測速資料的經緯度轉為數值
    df_c['緯度'] = pd.to_numeric(df_c['緯度'], errors='coerce')
    df_c['經度'] = pd.to_numeric(df_c['經度'], errors='coerce')

    # 將經緯度為缺失值的那筆資料刪掉
    df_c = df_c.dropna(subset=['緯度', '經度'])  

    # 建立空列
    new_column = pd.Series

    # 在指定位置插入空的列
    df_a.insert(7, 'STATION_ID', new_column)

# 將日期的年月日分別切割之後放在list裡
def spilt_date(date):
    date_list = []
    match = re.search(patternYears_month_date, date)
    date_list.append(match.group(1))
    date_list.append(match.group(2))
    date_list.append(match.group(3))
    return date_list

# 該方法用來判斷該場車禍是否在指定測站的營運時間內，a、b、c值分別為六都車禍的日期、測站開始日期、測站撤銷日期
def during_activation_True_False(a, b, c):
    try:
        accident_date = spilt_date(a)
        station_start_date = spilt_date(b)
    
    except AttributeError:
        return_ture_false= False
        return return_ture_false
    
    if station_start_date[0]>accident_date[0]:
        after_start_date = False
        
    elif station_start_date[0] == accident_date[0]:
        
        if station_start_date[1]>accident_date[1]:
            after_start_date = False
            
        elif station_start_date[1] == accident_date[1]:
            
            if station_start_date[2] > accident_date[2]:
                after_start_date = False
                
            else:
                after_start_date = True
        else:
            after_start_date = True
                
    else:
        after_start_date = True
    
    if after_start_date:
        
        if pd.notna(c):
            station_stop_date = spilt_date(c)
            
            if accident_date[0] > station_stop_date[0]:
                return_ture_false = False
            
            elif accident_date[0] == station_stop_date[0]:
                
                if accident_date[1] > station_stop_date[1]:
                    return_ture_false = False
                    
                elif accident_date[1] == station_stop_date[1]:
                    
                    if accident_date[2] > station_stop_date[2]:
                        return_ture_false = False
                        
                    else: 
                        return_ture_false = True
                    
                else:
                    return_ture_false = True
                
            else: 
                return_ture_false = True
        
        else:
            return_ture_false= True
        
    else:
        return_ture_false= False
        
    return return_ture_false

# 對於df_a表中進來的每一筆資料，找到最接近的測站
def find_nearest_station():
    min_distance = float('inf')
    station_id = None
    for _, coord in df_staion.iterrows():
        distance = geodesic((row['LATITUDE'],row['LONGITUDE']), (coord['緯度'], coord['經度'])).kilometers
        
        if distance < min_distance:
            if during_activation_True_False(row['WHOLE_DATE'], coord['資料起始日期'], coord['撤站日期']):
                min_distance = distance
                print(min_distance)
                station_id = coord['站號']
            else:
                continue
    return station_id

# # 對於df_a表中進來的每一筆資料，找到最接近的測速，並限定範圍
def find_nearest_camera():
    min_distance = float('inf')
    camera_id = None
    limit_distance = 0.3
    for _, coord in df_camera.iterrows():
        distance = geodesic((row['LATITUDE'], row['LONGITUDE']), (coord['緯度'], coord['經度'])).kilometers
        if distance < min_distance:
            min_distance = distance
            camera_id = coord['TEMP ID']
    if min_distance <= limit_distance:
        return camera_id

    else:
        return None
    
if __name__ == "__main__":
    # 讀取事故資料作為df_a
    df_accident_hotspot = pd.read_csv('事故熱點.csv')

    # 讀取測站資料作為df_b
    df_staion = pd.read_csv('氣象測站清單20230726.csv')

    # 讀取測速資料作為df_c
    df_camera = pd.read_csv('臺中市_camera.csv')

    # 日期的正規表達式
    patternYears_month_date = r"(\d+)/(\d+)/(\d+)"

    data_preprocessing(df_accident_hotspot, df_camera)

    # 執行一次就好(如果不小心重複執行，就看下面的程式執行到哪筆再把0修改為看到的最後一筆的數字)
    stop_point = 0

    # 上面遇到未知狀況或手動停止，除錯完之後再執行這個程式
    for index, row in df_accident_hotspot.iterrows():
        if index >= stop_point:
            df_accident_hotspot.loc[index, 'STATION_ID'] = find_nearest_station()
            df_accident_hotspot.loc[index, 'CAMERA_ID'] = find_nearest_camera()
            if index % 1000 == 0:
                df_accident_hotspot.to_csv('台中市_A2_new_20230705.csv', index=False, encoding= 'UTF-8-sig')
            stop_point += 1
            print(f'第{index}筆已新增完畢')

    # 儲存結果到新的CSV檔案
    df_accident_hotspot.to_csv('台中市_A2_new_20230705.csv', index=False, encoding= 'UTF-8-sig')