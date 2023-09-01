import requests
import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
from datetime import datetime
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
import pickle
import json

def determine_the_csv_to_read():
    now_time = datetime.now()
    # 獲取當前的小時
    current_hour = now_time.hour

    # 判斷是要讀取早上的csv檔還是晚上的csv檔
    if current_hour >= 6 and current_hour <= 17:
        df = pd.read_csv("SIX_CITY_new_hot_spot_morning.csv",  encoding="UTF-8")
    else:
        df = pd.read_csv("SIX_CITY_new_hot_spot_night.csv",  encoding="UTF-8")
    
    return df


# 取得即時天氣API模型需要的值
def get_instant_weather_data():
    url_automatic_station = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0001-001"

    url_automatic_station_rain = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001"

    params = {
        "Authorization": "CWB-69742410-F705-4E20-A583-CDF7EA930E9A",
        "format": "JSON",
        "stationId": "C0A770,C0A9F0,C0AC60,C0AC70,C0AC80,C0ACA0,C0AD30,C0AD40,C0AG80,C0AH00,C0AH10,C0AH70,C0AI00,C0AI30,C0AD40,C0C480,C0C490,C0C590,C0C620,C0C650,C0C670,C0C680,C0C700,C0F970,C0F9K0,C0F9M0,C0F9N0,C0F9O0,C0F9P0,C0F9R0,C0F9T0,C0F9U0,C0V440,C0V490,C0V660,C0V680,C0V700,C0V710,C0V730,C0V760,C0V810,C0V890,C0X100,C0X110,C0X160,C0A980,C0V760,C0F9T0,C0FA40,C0F9R0"
    }
    headers = {
        "accept": "application/json"
    }

    response = requests.get(url_automatic_station, headers=headers, params=params)

    # 取得JSON格式的回應內容
    automatic_station_data = response.json()

    response = requests.get(url_automatic_station_rain, headers=headers, params=params)

    # 取得JSON格式的回應內容
    automatic_station_rain_data = response.json()

    # 需要取得的value的key
    needs_elements = {"TEMP", "WDSD", "HUMD"}

    # 用來儲存所有 location 的元素值
    all_station_needs_values_dict = {}

    # 讀取全部location(六都)裡面的內容
    for location in automatic_station_data['records']['location']:
        # 儲存這個 location 的元素值
        element_values = {}
        
        # 取得每個測站needs_elements的值，並存成dict
        for element in location['weatherElement']:

            # 只取得needs_elements的值
            if element['elementName'] in needs_elements:
                element_values[element['elementName']] = element['elementValue']
        all_station_needs_values_dict[location['stationId']] = element_values

    # 取得雨量值，並新增到all_station_needs_values_dict裡
    for location in automatic_station_rain_data['records']['location']:
        
        # 取得每個測站RAIN的值，並新增到dict
        for element in location['weatherElement']:
            if element['elementName'] == "RAIN":
                all_station_needs_values_dict[location['stationId']]["RAIN"] = element['elementValue']
    return all_station_needs_values_dict

# 將取得的location裡的元素值和前端傳回來的車種、性別、年齡加入到dataframe裡
def add_data_to_six_city_hot_spots(df_six_city_hot_spots, weather_api_data_dict, vehicle, gender, age):
    
    # 獲取當前的日期和時間
    now_time = datetime.now()

    # 獲取當前的小時
    current_hour = now_time.hour

    # 獲取即時天氣api需要的值
    df_six_city_hot_spots['STATION_ID'] = df_six_city_hot_spots['STATION_ID'].astype(str)
    
    # 加入參數帶進來的值
    for index, row in df_six_city_hot_spots.iterrows():
        try:
            df_six_city_hot_spots.loc[index, 'WHOLE_TIME'] = current_hour
            df_six_city_hot_spots.loc[index, 'Temperature'] = weather_api_data_dict[row['STATION_ID']]["TEMP"]
            df_six_city_hot_spots.loc[index, 'WS'] = weather_api_data_dict[row['STATION_ID']]["WDSD"]
            df_six_city_hot_spots.loc[index, 'RH'] = weather_api_data_dict[row['STATION_ID']]["HUMD"]
            df_six_city_hot_spots.loc[index, 'Precp'] = weather_api_data_dict[row['STATION_ID']]["RAIN"]
            df_six_city_hot_spots.loc[index, 'VEHICLE_MAIN'] = vehicle
            df_six_city_hot_spots.loc[index, 'OBJ_GENDER'] = gender
            df_six_city_hot_spots.loc[index, 'OBJ_AGE'] = age
        except KeyError as e:
            df_six_city_hot_spots.loc[index, 'Precp'] = 0.00
    # 因為-998.00代表Precp為0所以將-998.00替換為0.00
    df_six_city_hot_spots['Precp'] = df_six_city_hot_spots['Precp'].replace("-998.00", "0.00")
    
    
    return df_six_city_hot_spots

# 餵進模型前的預處理
def preprocessing_for_feeding_model(df):

    # 讀取label_encoder
    with open("label_encoders.pickle", "rb") as f:
        label_encoders = pickle.load(f)

    # 讀取minmax_scaler
    with open("minmax_scaler.pkl", "rb") as f:
        scaler = pickle.load(f)

    # 建立留下的特徵
    features_to_keep = [
        "WHOLE_TIME",
        "CITY",
        "LIGHT",
        "Temperature",
        "WS",
        "RH",
        "Precp",
        "ROAD_TYPE_SUB1",
        "SIGNAL_TYPE",
        "VEHICLE_MAIN",
        "OBJ_GENDER",
        "OBJ_AGE",
        "CAMERA_ID",
        "EQUIP_TYPE"
    ]


    # 使用 df.drop() 將其餘特徵都 drop 掉
    df = df.drop(columns=[col for col in df.columns if col not in features_to_keep])

    # 將樹執行資料都先轉為數值
    df[["Temperature", "RH", "WS", "Precp"]] = df[
        ["Temperature", "RH", "WS", "Precp"]
    ].apply(pd.to_numeric, errors="coerce")

    # 將CAMERA_ID有值的欄位改成1
    df["CAMERA_ID"] = df["CAMERA_ID"].notna().astype(int)

    # 將CAMERA_ID沒有值的欄位改成0
    df["CAMERA_ID"] = df["CAMERA_ID"].fillna(0)

    # 將CAMERA_ID 0的部分改為無，1的部分改為有
    df["CAMERA_ID"] = df["CAMERA_ID"].map({0: "無", 1: "有"})
    df["EQUIP_TYPE"] = df["EQUIP_TYPE"].fillna("無")

    # 將GENDER的男女改成1和0
    df["OBJ_GENDER"] = df["OBJ_GENDER"].map({"男": 1, "女": 0})


    # 將資料中的"\n"替換為NaN
    df.replace("\n", np.nan, inplace=True)

    # 刪除包含空值的列
    df.dropna(inplace=True)

    # 用label_encoder轉換資料
    for column in df.select_dtypes(include="object"):
        df[column] = label_encoders[column].transform(df[column])

    # 使用MinMaxScaler對數值特徵進行最小-最大標準化
    df_numerical = df.select_dtypes(include=["int64", "float64"])
    df_numerical.fillna(df_numerical.mean(), inplace=True)

    # 在使用 MinMaxScaler 之前，記錄特徵的順序
    scaler.fit(df_numerical)

    # 將特徵進行最小-最大標準化並轉換資料
    df_numerical_scaled = scaler.transform(df_numerical)
    df[df_numerical.columns] = df_numerical_scaled

    # 定義新的特徵名稱順序
    new_feature_order = ['WHOLE_TIME', 'CITY', 'LIGHT', 'Temperature', 'WS', 'RH', 'Precp',
        'ROAD_TYPE_SUB1', 'SIGNAL_TYPE', 'VEHICLE_MAIN', 'OBJ_GENDER',
        'OBJ_AGE', 'CAMERA_ID', 'EQUIP_TYPE'] 


    # 使用 reindex 將 DataFrame 重新排序
    df = df.reindex(columns=new_feature_order)

    # 切割出 X_test，並在測試資料集中只保留訓練時使用的特徵
    X_test = df[new_feature_order]

    # 在測試資料集中只保留訓練時使用的特徵
    X_test = X_test[new_feature_order]

    return X_test

# 取的發生車禍的情況下會發生死亡車禍的機率
def get_probability(X_test, df_prob):

    # 載入模型
    with open("catboost_model.pkl", "rb") as f:
        model = pickle.load(f)

    # 一行一行進模型並加入機率(當時用整個df產生機率會有問題，所以改用一行一行預測機率)
    for index, row in X_test.iterrows():
        df_row = pd.DataFrame(row).transpose()
        y_prob = model.predict_proba(df_row)[:, 1]
        df_prob.loc[index, 'Probability'] = y_prob

    # 為了增加網頁上呈現的可讀性將小於0.01%的變成小於0.01%
    df_prob["Probability"] = df_prob["Probability"].apply(lambda x: f'{x*100:.2f}%' if x*100 > 0.001 else "小於0.01%")

    return df_prob

# 將預測完的肇事熱點從df轉成JSON格式
def get_six_city_hot_spots_json(df):

    # 使用 to_json 方法將 DataFrame 轉換為 JSON 字串
    json_str = df.to_json(orient='records')

    # 使用 json 模塊將 JSON 字串轉換為 JSON 對象
    json_obj = json.loads(json_str)

    # 使用 json.dumps 將 JSON 對象轉換回 JSON 字串，並設定 ensure_ascii=False
    json_str = json.dumps(json_obj, ensure_ascii=False)
    return json_str



# 以下為在其他檔案要import上面的方法的程式碼
if __name__ == "__main__":
    from instant_forecast_funtion import get_instant_weather_data , add_data_to_six_city_hot_spots, preprocessing_for_feeding_model, get_probability,get_six_city_hot_spots_json
    df_six_city_hot_spots = determine_the_csv_to_read()
    weather_api_data_dict = get_instant_weather_data()
    vehicle = "機車"
    gender = "男"
    age = "青年"
    df = add_data_to_six_city_hot_spots(df_six_city_hot_spots, weather_api_data_dict, vehicle, gender, age)
    df_prob = df
    X_test = preprocessing_for_feeding_model(df)
    df_prob = get_probability(X_test, df_prob)
    six_city_hot_spots_json = get_six_city_hot_spots_json(df_prob)
    print(six_city_hot_spots_json)