import requests
import pandas as pd
from datetime import datetime
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
import pickle
import json
import catboost


def determine_the_csv_to_read():
    now_time = datetime.now()
    # 獲取當前的小時
    current_hour = now_time.hour

    if current_hour >= 6 and current_hour <= 17:
        df = pd.read_csv(
            "./app/MachineLearning/Catboost/SIX_CITY_new_hot_spot_morning.csv",  encoding="UTF-8")
    else:
        df = pd.read_csv("./app/MachineLearning/Catboost/SIX_CITY_new_hot_spot_night.csv",
                         encoding="UTF-8")

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

    response = requests.get(url_automatic_station,
                            headers=headers, params=params)

    # 取得JSON格式的回應內容
    automatic_station_data = response.json()
    response = requests.get(url_automatic_station_rain,
                            headers=headers, params=params)

    # 取得JSON格式的回應內容
    automatic_station_rain_data = response.json()

    needs_elements = {"TEMP", "WDSD", "HUMD"}

# 儲存所有天氣測站的所需的值
    all_station_needs_values_dict = {}

    for location in automatic_station_data['records']['location']:
        # 儲存這個 location 的元素值
        element_values = {}

        # 抓出weatherElement裡面需要的欄位
        for element in location['weatherElement']:
            if element['elementName'] in needs_elements:
                # element_values的Key是elementName，值是elementValue{'WDSD': '6.8', 'TEMP': '24.9', 'HUMD': '0.94'}
                element_values[element['elementName']
                               ] = element['elementValue']
        # stationId 為key,value是element_values{'C0AC60': {'WDSD': '0.3', 'TEMP': '29.0', 'HUMD': '0.83'}}
        all_station_needs_values_dict[location['stationId']] = element_values

    for location in automatic_station_rain_data['records']['location']:
        # 儲存這個 location 的元素值
        for element in location['weatherElement']:
            if element['elementName'] == "RAIN":
                all_station_needs_values_dict[location['stationId']
                                              ]["RAIN"] = element['elementValue']
    # print(all_station_needs_values_dict) {'C0AC60': {'WDSD': '0.3', 'TEMP': '29.0', 'HUMD': '0.83', 'RAIN': '-998.00'}}
    return all_station_needs_values_dict


def add_data_to_six_city_hot_spots(df_six_city_hot_spots, weather_api_data_dict, vehicle, gender, age):

    # 獲取當前的日期和時間
    now_time = datetime.now()

    # 獲取當前的小時
    current_hour = now_time.hour

    # 獲取即時天氣api需要的值
    df_six_city_hot_spots['STATION_ID'] = df_six_city_hot_spots['STATION_ID'].astype(
        str)

    # pandas中的iterrows()方法來遍歷DataFrame df_six_city_hot_spots 中的每一行資料，並根據每一行的 'STATION_ID' 值，更新 'WHOLE_TIME' 和 'Temperature' 等欄位的值
    for index, row in df_six_city_hot_spots.iterrows():
        try:
            df_six_city_hot_spots.loc[index, 'WHOLE_TIME'] = current_hour
            df_six_city_hot_spots.loc[index,
                                      'Temperature'] = weather_api_data_dict[row['STATION_ID']]["TEMP"]
            df_six_city_hot_spots.loc[index,
                                      'WS'] = weather_api_data_dict[row['STATION_ID']]["WDSD"]
            df_six_city_hot_spots.loc[index,
                                      'RH'] = weather_api_data_dict[row['STATION_ID']]["HUMD"]
            df_six_city_hot_spots.loc[index,
                                      'Precp'] = weather_api_data_dict[row['STATION_ID']]["RAIN"]
            df_six_city_hot_spots.loc[index, 'VEHICLE_MAIN'] = vehicle
            df_six_city_hot_spots.loc[index, 'OBJ_GENDER'] = gender
            df_six_city_hot_spots.loc[index, 'OBJ_AGE'] = age
        except KeyError as e:
            df_six_city_hot_spots.loc[index,
                                      'Precp'] = 0.00
    df_six_city_hot_spots['WHOLE_TIME'] = df_six_city_hot_spots['WHOLE_TIME'].astype(
        int)
    df_six_city_hot_spots['Precp'] = df_six_city_hot_spots['Precp'].replace(
        "-998.00", "0.00")

    return df_six_city_hot_spots


def preprocessing_for_feeding_model(df):
    with open("./app/MachineLearning/Catboost/label_encoders.pickle", "rb") as f:
        label_encoders = pickle.load(f)

    with open("./app/MachineLearning/Catboost/minmax_scaler.pkl", "rb") as f:
        scaler = pickle.load(f)

    # 去除特徵
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
    df = df.drop(
        columns=[col for col in df.columns if col not in features_to_keep])

    # 轉換為數值型態，"coerce" 表示當出現錯誤時，將錯誤值轉換為 NaN（Not a Number）
    # df["SPEED_LIMIT"] = df["SPEED_LIMIT"].astype(int)
    df[["Temperature", "RH", "WS", "Precp"]] = df[
        ["Temperature", "RH", "WS", "Precp"]
    ].apply(pd.to_numeric, errors="coerce")
    df["CAMERA_ID"] = df["CAMERA_ID"].notna().astype(int)

    df["CAMERA_ID"] = df["CAMERA_ID"].fillna(0)
    df["CAMERA_ID"] = df["CAMERA_ID"].map({0: "無", 1: "有"})
    df["EQUIP_TYPE"] = df["EQUIP_TYPE"].fillna("無")
    # df["ACCIDENT_TYPE"] = df["ACCIDENT_TYPE"].map({"A1": 1, "A2": 0})
    df["OBJ_GENDER"] = df["OBJ_GENDER"].map({"男": 1, "女": 0})

    # 將資料中的"\n"替換為NaN
    df.replace("\n", np.nan, inplace=True)

    # 刪除包含空值的列
    df.dropna(inplace=True)

    # 轉換資料
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


def get_probability(X_test, df_prob):

    # 載入模型
    with open("./MachineLearning/Catboost/catboost_model.pkl", "rb") as f:
        model = pickle.load(f)

    for index, row in X_test.iterrows():
        df_row = pd.DataFrame(row).transpose()
        y_prob = model.predict_proba(df_row)[:, 1]
        df_prob.loc[index, 'Probability'] = y_prob

    df_prob['CITY'] = df_prob['CITY'].replace(["臺北市", "新北市", "桃園市", "臺中市", "臺南市", "高雄市"], [
                                              "TPE", "NTP", "TY", "TC", "TN", "KS"])
    # 機率*100, 機率>0.01 就要顯示機率, 小於
    df_prob["Probability1"] = df_prob["Probability"].apply(
        lambda x: f'{x*100:.2f}%' if x*100 > 0.001 else "小於0.01%")

    times_series = df_prob['TIMES']
    # 使用cut函數將times欄位的值分為兩個區間（兩群），分為兩個群，bins=[1, 4, 7]代表切點為1和4，即[1, 4)和[4, 7)
    df_prob['GROUP'] = pd.cut(
        times_series, bins=[0, 3, 7], labels=['Yellow', 'Red'])

    # print(df_prob)

    return df_prob


def get_six_city_hot_spots_json(df):
    # 使用 to_json 方法將 DataFrame 轉換為 JSON 字串
    json_str = df.to_json(orient='records')

    # 使用 json 模塊將 JSON 字串轉換為 JSON 對象
    json_obj = json.loads(json_str)

    # 使用 json.dumps 將 JSON 對象轉換回 JSON 字串，並設定 ensure_ascii=False
    json_str = json.dumps(json_obj, ensure_ascii=False)
    return json_str


# 以下為在其他檔案要import上面的方法的程式碼
# if __name__ == "__main__":
#     from instant_forecast_funtion import get_instant_weather_data, add_data_to_six_city_hot_spots, preprocessing_for_feeding_model, get_probability, get_six_city_hot_spots_json
#     df_six_city_hot_spots = determine_the_csv_to_read()
#     weather_api_data_dict = get_instant_weather_data()
#     vehicle = "機車"
#     gender = "女"
#     age = "中年"
#     df = add_data_to_six_city_hot_spots(
#         df_six_city_hot_spots, weather_api_data_dict, vehicle, gender, age)
#     df_prob = df
#     X_test = preprocessing_for_feeding_model(df)
#     df_prob = get_probability(X_test, df_prob)
#     six_city_hot_spots_json = get_six_city_hot_spots_json(df_prob)
#     print(df_prob["Probability"])

    # print(six_city_hot_spots_json)
