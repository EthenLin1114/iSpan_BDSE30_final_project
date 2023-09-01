from instant_forecast_funtion import get_instant_weather_data , add_data_to_six_city_hot_spots, preprocessing_for_feeding_model, get_probability,get_six_city_hot_spots_json,determine_the_csv_to_read

if __name__ == "__main__":
    df_six_city_hot_spots = determine_the_csv_to_read()
    weather_api_data_dict = get_instant_weather_data()
    vehicle = "請改為前端輸出的值"
    gender = "請改為前端輸出的值"
    age = "請改為前端輸出的值"
    df = add_data_to_six_city_hot_spots(df_six_city_hot_spots, weather_api_data_dict, vehicle, gender, age)
    df_prob = df
    X_test = preprocessing_for_feeding_model(df)
    df_prob = get_probability(X_test, df_prob)
    six_city_hot_spots_json = get_six_city_hot_spots_json(df_prob)
    print(six_city_hot_spots_json)