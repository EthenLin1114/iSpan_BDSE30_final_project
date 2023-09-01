# 利用catboost訓練好的模型對歷年來的事故熱區做發生事故的情況下會發生死亡車禍的機率
將處理好的事故熱區資料串接中央氣象局的API並接收前端傳回來的車種、性別、年齡，並進行餵進模型前的欄位處理，再餵進catboost模型進行機率生成，並轉換成JSON檔，供前端使用

## 套件安裝
pip install requests==2.29.0
pip install pandas==1.5.3
pip install numpy==1.24.3
pip install scikit-learn==1.2.2

## 檔案說明
使用的兩個csv檔，是由https://roadsafety.tw/AcclocCbi 藉由CBI指標來做排名後並抓取107~112年每年的前十名，將這些肇事熱點的經緯度查出來加入資料，並寫程式找出歷史資料最近的事故地點並套用歷史資料的道路類型、燈光和標誌類型並區分為早上和晚上的檔案分別為SIX_CITY_new_hot_spot_morning.csv和SIX_CITY_new_hot_spot_night.csv
catboost_model.pkl為利用歷史資料訓練好的模型
label_encoders.pickle為在訓練模型的時候的label encoder
minmax_scaler.pkl為在訓練模型的時候的minmax scaler