import csv
import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
# 操作 browser 的 API
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# ChromeDriver 的下載管理工具
from webdriver_manager.chrome import ChromeDriverManager
# 處理逾時例外的工具
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementNotInteractableException
# 面對動態網頁，等待某個元素出現的工具，通常與 exptected_conditions 搭配
from selenium.webdriver.support.ui import WebDriverWait
# 搭配 WebDriverWait 使用，對元素狀態的一種期待條件，若條件發生，則等待結束，往下一行執行
from selenium.webdriver.support import expected_conditions as EC
# 期待元素出現要透過什麼方式指定，通常與 EC、WebDriverWait 一起使用s
from selenium.webdriver.common.by import By
from time import sleep

def webdriver_setting():
    # 啟動瀏覽器工具的選項
    my_options = webdriver.ChromeOptions()
    # my_options.add_argument("--headless")             #不開啟實體瀏覽器背景執行
    my_options.add_argument("--start-maximized")        #最大化視窗
    my_options.add_argument("--incognito")              #開啟無痕模式
    my_options.add_argument("--disable-popup-blocking") #禁用彈出攔截
    my_options.add_argument("--disable-notifications")  #取消 chrome 推播通知
    my_options.add_argument("--lang=zh-TW")             #設定為正體中文
    my_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36')

    driver_path = r'C:\\Users\\Ethen\\iSpan\\project\\GitHub程式\\address_trans\\chromedriver.exe'
    driver = webdriver.Chrome(executable_path=driver_path, options=my_options)
    return driver
    
# 打開input的網址
def open_webdriver(driver,url):
    driver.get(url)

# 判斷是否轉址成功
def determine_transform_successful(driver, csv_trans,i , latitude_and_longitude_r):
    title = driver.title
    if (title != "Google 地圖"):
        current_url = driver.current_url
        match = re.search(latitude_and_longitude_r, current_url)
        match_Latitude = match.group(2)
        match_Longitude = match.group(1)
        csv_trans.loc[i, '緯度'] = match_Longitude
        csv_trans.loc[i, '經度'] = match_Latitude
        print("第{}筆座標轉換成功,{},{}".format(i,match_Longitude,match_Latitude))
    else:
        print("{}座標轉換失敗,索引值為{}".format(address,i))

def write_to_csv(dataframe):
    dataframe.to_csv("地址轉座標.csv", index=False, encoding="utf_8_sig")
    

if __name__ == "__main__":
    latitude_and_longitude_r = r"@(-?\d+\.\d+),(-?\d+\.\d+),"
    csv_trans = pd.read_csv("example.csv", header=0)
    city_header_name = '縣市'
    address_header_name = '地點'
    driver = webdriver_setting()
    for i in range (len(csv_trans[f'{address_header_name}'])):
        address = f"{csv_trans[f'{city_header_name}'][i]}{csv_trans[f'{address_header_name}'][i]}"
        address = f"{csv_trans[f'{address_header_name}'][i]}"
        url = f"https://www.google.com/maps/place/{address}"
        open_webdriver(driver,url)
        sleep(6)
        determine_transform_successful(driver, latitude_and_longitude_r)
        if (i%10 == 0):
            write_to_csv(csv_trans)
    write_to_csv(csv_trans)