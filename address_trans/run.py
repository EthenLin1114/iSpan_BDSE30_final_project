from address_trans import webdriver_setting, open_webdriver, determine_transform_successful, write_to_csv
from time import sleep
import pandas as pd

if __name__ == "__main__":
    # 抓經緯度的正規表達式
    latitude_and_longitude_r = r"@(-?\d+\.\d+),(-?\d+\.\d+),"

    # 更改為需要轉址的csv檔
    csv_trans = pd.read_csv("放入要轉址的csv檔", header=0)

    # 如果原始資料的縣市和地址欄位是分開才須修改此變數
    city_header_name = '縣市欄位名稱'

    # 修改為地址表頭名稱
    address_header_name = '地址欄位名稱'

    driver = webdriver_setting()

    for i in range (len(csv_trans[f'{address_header_name}'])):

        # 如果原始資料的縣市和地址欄位是分開的請改為以下程式
        address = f"{csv_trans[f'{city_header_name}'][i]}{csv_trans[f'{address_header_name}'][i]}"

        # 如果原始資料的縣市與地址是合在一起的請更改以下的程式
        address = f"{csv_trans[f'{address_header_name}'][i]}"

        # 將地址串接網址
        url = f"https://www.google.com/maps/place/{address}"

        # 將串接好的網址進行查詢
        open_webdriver(driver, url)

        # 因為要等待網址出現經緯度
        sleep(6)

        # 判斷是否轉址成功，成功會直接對資料進行新經緯度，並印出成功字串，失敗則會將抓不到經緯度的地址印出來
        determine_transform_successful(driver, csv_trans, i ,latitude_and_longitude_r)
        
        # 每10筆做一次儲存(可更改次數)
        if (i%10 == 0):
            write_to_csv(csv_trans)

        
    # 最後儲存csv檔
    write_to_csv(csv_trans)
                
    