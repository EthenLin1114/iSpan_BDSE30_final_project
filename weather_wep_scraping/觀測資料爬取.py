#請先將selenium版本設定為3.141.0版
# !pip install selenium == 3.141.0

# 操作 browser 的 API
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

# 處理逾時例外的工具
from selenium.common.exceptions import TimeoutException

from selenium.common.exceptions import ElementNotInteractableException

# 面對動態網頁，等待某個元素出現的工具，通常與 exptected_conditions 搭配
from selenium.webdriver.support.ui import WebDriverWait

# 搭配 WebDriverWait 使用，對元素狀態的一種期待條件，若條件發生，則等待結束，往下一行執行
from selenium.webdriver.support import expected_conditions as EC

# 期待元素出現要透過什麼方式指定，通常與 EC、WebDriverWait 一起使用
from selenium.webdriver.common.by import By

from selenium.common.exceptions import NoSuchElementException

# 強制等待 (執行期間休息一下)
from time import sleep

# 執行 command 的時候用的
import os

from selenium.webdriver.support.ui import Select

# 啟動瀏覽器工具的選項
my_options = webdriver.ChromeOptions()
my_options.add_argument("--start-maximized")         #最大化視窗
my_options.add_argument("--incognito")               #開啟無痕模式
my_options.add_argument("--disable-popup-blocking") #禁用彈出攔截
my_options.add_argument("--disable-notifications")  #取消 chrome 推播通知
my_options.add_argument("--lang=zh-TW")  #設定為正體中文

# 使用 Chrome 的 WebDriver(請先到https://chromedriver.chromium.org/downloads下載與自己Chrome版本相符的chromedriver)
# 將driver_path變更為chromedriver.exe的所在路徑
driver_path = r'C:\\Users\\Ethen\\iSpan\\project\\GitHub程式\\chromedriver.exe'
driver = webdriver.Chrome(executable_path=driver_path, options=my_options)


# 建立儲存檔案的資料夾
folderPath = '台中市資料'
if not os.path.exists(folderPath):
    os.makedirs(folderPath)

# os.getcwd()為抓到當前目錄的路徑，windows的路徑會用\做分隔，但在Python裡\為跳脫字元，所以下面要放\\
fullDownloadPath = os.getcwd() + '\\' + folderPath

# 設定WebDriver的行為，"prefs"為偏好設定
my_options.add_experimental_option("prefs", {
    
    # 將fullDownloadPath的路徑設定為默認下載檔案會放的路徑
    "download.default_directory": fullDownloadPath,

    # 可以設定要不要禁用詢問如何處理下載的提示。
    "download.prompt_for_download": False,

    # 在訪問危險網站或下載危險文件時，不會收到任何警告(須確定網頁是否安全再關閉這個功能)
    "safebrowsing.enabled": False,
})

# 走訪頁面
def visit():
    driver.get('https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=467770&stname=%25E6%25A2%25A7%25E6%25A3%25B2&datepicker=2013-01-01&altitude=31.73m')

# 手動或其他原因停止執行此方法
def visit1(url):
    #將網址會成停止的日期的測站網址
    driver.get(url)

# 將csv下載下來
def download_weather_csv(stop_point):
    
    try:
        # 等待元素出現
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located( 
                (By.CSS_SELECTOR, "select#selectStno") 
            )
        )
        
        # 找到下拉式選單元素
        select_element = driver.find_element(By.CSS_SELECTOR, "select#selectStno")

        # 創建 Select 對象
        select = Select(select_element)

        # 獲取下拉式選單的選項数量
        options_count = len(select.options)
        
        sleep(1)
        
        #如果停止將0變為停止的數字
        for j in range(stop_point,options_count+1):
            print(j)
            # 等待元素出現
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located( 
                    (By.CSS_SELECTOR, "select#selectStno") 
                )
            )
            
            # 找到下拉式選單元素
            select_element = driver.find_element(By.CSS_SELECTOR, "select#selectStno")

            # 創建 Select 對象
            select = Select(select_element)

            # 選擇選項
            select.select_by_index(j)

            while True:
                try:
                    # 等待CSV下載的按件出現
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located( 
                            (By.CSS_SELECTOR, "a#downloadCSV input") 
                        )
                    )

                    # 點擊CSV下載的按件
                    driver.find_element(
                        By.CSS_SELECTOR, 
                        "a#downloadCSV input"
                    ).click()

                # 等待下一天的按件出現
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located( 
                            (By.CSS_SELECTOR, "a#nexItem input") 
                        )
                    )
                    # 點擊下一天的按件
                    driver.find_element(
                        By.CSS_SELECTOR, 
                        "a#nexItem input"
                    ).click()

                    
                except ElementNotInteractableException:
                    break
                
            visit()
                
            sleep(1)
        
    except TimeoutException:
        print("等待逾時")


    # 最一開始執行的主程式
# if __name__ == '__main__':
#     visit()
#     download_weather_csv(0)

# 停止後主程式
if __name__ == '__main__':
    url = 'https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=467770&stname=%25E6%25A2%25A7%25E6%25A3%25B2&datepicker=2013-01-01&altitude=31.73m'#"放入停止時的網址"
    visit1(url)
    #放入停止的數字
    download_weather_csv(0)#"停止的數字"