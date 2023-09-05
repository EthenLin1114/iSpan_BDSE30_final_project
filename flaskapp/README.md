# 在GCP上開啟專題網站
將這個資料夾上傳到GCP的虛擬主機上後執行以下動作:
1. cd ~/flaskapp
2. sudo docker-compose build
3. sudo docker-compose up (如果有更新flaskapp內的東西才需要加上 --build)
4. 將back.zip解壓後將back.sql移到~/flaskapp/data/mysql/db/內
5. docker exec -it db env LANG=C.UTF-8 /bin/bash
6. mysql -u root -pteamone
7. CREATE USER 'teamone'@'%' IDENTIFIED BY 'teamone'; ALTER USER 'teamone' IDENTIFIED WITH mysql_native_password BY 'teamone'; GRANT ALL PRIVILEGES ON *.* TO 'teamone'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES;
8. create database teamone;
9. exit;
10. mysql -u root -pteamone teamone < back.sql
11. 建立預存程序
    DELIMITER //
    CREATE PROCEDURE RETURN_LL (
        IN city VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci, 
        IN inMonth INT, 
        IN inYear INT, 
        IN intype VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    )
    BEGIN
        CASE city
            WHEN '臺北市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_tp
                WHERE inMonth= sepdate_tp.Month AND inYear= sepdate_tp.Year AND intype = ACCIDENT_TYPE;    
            WHEN '新北市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_np
                WHERE inMonth= sepdate_np.Month AND inYear= sepdate_np.Year AND intype = ACCIDENT_TYPE;
                WHEN '桃園市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_ty
                WHERE inMonth= sepdate_ty.Month AND inYear= sepdate_ty.Year AND intype = ACCIDENT_TYPE;
            WHEN '臺中市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_tc
                WHERE inMonth= sepdate_tc.Month AND inYear= sepdate_tc.Year AND intype = ACCIDENT_TYPE;
                WHEN '臺南市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_tn
                WHERE inMonth= sepdate_tn.Month AND inYear= sepdate_tn.Year AND intype = ACCIDENT_TYPE;
            WHEN '高雄市' THEN
                SELECT LONGITUDE, LATITUDE, ACCIDENT_DEAD, ACCIDENT_INJURY FROM sepdate_ks
                            WHERE inMonth= sepdate_ks.Month AND inYear= sepdate_ks.Year AND intype = ACCIDENT_TYPE;
            ELSE
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'Invalid city name.';
        END CASE;
    END //
    DELIMITER;
## 檔案說明
請先將flaskapp/app/data/json.zip解壓縮