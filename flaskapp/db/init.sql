CREATE USER 'teamone'@'%' IDENTIFIED BY 'teamone';

ALTER USER 'teamone' IDENTIFIED WITH mysql_native_password BY 'teamone';

GRANT ALL PRIVILEGES ON *.* TO 'teamone'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;

