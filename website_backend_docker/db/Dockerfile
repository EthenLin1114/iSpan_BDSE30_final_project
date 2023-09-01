FROM mysql/mysql-server 

RUN microdnf update && microdnf install sudo curl wget zip tree iputils hostname findutils procps less nano net-tools && \
microdnf clean all  

COPY my.cnf /etc/mysql/my.cnf
COPY ./init.sql /docker-entrypoint-initdb.d 

EXPOSE 3306
