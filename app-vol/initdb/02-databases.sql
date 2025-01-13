drop DATABASE if EXISTS airflowdb;
CREATE DATABASE airflowdb;
GRANT ALL PRIVILEGES ON DATABASE airflowdb TO bn_airflow;

drop DATABASE if EXISTS taskdb;
CREATE DATABASE taskdb;
GRANT ALL PRIVILEGES ON DATABASE taskdb TO bn_airflow, testus;
