docker cp .\build\libs\MapReduce-1.0-SNAPSHOT.jar namenode:/tmp/
docker cp .\0.csv namenode:/tmp/
docker cp .\1.csv namenode:/tmp/
docker cp .\2.csv namenode:/tmp/
docker cp .\3.csv namenode:/tmp/
docker cp .\4.csv namenode:/tmp/
docker cp .\5.csv namenode:/tmp/
docker cp .\6.csv namenode:/tmp/
docker cp .\7.csv namenode:/tmp/

# Запустить bash контейнера
docker exec -it namenode /bin/bash
cd tmp/

hdfs dfs -mkdir -p /user/root/input
hdfs dfs -put *.csv /user/root/input

hadoop jar /tmp/MapReduce-1.0-SNAPSHOT.jar input output 1 60000
hadoop fs -rm -r /user/root/output
hadoop fs -rm -r /user/root/output_temp

# Чтобы убедиться, что файл скопирован успешно, используйте команду hadoop fs -ls:
hadoop fs -ls /user/root/output

# Для копирования файла из HDFS на локальный компьютер используется команда hadoop fs -get:
hadoop fs -get /user/root/output/part-r-00000 ./part-r-00000

#Предположим, у вас есть контейнер с ID abc123 и вы хотите скопировать файл /app/data.txt из контейнера на локальный компьютер в текущую директорию:
docker cp namenode:/tmp/part-r-00000 ./part-r-00000

