# Intro

# Components

1. redpanda
2. jobmanager
3. taskmanager
4. postgres



Nota:
- en la parte donde se corre el script start_job.py desde docker, el comando mostrado en el ejemplo esta incorrecto, le falta la parte de /src

dice:

docker-compose exec jobmanager ./bin/flink run -py /opt/job/start_job.py -d

debe decir:

docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py -d




Comandos:

Para correr el proceso de ingesta

docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py


para correr el producer

(dataeng) maxkaizo@max:~/DE_Zoomcamp/module_6/workshop/src/producers$ ls
load_taxi_data.py  producer.py


para correr el stack de docker

(dataeng) maxkaizo@max:~/DE_Zoomcamp/module_6/workshop$ pwd
/home/maxkaizo/DE_Zoomcamp/module_6/workshop
(dataeng) maxkaizo@max:~/DE_Zoomcamp/module_6/workshop$ docker compose up




