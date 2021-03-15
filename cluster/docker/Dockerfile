FROM adoptopenjdk/openjdk11

RUN apt-get update && apt-get install -y \
 wget && \
 cd /home && mkdir spark && cd spark && \
 wget https://apache-mirror.rbc.ru/pub/apache/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz && \
 tar -xvf spark-3.1.1-bin-hadoop2.7.tgz

ENTRYPOINT $ENTRY_POINT
