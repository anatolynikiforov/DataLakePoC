FROM adoptopenjdk/openjdk11

RUN apt-get update && apt-get install -y \
 wget && \
 cd /home && mkdir spark && cd spark && \
 wget https://apache-mirror.rbc.ru/pub/apache/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
 tar -xvf spark-3.1.1-bin-hadoop3.2.tgz && \
 rm spark-3.1.1-bin-hadoop3.2.tgz

ARG aws_access_key_id
ARG aws_secret_access_key

ENV AWS_ACCESS_KEY_ID=$aws_access_key_id
ENV AWS_SECRET_ACCESS_KEY=$aws_secret_access_key

ENV SPARK_HOME=/home/spark/spark-3.1.1-bin-hadoop3.2

COPY config/spark-defaults.conf $SPARK_HOME/conf

# download packages specified in spark-defaults.conf
RUN $SPARK_HOME/bin/spark-shell
