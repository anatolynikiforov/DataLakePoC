FROM spark-base:latest

RUN cd /home && mkdir hadoop && cd hadoop && mkdir -p data/namenode data/datanode data/tmp && \
  wget https://apache-mirror.rbc.ru/pub/apache/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz && \
  tar -xvf hadoop-3.2.2.tar.gz && \
  rm hadoop-3.2.2.tar.gz

RUN apt-get update && apt-get install -y vim openssh-client openssh-server

# not for production!
RUN echo 'root:root' | chpasswd
RUN mkdir -p /user/root

ENV HADOOP_HOME=/home/hadoop/hadoop-3.2.2
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

COPY config/hadoop/ $HADOOP_HOME/etc/hadoop/
COPY config/ssh_keys /root/.ssh
COPY config/ssh_keys/id_rsa.pub /root/.ssh/authorized_keys

RUN chmod 400 /root/.ssh/id_rsa
RUN $HADOOP_HOME/bin/hadoop namenode -format

ENTRYPOINT service ssh start && $ENTRY_POINT
