# ============================================================
# SPARK NODE IMAGE
# Lightweight image for Spark master/worker nodes
# ============================================================
FROM python:3.11-slim as spark-base

# --- INSTALACIÓN DE JAVA Y SPARK ---
RUN apt-get update && \
    apt-get install -y default-jdk wget curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Descargar e instalar Apache Spark
ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3
RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Pre-instalar JARs de AWS/Hadoop
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV WILDFLY_OPENSSL_VERSION=1.0.7.Final

WORKDIR /opt/spark/jars
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/${WILDFLY_OPENSSL_VERSION}/wildfly-openssl-${WILDFLY_OPENSSL_VERSION}.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR /opt/spark

# Copiar solo requirements de Spark (mínimos)
COPY deps/requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

# Sin copiar todo el proyecto - solo lo necesario de Spark
# Los workers solo ejecutan tareas distribuidas

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
