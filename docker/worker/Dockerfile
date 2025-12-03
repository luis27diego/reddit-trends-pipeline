# ==========================================
# PREFECT WORKER IMAGE
# Includes Spark capabilities for job submission
# ==========================================
FROM python:3.11-slim

# --- INSTALACIÓN DE JAVA Y SPARK ---
RUN apt-get update && \
    apt-get install -y default-jdk wget git curl && \
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

WORKDIR /app

# Copiar solo requirements de worker (con Prefect + Spark)
COPY deps/requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt

# Copiar solo los directorios necesarios (NO todo el proyecto)
COPY flows/ /app/flows/
COPY src/ /app/src/

CMD ["prefect", "worker", "start", "--pool", "default"]