FROM python:3.10-slim

ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y openjdk-17-jdk curl && \
    curl -fL --retry 5 --retry-delay 5 https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar xvf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark.tgz

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

ENV PATH=$PATH:$SPARK_HOME/bin

RUN pip install pyspark==${SPARK_VERSION}

WORKDIR /app/src

CMD ["python", "main.py"]
