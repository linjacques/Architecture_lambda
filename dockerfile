FROM python:3.10-slim

# Variables d'environnement
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Installation des dépendances et de Spark
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && \
    curl -fL --retry 5 --retry-delay 5 https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar xvf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark.tgz

# Mise à jour du PATH
ENV PATH=$PATH:$SPARK_HOME/bin

# Installation de PySpark
RUN pip install pyspark==${SPARK_VERSION}

# Répertoire de travail
WORKDIR /app/src

# Commande par défaut
CMD ["python", "main.py"]
