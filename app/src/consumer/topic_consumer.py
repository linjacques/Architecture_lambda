from pyspark.sql import SparkSession

def consume_topic(spark: SparkSession, topic: str,  bootstrap_servers: str):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
