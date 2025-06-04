from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, col

def produce_topic(df: DataFrame, topic: str, bootstrap_servers: str) -> None:

    df_kafka_ready = df.withColumn(
        "value", concat_ws(",", *df.columns)
    ).selectExpr("CAST(value AS STRING)")

    df_kafka_ready.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", topic) \
        .save()
