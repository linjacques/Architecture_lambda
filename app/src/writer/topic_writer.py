from pyspark.sql.functions import to_timestamp, date_format

def write_topic_to(df, output_path="data/file_system/", checkpoint_path="data/file_system/checkpoints"):
    df = df.withColumn("timestamp", to_timestamp("timestamp"))
    df = df.withColumn("hour_partition", date_format("timestamp", "yyyy-MM-dd-HH"))

    return df.write \
        .partitionBy("hour_partition") \
        .option("header", True) \
        .mode("append") \
        .json(output_path)
