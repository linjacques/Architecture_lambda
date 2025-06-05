from pyspark.sql.functions import col, split, to_timestamp, to_date, count

def batch_layer(spark, bootstrap_servers, topic, output_dir="data/batch_metrics"):
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    df_value = df_kafka.selectExpr("CAST(value AS STRING) as log")

    df_parsed = df_value.select(
        split(col("log"), ",")[0].alias("timestamp_str"),
        split(col("log"), ",")[1].alias("ip"),
        split(col("log"), ",")[2].alias("user_agent")
    ).withColumn("timestamp", to_timestamp("timestamp_str"))

    df_ip = df_parsed.groupBy("ip").agg(count("*").alias("nb_connexions"))
    df_ip.write.mode("overwrite").option("header", True).csv(f"{output_dir}/by_ip")

    df_ua = df_parsed.groupBy("user_agent").agg(count("*").alias("nb_connexions"))
    df_ua.write.mode("overwrite").option("header", True).csv(f"{output_dir}/by_agent")

    df_day = df_parsed.withColumn("date", to_date("timestamp")) \
                      .groupBy("date") \
                      .agg(count("*").alias("nb_connexions"))
    df_day.write.mode("overwrite").option("header", True).json(f"{output_dir}/by_day")
