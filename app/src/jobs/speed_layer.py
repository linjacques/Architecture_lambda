from pyspark.sql.functions import col, split, count, to_timestamp, window

def speed_layer(spark, stream_output_dir, topic, bootstrap_servers):
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    df_value = df_kafka.selectExpr("CAST(value AS STRING) as log")

    df_parsed = df_value.select(
        split(col("log"), ",")[0].alias("timestamp_str"),
        split(col("log"), ",")[1].alias("ip"),
        split(col("log"), ",")[2].alias("user_agent")
    ).withColumn("timestamp", to_timestamp("timestamp_str"))

    df_timed = df_parsed.withWatermark("timestamp", "2 seconds")

    # Crée les 3 DataFrames de métriques
    df_ip = df_timed.groupBy(
        window("timestamp", "30 seconds"), col("ip")
    ).agg(count("*").alias("nb_connexions"))

    df_ua = df_timed.groupBy(
        window("timestamp", "30 seconds"), col("user_agent")
    ).agg(count("*").alias("nb_connexions"))

    df_day = df_timed.groupBy(
        window("timestamp", "1 day")
    ).agg(count("*").alias("nb_connexions"))

    # Écriture en parallèle via Spark Structured Streaming
    queries = [
        df_ip.writeStream
            .format("json")
            .outputMode("append")
            .option("path", f"{stream_output_dir}/by_ip")
            .option("checkpointLocation", f"{stream_output_dir}/by_ip/_checkpoints")
            .start(),

        df_ua.writeStream
            .format("json")
            .outputMode("append")
            .option("path", f"{stream_output_dir}/by_agent")
            .option("checkpointLocation", f"{stream_output_dir}/by_agent/_checkpoints")
            .start(),

        df_day.writeStream
            .format("json")
            .outputMode("append")
            .option("path", f"{stream_output_dir}/by_day")
            .option("checkpointLocation", f"{stream_output_dir}/by_day/_checkpoints")
            .start()
    ]

    # Attend que l'un d'eux échoue (sinon ça tourne à l'infini)
    spark.streams.awaitAnyTermination()
