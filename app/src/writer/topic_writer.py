def write_topic_to(df, output_path = str, checkpoint_path="data/file_system/checkpoints"):
    return df.writeStream \
        .format("csv") \
        .option("header", True) \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 hour") \
        .outputMode("append") \
        .start()
