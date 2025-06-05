def consume_topic(spark, topic, bootstrap_servers, streaming=True):
    reader = spark.readStream if streaming else spark.read
    df = reader \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic)
    
    if streaming:
        df = df.option("startingOffsets", "latest")
    
    return df.load()
