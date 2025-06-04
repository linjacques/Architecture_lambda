from pyspark.sql import SparkSession
from producer.topic_producer import produce_topic
from data.generator.random_log import generate_log

spark = SparkSession.builder.appName("Architecture lambda").getOrCreate()


data = [generate_log() for _ in range(10)] 
columns = ["timestamp", "ip", "user_agent"]

df = spark.createDataFrame(data, columns)
produce_topic(df, topic="transaction_log", bootstrap_servers="broker:29092")
