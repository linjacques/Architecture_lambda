from pyspark.sql import SparkSession
from producer.topic_producer import produce_topic
from data.generator.random_log import generate_log
from writer.topic_writer import write_topic_to
from consumer.topic_consumer import consume_topic

spark = SparkSession.builder.appName("Architecture lambda").getOrCreate()
topic="transaction_log"
bootstrap_servers="broker:29092"

# Question 1 
data = [generate_log() for _ in range(10)] 
columns = ["timestamp", "ip", "user_agent"]
df = spark.createDataFrame(data, columns)
produce_topic(df, topic=topic, bootstrap_servers=bootstrap_servers)


# Question 2
website_topic = consume_topic(spark, bootstrap_servers=bootstrap_servers, topic=topic)
df_value = website_topic.selectExpr("CAST(value AS STRING) as log")
df_parsed = df_value.selectExpr(
        "split(log, ',')[0] as timestamp",
        "split(log, ',')[1] as ip",
        "split(log, ',')[2] as user_agent"
    )
query = write_topic_to(df_parsed, output_path="../src/data/file_system")
query.awaitTermination()