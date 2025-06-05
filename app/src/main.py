import threading
from time import sleep
from pyspark.sql import SparkSession
from producer.topic_producer import produce_topic
from consumer.topic_consumer import consume_topic
from writer.topic_writer import write_topic_to
from data.generator.random_log import generate_log
from jobs.batch_layer import batch_layer
from jobs.speed_layer import speed_layer
import time


spark = SparkSession.builder.appName("architecture_lambda").getOrCreate()
topic = "website_log"
bootstrap_servers = "broker:29092"
schema = "timestamp STRING, ip STRING, user_agent STRING"
stream_output_dir = "data/speed_metrics"
batch_output_dir = "data/batch_metrics"


# --- Question 1 ---
def run_producer():
    while True:
        data = [generate_log() for _ in range(10)]
        df = spark.createDataFrame(data, ["timestamp", "ip", "user_agent"])
        produce_topic(df, topic=topic, bootstrap_servers=bootstrap_servers)


# --- Question 2 ---
def run_writer():
    while True:
        df_kafka = consume_topic(spark, topic=topic, bootstrap_servers=bootstrap_servers, streaming=False)

        df_value = df_kafka.selectExpr("CAST(value AS STRING) as log")

        df_parsed = df_value.selectExpr(
            "split(log, ',')[0] as timestamp",
            "split(log, ',')[1] as ip",
            "split(log, ',')[2] as user_agent"
        )

        write_topic_to(df_parsed, output_path="data/file_system/")
        time.sleep(3600)  # 1 heure


# --- Question 3 ---
# --- application batch ---
def run_batch():
    while True:
        batch_layer(
            spark=spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        time.sleep(3600)

# ---  application streaming ---
def run_speed():
    speed_layer( 
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        stream_output_dir=stream_output_dir
    )

# pour lancer toutes les fonctions en meme temps !
threads = [
    threading.Thread(target=run_producer),  
    threading.Thread(target=run_writer),
    threading.Thread(target=run_speed),
    threading.Thread(target=run_batch), # sauf celui là car on veut qu'il demarre apres run_speed
]

for t in threads:
    t.start()

for t in threads:
    t.join()  