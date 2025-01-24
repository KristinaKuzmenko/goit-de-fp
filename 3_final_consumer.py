from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from configs import kafka_config
import os


# package for reading with Kafka from Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages '
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,'
    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4 '
    'pyspark-shell')

# create Spark Session
spark = (SparkSession.builder
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints-consumer")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .appName("KafkaStreaming")
    .master("local[*]")
    .getOrCreate())

my_name = "kry"
topic = f'{my_name}_enriched_athlete_avg'

schema = (StructType([
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("avg_height", DoubleType(), True),
            StructField("avg_weight", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
        ]))

# read final results from kafka topic
def read_from_kafka(topic, schema):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol", kafka_config['security_protocol']) \
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
        .option("kafka.sasl.jaas.config",
                    f"org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_config['username']} password={kafka_config['password']};") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "5") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")
    return df

df = read_from_kafka(topic, schema)

query = (
    df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()