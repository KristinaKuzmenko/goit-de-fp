from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from configs import mysql_config, kafka_config
import os

# package for reading with Kafka from Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages '
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,'
    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4 '
    'pyspark-shell')

# create Spark Session
spark = (SparkSession.builder
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .appName("JDBCToKafka")
    .master("local[*]")
    .getOrCreate())

# function for reading data from mysql
def read_from_mysql(jdbc_url, table_name):
    df= (spark.read.format('jdbc').options(
        url=jdbc_url,
        driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
        dbtable=table_name,
        user=mysql_config['username'],
        password=mysql_config['password'])
         .load())
    return df

#function for writing data to kafka topic
def write_to_kafka(df, topic):
    try:
        (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
         .option("kafka.security.protocol", kafka_config['security_protocol'])
         .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
         .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_config['username']} password={kafka_config['password']};")
         .option("topic", topic)
         #.option("checkpointLocation", "/tmp/checkpoints")
         .save())
    except Exception as e:
        print(f'An error occurred while writing to Kafka topic {topic}: {str(e)}')

# function for reading data from kafka topic
def read_from_kafka(topic, schema):
    df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
        .option("kafka.security.protocol", kafka_config['security_protocol'])
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
        .option("kafka.sasl.jaas.config",
                    f"org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_config['username']} password={kafka_config['password']};")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5")
        .load() \
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data")))
    return df

# function for writing data to mysql
def write_to_mysql(df, jdbc_url, table_name):
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", mysql_config['username']) \
        .option("password", mysql_config['password']) \
        .mode("append") \
        .save()

# forEachBatch function for writing data to kafka topic and to mysql simultaneously
def foreach_batch_function(batch_df):
    write_to_kafka(batch_df, topic_2)
    write_to_mysql(batch_df, jdbc_url_w,"krystyna_k_athlete_enriched_agg")

# mysql configs
jdbc_url_r = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_url_w = "jdbc:mysql://217.61.57.46:3306/neo_data"

# kafka topic names
my_name = "kry"
topic_1 = f'{my_name}_athlete_event_results'
topic_2 = f'{my_name}_enriched_athlete_avg'

# 1. read athlete physical data and 2. filter df from nulls and values which are not numbers
athlets_bio_df = read_from_mysql(jdbc_url_r, "athlete_bio").filter(
            (col("height").isNotNull())
            & (col("weight").isNotNull())
            & (col("height").cast("double").isNotNull())
            & (col("weight").cast("double").isNotNull()))


# 3.1 read from Mysql athlete event results
df = read_from_mysql(jdbc_url_r, "athlete_event_results")

# 3.2 write data from athlete event results to kafka topic
write_to_kafka(df, topic_1)

# 3.3 read event results from kafka topic
schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True),
])

event_results_df = read_from_kafka(topic_1, schema).select("data.athlete_id", "data.sport", "data.medal")

# 4. join athlete physical data with event results by athlete_id and 5. calculate average height and weight for each sport,medal,sex,country_noc combination
joined_df = (athlets_bio_df.join(event_results_df, on='athlete_id')
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("timestamp")
            ))


# 6. stream data to kafka topic and mysql with foreachBatch
(joined_df.writeStream
 .outputMode("update")
 .foreachBatch(foreach_batch_function)
 .option("checkpointLocation", "/tmp/checkpoints-stream")
 .start()
 .awaitTermination())
