# Notice we’re using pyspark.sql library here
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import logging 
import time

scala_version = '2.12'
spark_version = '3.1.2'

def postgres_config():
    dbhost = "localhost",
    dbport = 5432
    dbname = "test"
    dbuser = "postgres"
    dbpass = "postgres"
    url = "jdbc:postgresql://"+str(dbhost)+":"+str(dbport)+"/"+str(dbname)
    properties = {
        "driver": "org.postgresql.Driver",
        "user": dbuser,
        "password": dbpass
    }
    return url , properties
    
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
spark = SparkSession.builder \
        .master("local") \
        .config("spark.jars.packages", ",".join(packages))\
        .appName("CSV file loader") \
        .getOrCreate()

kafka_df = spark.readStream.format("kafka")\
                           .option("kafka.bootstrap.servers", "localhost:9092")\
                           .option("subscribe", "kafka-spark-integration")\
                           .option("startingOffsets", "earliest") \
                           .option("maxRatePerPartition" , 100) \
                           .option("maxOffsetsPerTrigger" , 10)\
                           .load()
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

jsonSchema = StructType([
                         StructField("username" , StringType() , True),
                         StructField("currency" , StringType() , True),
                         StructField("address" , StringType() , True),
                         StructField("amount" , IntegerType() , True),
                         StructField("id" , IntegerType() , True)
                         ])

json_df = kafka_df\
            .select(psf.from_json(psf.col('value'), jsonSchema).alias("kafka-spark-integration"))\
            .select("kafka-spark-integration.*")

# query = json_df\
#             .writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .start()



json_df.writeStream.outputMode("append")\
            .format("json")\
            .option("path", "/output_files/")\
            .option("header", True)\
            .option("checkpointLocation", "/output_files/checkpoint/")\
            .start()\
            .awaitTermination()

