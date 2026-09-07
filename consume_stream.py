import pyspark
from pyspark.sql import SparkSession

# Dynamically detect your Spark and Scala versions
spark_version = pyspark.__version__
scala_version = "2.13" if spark_version.startswith("4") else "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

print(f"Booting Spark {spark_version} using Scala {scala_version} drivers...")

# Initialize Spark Session and pull the EXACT matching connector
spark = (
    SparkSession.builder.appName("RedpandaConsumer")
    .config("spark.jars.packages", kafka_package)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Connect to the Redpanda broker
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "192.168.29.72:9092")
    .option("subscribe", "cic-ids-telemetry")
    .option("startingOffsets", "latest")
    .load()
)

# The data comes in as binary bytes; cast it to a readable string
parsed_stream = df.selectExpr("CAST(value AS STRING) as json_payload")

# Output the live stream directly to the console
query = parsed_stream.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
