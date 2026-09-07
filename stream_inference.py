import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import from_json, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

# 1. Initialize Spark with Kafka/Redpanda drivers
spark_version = pyspark.__version__
scala_version = "2.13" if spark_version.startswith("4") else "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

spark = (
    SparkSession.builder.appName("Live_Intrusion_Detection")
    .config("spark.jars.packages", kafka_package)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Load the Pre-Trained Brain
print("Loading Trained Random Forest Model from disk...")
model = RandomForestClassificationModel.load("ids_rf_model")

# 3. Connect to the Redpanda Edge Stream
print("Connecting to Redpanda Stream...")
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "192.168.29.72:9092")
    .option("subscribe", "cic-ids-telemetry")
    .load()
)

# 4. Define the Expected Network Data Schema
network_schema = StructType(
    [
        StructField("Destination Port", DoubleType(), True),
        StructField("Flow Duration", DoubleType(), True),
        StructField("Total Fwd Packets", DoubleType(), True),
        StructField("Total Backward Packets", DoubleType(), True),
        StructField("Packet Length Mean", DoubleType(), True),
        StructField("Average Packet Size", DoubleType(), True),
    ]
)

# 5. Parse the JSON Stream
parsed_stream = (
    df.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), network_schema).alias("data"))
    .select("data.*")
)

# 6. Assemble the Live Features
assembler = VectorAssembler(
    inputCols=[
        "Destination Port",
        "Flow Duration",
        "Total Fwd Packets",
        "Total Backward Packets",
        "Packet Length Mean",
        "Average Packet Size",
    ],
    outputCol="raw_features",
    handleInvalid="keep",
)
feature_stream = assembler.transform(parsed_stream)

# 7. Apply the Model in Real-Time (Inference)
predictions = model.transform(feature_stream)

# 8. Output the results (0.0 = Benign, 1.0 = Malicious)
query = (
    predictions.select("Destination Port", "prediction")
    .writeStream.outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()
