import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

print("Booting Spark for MLlib Training...")

# Forcing aggressive memory limits and garbage collection
spark = (
    SparkSession.builder.appName("IDS_Model_Training")
    .config("spark.driver.memory", "8g")
    .config("spark.memory.fraction", "0.8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# FIX 1: inferSchema=False prevents the massive initial memory spike
print("Loading dataset metadata...")
df = spark.read.csv(
    "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv", header=True, inferSchema=False
)

for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.strip())

features = [
    "Destination Port",
    "Flow Duration",
    "Total Fwd Packets",
    "Total Backward Packets",
    "Packet Length Mean",
    "Average Packet Size",
]

# FIX 2: Immediately drop the 72 columns we don't need BEFORE doing any math
df = df.select(*features, "Label")

# FIX 3: Downsample to 5% (more than enough for this proof-of-concept)
print("Downsampling and pruning columns...")
df = df.sample(withReplacement=False, fraction=0.05, seed=42)
df = df.dropna()

# Now cast just our 6 surviving columns to double
for f in features:
    df = df.withColumn(f, df[f].cast("double"))

assembler = VectorAssembler(
    inputCols=features, outputCol="raw_features", handleInvalid="skip"
)
assembled_df = assembler.transform(df)

indexer = StringIndexer(inputCol="Label", outputCol="label_index", handleInvalid="skip")
indexed_df = indexer.fit(assembled_df).transform(assembled_df)

train_data, test_data = indexed_df.randomSplit([0.8, 0.2], seed=42)

print("Training Random Forest on lightweight dataset...")
rf = RandomForestClassifier(
    featuresCol="raw_features", labelCol="label_index", numTrees=15, maxDepth=5
)
model = rf.fit(train_data)

predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index", predictionCol="prediction", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy: {accuracy * 100:.2f}%")

print("Saving model to disk...")
model.write().overwrite().save("ids_rf_model")
print("Done! The model is ready.")
