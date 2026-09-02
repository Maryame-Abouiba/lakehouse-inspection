"""
Script de streaming Kafka -> Landing Zone (MinIO local)
Consomme le topic 'sensor-readings' en continu et écrit les mesures
sous forme de fichiers CSV dans /home/iceberg/data/raw/sensors/
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

# --- 1. SparkSession ---
spark = SparkSession.builder \
    .appName("Kafka-Streaming-Sensors") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.access-key-id", "adminn") \
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "adminn") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Session Spark Streaming prête")

# --- 2. Schéma des messages JSON venant de Kafka ---
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("zone_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType())

# --- 3. Lecture du flux Kafka ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-readings") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

print("✅ Flux Kafka connecté, schéma appliqué")

# --- 4. Écriture continue dans la Landing Zone (même dossier que ton Bronze lit) ---
query = parsed_stream.writeStream \
    .format("csv") \
    .option("path", "s3a://landing-zone/sensors/") \
    .option("checkpointLocation", "s3a://landing-zone/_checkpoints/sensors_streaming/") \
    .option("header", "true") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✅ Streaming démarré — écriture continue dans /home/iceberg/data/raw/sensors/")

# --- 5. Bloque le script indéfiniment (comportement voulu pour un job streaming) ---
query.awaitTermination()