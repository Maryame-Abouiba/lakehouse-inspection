from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder \
    .appName("01-Bronze-Ingestion") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.access-key-id", "adminn") \
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

# JSON missions
df_json = spark.read.option("multiLine", "true").json("/home/iceberg/data/raw/inspection-app/")
df_json.withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .writeTo("demo.bronze.inspection_missions").createOrReplace()
print("✅ bronze.inspection_missions")

# CSV capteurs
# CSV capteurs
df_csv = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("s3a://landing-zone/sensors/")
df_csv.withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .writeTo("demo.bronze.sensor_readings").createOrReplace()
print("✅ bronze.sensor_readings")

# GeoJSON HBIM
df_geo = spark.read.option("multiLine", "true").json("/home/iceberg/data/raw/hbim/")
df_geo.withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .writeTo("demo.bronze.hbim_zones").createOrReplace()
print("✅ bronze.hbim_zones")

spark.stop()
print("🎉 Bronze ingestion terminée")