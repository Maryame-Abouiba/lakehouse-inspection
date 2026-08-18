from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date, to_timestamp, trim, initcap, upper, size, expr

spark = SparkSession.builder \
    .appName("02-Silver-Transform") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.access-key-id", "adminn") \
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

# Anomalies (déplier + normaliser)
df_missions = spark.table("demo.bronze.inspection_missions")
df_missions.select(
        col("mission_id"), to_date(col("inspection_date"), "yyyy-MM-dd").alias("inspection_date"),
        col("inspector_id"), col("zone_id"), explode(col("anomalies")).alias("anomaly")
    ).select(
        col("mission_id"), col("inspection_date"), col("inspector_id"), col("zone_id"),
        col("anomaly.anomaly_id").alias("anomaly_id"), col("anomaly.element_id").alias("element_id"),
        initcap(trim(col("anomaly.type"))).alias("anomaly_type"),
        upper(trim(col("anomaly.criticality"))).alias("criticality")
    ).filter(col("mission_id").isNotNull() & col("zone_id").isNotNull()) \
    .writeTo("demo.silver.inspection_anomalies").createOrReplace()
print("✅ silver.inspection_anomalies")

# Capteurs (nettoyer)
df_sensors = spark.table("demo.bronze.sensor_readings")
df_sensors_clean = df_sensors.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("temperature").isNotNull() & col("humidity").isNotNull()) \
    .filter((col("temperature") > -20) & (col("temperature") < 60)) \
    .filter((col("humidity") >= 0) & (col("humidity") <= 100))
df_sensors_clean.writeTo("demo.silver.sensor_readings_clean").createOrReplace()
print("✅ silver.sensor_readings_clean")

# HBIM (valider géométries)
df_hbim = spark.table("demo.bronze.hbim_zones")
df_hbim.select(explode(col("features")).alias("feature")) \
    .select(
        upper(trim(col("feature.properties.zone_id"))).alias("zone_id"),
        col("feature.properties.zone_name").alias("zone_name"),
        col("feature.properties.level").alias("level"),
        col("feature.properties.material").alias("material"),
        col("feature.geometry.type").alias("geometry_type"),
        col("feature.geometry.coordinates").alias("coordinates")
    ).filter(col("zone_id").isNotNull()) \
    .writeTo("demo.silver.hbim_zones_clean").createOrReplace()
print("✅ silver.hbim_zones_clean")

spark.stop()
print("🎉 Silver transform terminée")