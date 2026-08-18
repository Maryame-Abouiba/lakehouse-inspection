from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, when

spark = SparkSession.builder \
    .appName("03-Gold-Aggregation") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://lakehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .config("spark.sql.catalog.demo.s3.access-key-id", "adminn") \
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

df_anomalies = spark.table("demo.silver.inspection_anomalies")
df_hbim = spark.table("demo.silver.hbim_zones_clean")
df_sensors = spark.table("demo.silver.sensor_readings_clean")

# KPI globaux
spark.sql("""
    SELECT COUNT(DISTINCT mission_id) AS total_missions, COUNT(*) AS total_anomalies,
           SUM(CASE WHEN criticality IN ('ÉLEVÉE','CRITIQUE') THEN 1 ELSE 0 END) AS critical_anomalies,
           COUNT(DISTINCT zone_id) AS zones_inspected
    FROM demo.silver.inspection_anomalies
""").writeTo("demo.gold.kpi_summary").createOrReplace()
print("✅ gold.kpi_summary")

# Par type / criticité
df_anomalies.groupBy("anomaly_type").agg(count("*").alias("nb_anomalies")) \
    .writeTo("demo.gold.anomalies_by_type").createOrReplace()
df_anomalies.groupBy("criticality").agg(count("*").alias("nb_anomalies")) \
    .writeTo("demo.gold.anomalies_by_criticality").createOrReplace()
print("✅ gold.anomalies_by_type / anomalies_by_criticality")

# Zone summary (croisement)
df_anom_zone = df_anomalies.groupBy("zone_id").agg(count("*").alias("nb_anomalies"))
df_env_zone = df_sensors.groupBy("zone_id").agg(spark_round(avg("temperature"), 1).alias("temp_moyenne"),
                                                  spark_round(avg("humidity"), 1).alias("humidite_moyenne"))
df_hbim.select("zone_id", "zone_name", "level", "material") \
    .join(df_anom_zone, "zone_id", "left") \
    .join(df_env_zone, "zone_id", "left") \
    .fillna(0, subset=["nb_anomalies"]) \
    .writeTo("demo.gold.zone_summary").createOrReplace()
print("✅ gold.zone_summary")

spark.stop()
print("🎉 Gold aggregation terminée")