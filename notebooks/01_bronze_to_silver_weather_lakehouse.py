# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG weather;
# MAGIC USE SCHEMA weather;
# MAGIC
# MAGIC SHOW VOLUMES;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dt = "2026-02-25"

bronze_dir = f"dbfs:/Volumes/weather/weather/raw/bronze/openmeteo/dt={dt}/"
silver_out = f"dbfs:/Volumes/weather/weather/curated/silver/weather_hourly/dt={dt}/"

print("BRONZE:", bronze_dir)
print("SILVER:", silver_out)

display(dbutils.fs.ls(bronze_dir))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dt = "2026-02-25"
bronze_dir = f"dbfs:/Volumes/weather/weather/raw/bronze/openmeteo/dt={dt}/"

df = (
    spark.read.json(bronze_dir + "*.json")
    .select("*", "_metadata")  # precisa selecionar explicitamente
    .withColumn("_file_path", F.col("_metadata.file_path"))
    .withColumn("_file_name", F.col("_metadata.file_name"))
    .drop("_metadata")
)

w = Window.partitionBy(F.col("city.city_id")).orderBy(F.col("run_ts").desc())
df_latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

print("Arquivos lidos:", df.count())
print("Após dedupe (1 por cidade):", df_latest.count())

display(df_latest.select("run_ts", "city.city_id", "city.name", "_file_name").orderBy("city.city_id"))

# COMMAND ----------

silver = (
    df_latest
    .select(
        F.col("city.city_id").alias("city_id"),
        F.col("city.name").alias("city_name"),
        F.col("city.uf").alias("uf"),
        F.explode(
            F.arrays_zip(
                F.col("response.hourly.time").alias("time"),
                F.col("response.hourly.temperature_2m").alias("temperature_2m"),
                F.col("response.hourly.relative_humidity_2m").alias("relative_humidity_2m"),
                F.col("response.hourly.precipitation").alias("precipitation"),
                F.col("response.hourly.wind_speed_10m").alias("wind_speed_10m"),
            )
        ).alias("z")
    )
    .select(
        "city_id","city_name","uf",
        F.to_timestamp(F.col("z.time")).alias("time_utc"),
        F.col("z.temperature_2m").cast("double").alias("temperature_2m"),
        F.col("z.relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
        F.col("z.precipitation").cast("double").alias("precipitation"),
        F.col("z.wind_speed_10m").cast("double").alias("wind_speed_10m"),
    )
    .withColumn("date", F.to_date("time_utc"))
)

print("Silver rows:", silver.count())
display(silver.limit(20))

# COMMAND ----------

silver.write.mode("overwrite").parquet(silver_out)
print("SILVER OK ->", silver_out)
display(dbutils.fs.ls(silver_out))