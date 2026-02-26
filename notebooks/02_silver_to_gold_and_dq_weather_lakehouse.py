# Databricks notebook source
from pyspark.sql import functions as F
import json
from datetime import datetime

dt = "2026-02-25"

silver_in = f"dbfs:/Volumes/weather/weather/curated/silver/weather_hourly/dt={dt}/"
gold_out  = f"dbfs:/Volumes/weather/weather/curated/gold/weather_daily/dt={dt}/"
dq_dir    = f"dbfs:/Volumes/weather/weather/curated/quality/reports/dt={dt}/"

print("SILVER IN:", silver_in)
print("GOLD OUT:", gold_out)
print("DQ DIR:", dq_dir)

silver = spark.read.parquet(silver_in)
print("Silver rows:", silver.count())
display(silver.limit(10))

# COMMAND ----------

gold = (
    silver
    .groupBy("date","city_id","city_name","uf")
    .agg(
        F.avg("temperature_2m").alias("avg_temp"),
        F.max("wind_speed_10m").alias("max_wind"),
        F.sum("precipitation").alias("total_precip"),
        F.avg("relative_humidity_2m").alias("avg_humidity"),
    )
)

display(gold)

# COMMAND ----------

gold.write.mode("overwrite").parquet(gold_out)
print("GOLD OK ->", gold_out)
display(dbutils.fs.ls(gold_out))

# COMMAND ----------

dq = {
    "dt": dt,
    "rows": silver.count(),
    "null_time_utc": silver.filter(F.col("time_utc").isNull()).count(),
    "null_city_id": silver.filter(F.col("city_id").isNull()).count(),
    "temp_out_of_range": silver.filter((F.col("temperature_2m") < -20) | (F.col("temperature_2m") > 55)).count(),
    "humidity_out_of_range": silver.filter((F.col("relative_humidity_2m") < 0) | (F.col("relative_humidity_2m") > 100)).count(),
    "precip_negative": silver.filter(F.col("precipitation") < 0).count(),
    "wind_negative": silver.filter(F.col("wind_speed_10m") < 0).count(),
}
dq["ok"] = all(v == 0 for k, v in dq.items() if k not in ("dt","rows","ok"))

payload = {"generated_at_utc": datetime.utcnow().isoformat() + "Z", "checks": dq}

dbutils.fs.mkdirs(dq_dir)
dbutils.fs.put(dq_dir + "dq_report.json", json.dumps(payload, ensure_ascii=False, indent=2), overwrite=True)

print("DQ OK ->", dq_dir + "dq_report.json")
display(dbutils.fs.ls(dq_dir))