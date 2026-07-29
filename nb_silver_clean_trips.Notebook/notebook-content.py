# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "40eb20df-bc83-406d-b160-cec3d6f5e54e",
# META       "default_lakehouse_name": "lh_mobility",
# META       "default_lakehouse_workspace_id": "8b9653df-c525-4a85-bd61-90fd49df0c04",
# META       "known_lakehouses": [
# META         {
# META           "id": "40eb20df-bc83-406d-b160-cec3d6f5e54e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# Workaround for Fabric optimizer issue with TimestampNTZType
spark.conf.set("spark.sql.cbo.enabled", "false")

df = spark.table("bronze.yellow_trips")

# Convert timestamp_ntz to standard Spark timestamp
df = (
    df
    .withColumn(
        "tpep_pickup_datetime",
        F.col("tpep_pickup_datetime").cast("timestamp")
    )
    .withColumn(
        "tpep_dropoff_datetime",
        F.col("tpep_dropoff_datetime").cast("timestamp")
    )
)

df_clean = (
    df
    .filter(
        F.col("tpep_pickup_datetime") >=
        F.lit("2024-01-01 00:00:00").cast("timestamp")
    )
    .filter(
        F.col("tpep_pickup_datetime") <
        F.lit("2025-01-01 00:00:00").cast("timestamp")
    )
    .filter(
        F.col("tpep_dropoff_datetime") >
        F.col("tpep_pickup_datetime")
    )
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("trip_distance") < 200)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("passenger_count").between(1, 8))
    .withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp("tpep_dropoff_datetime") -
            F.unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )
    .filter(F.col("trip_duration_min").between(1, 480))
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )
    .withColumn(
        "avg_speed_mph",
        F.round(
            F.col("trip_distance") /
            (F.col("trip_duration_min") / 60.0),
            2
        )
    )
    .dropDuplicates([
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "total_amount"
    ])
)

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_date")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.trips")
)

print(f"silver.trips created with {df_clean.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_cnt = spark.table("bronze.yellow_trips").count()
clean_cnt = spark.table("silver.trips").count()

dq = spark.createDataFrame([{
    "run_ts": str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
    "bronze_rows": raw_cnt,
    "silver_rows": clean_cnt,
    "rejected_rows": raw_cnt - clean_cnt,
    "rejection_pct": round(100.0 * (raw_cnt - clean_cnt) / raw_cnt, 2),
}])
dq.write.mode("append").saveAsTable("silver.dq_trip_metrics")
display(spark.table("silver.dq_trip_metrics"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("OPTIMIZE silver.trips VORDER")
spark.sql("DESCRIBE DETAIL silver.trips").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
