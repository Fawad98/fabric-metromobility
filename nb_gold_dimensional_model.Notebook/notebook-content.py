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

# dim_zone (from lookup)
zones = spark.table("bronze.taxi_zones") \
    .select(F.col("LocationID").alias("zone_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone_name"),
            F.col("service_zone"))
zones.write.mode("overwrite").saveAsTable("gold.dim_zone")

# dim_date
dates = spark.sql("""
    SELECT explode(sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)) AS date_key
""").select(
    "date_key",
    F.year("date_key").alias("year"),
    F.month("date_key").alias("month"),
    F.date_format("date_key", "MMMM").alias("month_name"),
    F.dayofweek("date_key").alias("day_of_week"),
    F.date_format("date_key", "EEEE").alias("day_name"),
    (F.dayofweek("date_key").isin(1, 7)).alias("is_weekend"))
dates.write.mode("overwrite").saveAsTable("gold.dim_date")

# dim_payment
payment = spark.createDataFrame(
    [(1, "Credit card"), (2, "Cash"), (3, "No charge"),
     (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")],
    ["payment_type_id", "payment_type_name"])
payment.write.mode("overwrite").saveAsTable("gold.dim_payment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact = (spark.table("silver.trips")
    .select(
        F.col("pickup_date").alias("date_key"),
        F.col("pickup_hour"),
        F.col("PULocationID").alias("pickup_zone_id"),
        F.col("DOLocationID").alias("dropoff_zone_id"),
        F.col("payment_type").cast("int").alias("payment_type_id"),
        "passenger_count", "trip_distance", "trip_duration_min",
        "avg_speed_mph", "fare_amount", "tip_amount", "total_amount"))

(fact.write.mode("overwrite")
    .partitionBy("date_key")
    .saveAsTable("gold.fact_trips"))

spark.sql("OPTIMIZE gold.fact_trips VORDER")

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
