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

from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T

# ---------------------------------------------------------------
# 1. One-time initialization (idempotent — won't wipe history on re-run)
# ---------------------------------------------------------------
if not spark.catalog.tableExists("gold.dim_zone_scd2"):
    spark.sql("""
        CREATE TABLE gold.dim_zone_scd2 AS
        SELECT zone_id, borough, zone_name, service_zone,
               current_timestamp() AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to,
               true AS is_current
        FROM gold.dim_zone
    """)
    print("initialized gold.dim_zone_scd2")
else:
    print("gold.dim_zone_scd2 already exists — skipping init")

# ---------------------------------------------------------------
# 2. Simulated incoming change: zone 132 renamed
#    Cast columns to match the target table's schema exactly.
#    zone_id is bigint (from inferSchema on the zone lookup CSV),
#    so the int literal 132 must be cast to long or the append fails
#    with DELTA_FAILED_TO_MERGE_FIELDS.
# ---------------------------------------------------------------
updates = spark.createDataFrame(
    [(132, "Queens", "JFK Airport - Terminal Complex", "Airports")],
    ["zone_id", "borough", "zone_name", "service_zone"]
).withColumn("zone_id", F.col("zone_id").cast(T.LongType()))

tgt = DeltaTable.forName(spark, "gold.dim_zone_scd2")

# ---------------------------------------------------------------
# 3. Close the current record (expire the old version)
# ---------------------------------------------------------------
(tgt.alias("t").merge(
        updates.alias("s"),
        "t.zone_id = s.zone_id AND t.is_current = true AND t.zone_name <> s.zone_name")
    .whenMatchedUpdate(set={
        "valid_to": F.current_timestamp(),
        "is_current": F.lit(False)})
    .execute())

# ---------------------------------------------------------------
# 4. Insert the new version — via MERGE so re-running the cell
#    doesn't create duplicate current rows for the same change.
#    Only inserts when no current row already matches this zone_name.
# ---------------------------------------------------------------
new_rows = (updates
    .withColumn("valid_from", F.current_timestamp())
    .withColumn("valid_to", F.lit(None).cast(T.TimestampType()))
    .withColumn("is_current", F.lit(True)))

(tgt.alias("t").merge(
        new_rows.alias("s"),
        "t.zone_id = s.zone_id AND t.is_current = true AND t.zone_name = s.zone_name")
    .whenNotMatchedInsertAll()
    .execute())

# ---------------------------------------------------------------
# 5. Verify: should show one expired row + one current row for zone 132
# ---------------------------------------------------------------
display(spark.sql("""
    SELECT * FROM gold.dim_zone_scd2
    WHERE zone_id = 132
    ORDER BY valid_from
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.mlv_daily_borough_revenue
# MAGIC AS
# MAGIC SELECT d.date_key, z.borough,
# MAGIC        COUNT(*) AS trip_count,
# MAGIC        ROUND(SUM(f.total_amount), 2) AS total_revenue,
# MAGIC        ROUND(AVG(f.trip_distance), 2) AS avg_distance
# MAGIC FROM gold.fact_trips f
# MAGIC JOIN gold.dim_zone z ON f.pickup_zone_id = z.zone_id
# MAGIC JOIN gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.date_key, z.borough

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
