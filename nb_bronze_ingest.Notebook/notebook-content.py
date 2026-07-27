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

from pyspark.sql import Row

config = [
    Row(source_path="Files/raw/yellow_taxi/*.parquet", fmt="parquet",
        target_schema="bronze", target_table="yellow_trips", load_mode="overwrite"),
    Row(source_path="Files/raw/reference/taxi_zone_lookup.csv", fmt="csv",
        target_schema="bronze", target_table="taxi_zones", load_mode="overwrite"),
]

spark.createDataFrame(config).write.mode("overwrite") \
    .saveAsTable("bronze.ingest_config")
display(spark.table("bronze.ingest_config"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

cfg_rows = spark.table("bronze.ingest_config").collect()

for row in cfg_rows:
    print(f"loading {row.target_schema}.{row.target_table} from {row.source_path}")
    reader = spark.read
    if row.fmt == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    df = reader.format(row.fmt).load(row.source_path)

    df = (df
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))

    (df.write.mode(row.load_mode)
       .option("overwriteSchema", "true")
       .saveAsTable(f"{row.target_schema}.{row.target_table}"))

    cnt = spark.table(f"{row.target_schema}.{row.target_table}").count()
    print(f"  -> {cnt:,} rows")

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
