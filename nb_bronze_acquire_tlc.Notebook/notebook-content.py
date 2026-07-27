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

import requests, os

months = [f"2024-{m:02d}" for m in range(1, 13)]
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
landing = "/lakehouse/default/Files/raw/yellow_taxi"

os.makedirs(landing, exist_ok=True)

for month in months:
    url = base_url.format(month)
    dest = f"{landing}/yellow_tripdata_{month}.parquet"
    if os.path.exists(dest):
        print(f"skip {month} (exists)")
        continue
    print(f"downloading {month} ...")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    with open(dest, "wb") as f:
        f.write(r.content)

print("done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
dest = "/lakehouse/default/Files/raw/reference/taxi_zone_lookup.csv"
import os; os.makedirs(os.path.dirname(dest), exist_ok=True)
open(dest, "wb").write(requests.get(url, timeout=60).content)
print("zone lookup saved")


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
