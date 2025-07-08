# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "466e2184-c19b-4669-a1ea-0398d3683846",
# META       "default_lakehouse_name": "Vscode",
# META       "default_lakehouse_workspace_id": "36952dd4-29ea-4c9a-85fa-b35583ab33cc",
# META       "known_lakehouses": [
# META         {
# META           "id": "466e2184-c19b-4669-a1ea-0398d3683846"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import time
import pandas as pd
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

file_path = "watermark.txt"

if os.path.exists(file_path):
    os.remove(file_path)
    print("File deleted.")
else:
    print("File does not exist.")
#s

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

# ✅ Step 1: Get max crash_date
max_crash_date = spark.sql("SELECT CAST(MAX(crash_date) AS DATE) as max_date FROM nyc_crashes").collect()[0]["max_date"]
print("📅 Max crash date:", max_crash_date)

# ✅ Step 2: Convert to datetime
max_crash_datetime = datetime.combine(max_crash_date, datetime.min.time())

# ✅ Step 3: Define schema
schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("last_watermark", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# ✅ Step 4: Create DataFrame with datetime
data = [("nyc_crashes", max_crash_datetime, datetime.utcnow())]
df = spark.createDataFrame(data, schema)

# ✅ Step 5: Write to table
df.write.mode("overwrite").saveAsTable("extraction_watermark")
print("✅ Watermark table created and populated.")
print(f"Max Datetime: {max_crash_datetime} ")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# === CONFIGURATION ===
BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
LIMIT = 1000
TABLE_NAME = "nyc_crashes"
WATERMARK_TABLE = "extraction_watermark"

SELECTED_COLUMNS = [
    "collision_id", "crash_date", "crash_time", "borough",
    "latitude", "longitude", "number_of_persons_injured",
    "number_of_persons_killed", "contributing_factor_vehicle_1",
    "vehicle_type_code1"
]

# === 1. Get last watermark from watermark table ===
print("🔍 Getting last watermark...")

try:
    watermark_df = spark.sql(f"SELECT last_watermark FROM {WATERMARK_TABLE} WHERE table_name = '{TABLE_NAME}'")
    last_watermark = watermark_df.collect()[0]["last_watermark"]
except Exception as e:
    print(f"⚠️ Could not get watermark, defaulting to 2023-01-01T00:00:00. Reason: {e}")
    last_watermark = "2023-01-01T00:00:00"

print(f"⏱️ Watermark used: {last_watermark}")

# === 2. Begin Data Extraction ===
all_data = []
extraction_time = datetime.utcnow().isoformat()
offset = 0

while True:
    formatted_watermark = str(last_watermark).replace(" ", "T")
    params = {
        "$limit": LIMIT,
        "$offset": offset,
        "$select": ",".join(SELECTED_COLUMNS),
        "$where": f"crash_date > '{formatted_watermark}'"
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if not data:
        print("✅ No new data to fetch.")
        break

    for record in data:
        record["extraction_timestamp"] = extraction_time

    all_data.extend(data)
    offset += LIMIT
    time.sleep(0.5)

# === 3. Convert & Save Data ===
if all_data:
    print(f"💾 Total new records fetched: {len(all_data)}")

    df = pd.DataFrame(all_data)
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("append").saveAsTable(TABLE_NAME)

    # === 4. Update Watermark ===
    max_crash_date = max(record["crash_date"] for record in all_data)
    print(f"🆕 Updating watermark to: {max_crash_date}")

    watermark_update_df = spark.createDataFrame(
        [("nyc_crashes", max_crash_date, datetime.utcnow())],
        ["table_name", "last_watermark", "updated_at"]
    ).withColumn("last_watermark", to_timestamp("last_watermark"))

    try:
        existing_df = spark.sql(f"SELECT * FROM {WATERMARK_TABLE}").filter(f"table_name != '{TABLE_NAME}'")
        final_df = existing_df.union(watermark_update_df)
    except Exception as e:
        print(f"⚠️ Could not read existing watermark table. Creating a new one. Reason: {e}")
        final_df = watermark_update_df

    final_df.write.mode("overwrite").saveAsTable(WATERMARK_TABLE)
    print("✅ Watermark table updated.")
else:
    print("⚠️ No new records to insert. Watermark not changed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Vscode.nyc_crashes")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import trunc, count

df.groupBy('extraction_timestamp') \
  .agg(count('*').alias('count')) \
  .orderBy('extraction_timestamp') \
  .show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df

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
