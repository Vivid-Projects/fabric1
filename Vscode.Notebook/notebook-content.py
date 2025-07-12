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

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS fact_error_event (
# MAGIC     table_name STRING,
# MAGIC     error_timestamp TIMESTAMP,
# MAGIC     error_type STRING,
# MAGIC     severity STRING,
# MAGIC     columns_checked STRING,
# MAGIC     row_identifier STRING,
# MAGIC     error_description STRING,
# MAGIC     collision_id STRING,
# MAGIC     extraction_timestamp string
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Vscode.nyc_crashes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *


df = df.withColumn(
    "crash_time", 
    date_format(to_timestamp("crash_time", "H:mm"), "HH:mm:ss")  # allows single-digit hour
).withColumn(
    "extraction_timestamp", 
    to_timestamp("extraction_timestamp")  # automatically handles ISO8601 like "2025-07-04T12:33:34.912482"
)
df = df.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
df = df.withColumn("number_of_persons_injured", col("number_of_persons_injured").cast("int"))
df = df.withColumn("number_of_persons_killed", col("number_of_persons_killed").cast("int"))
df = df.withColumn("latitude", col("latitude").cast("float"))
df = df.withColumn("longitude", col("longitude").cast("float"))
display(df.printSchema())




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##checking for null counts in all columns 

from pyspark.sql.functions import * 

null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

numeric_types = (IntegerType, LongType, DoubleType, FloatType, DecimalType)

numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, numeric_types)]

df.select(numeric_cols).describe().show()
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **As you can see there is an invalid value in the latitude column with 0.0000 record as min
# 
# Also in the longitude column as max**

# CELL ********************

df_valuecheck = df.filter((col("latitude") == 0.0000000) & (col("longitude") == 0.0000000))
display(df_valuecheck.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter rows that have at least one null value
rows_with_nulls = df.filter(
    " OR ".join([f"{c} IS NULL" for c in df.columns])
)

rows_with_nulls.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

# Step 1: Extract null rows from nyc_crashes
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE latitude IS NULL and longitude IS NULL
""").withColumn("columns_checked", 
      lit("log$lat")
).withColumn("error_type", 
      lit("NULL_CHECK")
).withColumn("severity", 
      lit("High")
).withColumn("error_description", 
      lit("Nulls found in log & lat")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "NULL_CHECK"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of nulls records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## dropping the nulls in the bronze table since its already on the error fact table 
df_copy=df.na.drop(subset=['longitude','latitude'])
#df_copy.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

# Step 1: Extract invalid values from nyc_crashes
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE latitude =0.0 and longitude = 0.0
""").withColumn("columns_checked", 
      lit("log$lat")
).withColumn("error_type", 
      lit("Value_check")
).withColumn("severity", 
      lit("High")
).withColumn("error_description", 
      lit("Invalid log and lat")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "Value_check"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of invalid records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_copy = df_copy.filter((col("latitude") != 0.0000000) & (col("longitude") != 0.0000000))
print(f"Total of {df_copy.count()} is now valid in the dataset")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## checking the if the value in the location column if its valid

df_copy.select("latitude").describe().show()
df_copy.select("longitude").describe().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## checkking if there is still out of value records 
df_copyy = df_copy.filter((col("latitude") == 0.0) & (col("longitude") == 0.0))
display(df_copyy)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Extract null rows from nyc_crashes in the borough column
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE borough IS NULL
""").withColumn("columns_checked", 
      lit("borough")
).withColumn("error_type", 
      lit("Null_check")
).withColumn("severity", 
      lit("Meduim")
).withColumn("error_description", 
      lit("Nulls found in borough")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "Null_check"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of nulls records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Define columns to check for duplicates
duplicate_check_cols = ["crash_date", "borough", "vehicle_type_code1", "collision_id", "latitude", "longitude", "crash_time"]

# Step 2: Identify duplicate keys
duplicate_keys = (
    spark.table("nyc_crashes")
    .groupBy(duplicate_check_cols)
    .count()
    .filter(col("count") > 1)
    .drop("count")
)


# Ensure extraction_timestamp is present and properly typed
if "extraction_timestamp" not in null_rows.columns:
    null_rows = null_rows.withColumn("extraction_timestamp", lit(None).cast(TimestampType()))
else:
    #null_rows = null_rows.withColumn("extraction_timestamp", col("extraction_timestamp").cast(TimestampType()))
    pass

# Step 3: Get full duplicate rows
duplicate_rows = (
    spark.table("nyc_crashes")
    .join(duplicate_keys, on=duplicate_check_cols, how="inner")
    .withColumn("columns_checked", lit(",".join(duplicate_check_cols)))
    .withColumn("error_type", lit("Duplicat_check"))
    .withColumn("severity", lit("Low"))
    .withColumn("error_description", lit("Duplicate record found"))
    .withColumn("error_timestamp", current_timestamp())
    .withColumn("table_name", lit("nyc_crashes"))
    .withColumn("row_identifier", concat_ws("-", *duplicate_check_cols))
)

# Step 4: Load existing duplicate error logs
existing_duplicates = spark.table("fact_error_event").filter(
    col("error_type") == "Duplicat_check"
).select("row_identifier", "columns_checked", "error_type")

# Step 5: Filter out already logged duplicates
new_duplicates = duplicate_rows.join(
    existing_duplicates,
    on=["row_identifier", "columns_checked", "error_type"],
    how="left_anti"
).select(
    "table_name",
    "error_timestamp",
    "error_type",
    "severity",
    "columns_checked",
    "row_identifier",
    "error_description",
    "collision_id",
    "extraction_timestamp"  # ensure collision_id is included
)

# Step 6: Write new duplicate errors to fact table
if new_duplicates.count() > 0:
    inserted_count = duplicate_rows.count()
    new_duplicates.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of duplicate records inserted: {inserted_count}")
else:
    print('No new duplicate errors found')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## dropping duplicates since its already inserted into the fact error table 

df = df_copy.dropDuplicates(["crash_date", "borough", "vehicle_type_code1", "collision_id", "latitude", "longitude", "crash_time"]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

## Step 1: Extract null rows from nyc_crashes in the factor&vehicle column
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE contributing_factor_vehicle_1 IS NULL
""").withColumn("columns_checked", 
      lit("Factor")
).withColumn("error_type", 
      lit("Null_check")
).withColumn("severity", 
      lit("Meduim")
).withColumn("error_description", 
      lit("Nulls found in factor&vehicle")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "Null_check"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of nulls records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.fillna('Unknown', subset=['contributing_factor_vehicle_1'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws

## Step 1: Extract null rows from nyc_crashes in the factor&vehicle column
null_rows = spark.sql("""
    SELECT * FROM nyc_crashes
    WHERE vehicle_type_code1 IS NULL 
""").withColumn("columns_checked", 
      lit("Vehicle_type")
).withColumn("error_type", 
      lit("Null_check")
).withColumn("severity", 
      lit("Meduim")
).withColumn("error_description", 
      lit("Nulls found in factor&vehicle")
).withColumn("error_timestamp", 
      current_timestamp()
).withColumn("table_name", 
      lit("nyc_crashes")
).withColumn("row_identifier", 
      concat_ws("-", *spark.table("nyc_crashes").columns)
)

# Step 2: Load existing error fact rows
existing_errors = spark.table("fact_error_event").filter(
    col("error_type") == "Null_check"
).select("row_identifier", "columns_checked", "error_type")

# Step 3: Anti-join to exclude already logged issues
new_errors = null_rows.join(existing_errors, 
    on=["row_identifier", "columns_checked", "error_type"], 
    how="left_anti"
).select(
    "table_name", 
    "error_timestamp", 
    "error_type", 
    "severity",
    "columns_checked", 
    "row_identifier", 
    "error_description", 
    "collision_id",
    'extraction_timestamp'  # include this field
)

# Step 4: Append only new error rows
if new_errors.count() > 0:
    inserted_count = new_errors.count()    
    new_errors.write.mode("append").format("delta").saveAsTable("fact_error_event")
    print(f"Number of nulls records inserted: {inserted_count}")
else:
    print('No new errors identified')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df = df.fillna('Unknown', subset=['vehicle_type_code1'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## checking if there is still duplicates


df.groupBy(duplicate_check_cols).count().filter("count > 1").show()

## nothing will be returned if there is no duplicate.



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_error = spark.sql("SELECT * FROM Vscode.fact_error_event")
df_error.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
## checking the error distribution and error event timestamp


error_df =df_error.groupBy('error_timestamp',
    'error_type', 
    'columns_checked',
    'severity',
    to_date(col("extraction_timestamp")).alias('extraction_date')
    ).agg(count('*').alias('count'))


error_df = error_df.withColumn("error_type", initcap("error_type"))
display(error_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

error_df.write.mode('overwrite').format('delta').saveAsTable('Error_Counts')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## loading the geojson file to fill in the borough.

dff = spark.read.option("multiline", "true").json("Files/nyc-boroughs.json")
# df now is a Spark DataFrame containing JSON data from "Files/nyc-boroughs.json".
display(dff.printSchema())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## checking the columns

dff.selectExpr("features[0].geometry.coordinates").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## extracting the lon-lat co_ordinate from the json file loaded
# Define UDF to get first coordinate from Polygon
def extract_lon_lat(coords):
    try:
        return coords[0][0]  # [lon, lat]
    except Exception:
        return [None, None]

extract_point_udf = udf(extract_lon_lat, ArrayType(DoubleType()))

# Explode features and extract
df_coords = dff.selectExpr("explode(features) as feature") \
    .withColumn("borough", col("feature.properties.borough")) \
    .withColumn("coords", col("feature.geometry.coordinates")) \
    .withColumn("point", extract_point_udf(col("coords"))) \
    .withColumn("longitude", col("point")[0]) \
    .withColumn("latitude", col("point")[1]) \
    .select("borough", "longitude", "latitude")

df_coords.show(1000)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sqrt, pow, col, when, row_number
from pyspark.sql.window import Window

# Step 1: Alias and join
df_cr = df.alias("cr")
df_br = df_coords.select(
    col("borough").alias("br_borough"),
    col("latitude").alias("br_latitude"),
    col("longitude").alias("br_longitude")
).alias("br")

# Step 2: Join on proximity
joined = df_cr.join(
    df_br,
    (abs(col("cr.latitude") - col("br.br_latitude")) < 0.072) &
    (abs(col("cr.longitude") - col("br.br_longitude")) < 0.095),
    how='left'
)

# Step 3: Compute distance
joined = joined.withColumn(
    "distance",
    sqrt(
        pow(col("cr.latitude") - col("br.br_latitude"), 2) +
        pow(col("cr.longitude") - col("br.br_longitude"), 2)
    )
)

# Step 4: Window and filtering
window = Window.partitionBy("cr.collision_id").orderBy(col("distance"))
ranked = joined.withColumn("rn", row_number().over(window))
closest = ranked.filter(col("rn") == 1)

# Step 5: Select all original df columns + few from right
df_filled = closest.select(
    *[col(f"cr.{c}").alias(c) for c in df.columns],  # all original columns
    col("br_borough"),
    col("distance")
).withColumn(
    "borough_final",
    when(col("borough").isNotNull(), col("borough")).otherwise(col("br_borough"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## display the filled table 
df_copyy = df_filled.filter((col("latitude") == '0.0000000') & (col("longitude") == '0.0000000'))
display(df_copyy)
display(df_filled.printSchema())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null=df_filled.filter(col("borough_final").isNull()).count()
print(f'Out of over 65000 null values in the borough column there was {null} record not filled hence they will be filled with Unknown')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final=df_filled.fillna('Unknown',subset=['borough_final'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of columns to exclude
exclude_cols = ['br_borough', 'distance', 'borough','borough_final']

# Get all columns except the excluded ones
cols_to_select = [c for c in df_final.columns if c not in exclude_cols]

# Add renamed borough_final column as borough

df_new = df_final.select(
    *cols_to_select,
    col('borough_final').alias('borough')
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new.filter(col("vehicle_type_code1").isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new = df_new.withColumn('borough',initcap("borough"))
df_new = df_new.withColumn('contributing_factor_vehicle_1',initcap("contributing_factor_vehicle_1"))
df_new = df_new.withColumn('vehicle_type_code1',initcap("vehicle_type_code1"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new.printSchema())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add a partition column for the month (e.g., truncated crash_date to month)
df_partitioned = df_new.withColumn("crash_month", trunc(col("crash_date"), "month"))

# Write as a Delta table partitioned by crash_month
df_partitioned.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("crash_month") \
    .saveAsTable("silver_nyc_crashes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver_nyc_crashes S
# MAGIC JOIN fact_crashes F ON 
# MAGIC S.collision_id =F.collision_id
# MAGIC JOIN dim_location L ON
# MAGIC L.borough = S.borough
# MAGIC WHERE DATE(extraction_timestamp) >= DATE('2025-07-11')
# MAGIC AND location_key IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dim_location
# MAGIC WHERE surrogate_key IS NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT  columns_checked, count(*) as cou FROM fact_error_event
# MAGIC GROUP BY columns_checked
# MAGIC ORDER BY cou desc

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# sdf
# 


# CELL ********************

display(df_new)

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
