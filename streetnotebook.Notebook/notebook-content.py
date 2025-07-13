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
# MAGIC 
# MAGIC 
# MAGIC DROP VIEW IF EXISTS fat_view;
# MAGIC 
# MAGIC CREATE VIEW fat_view AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     CONCAT(CAST(latitude AS STRING), '_', CAST(longitude AS STRING)) AS log_lat
# MAGIC FROM nyc_crashes1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS fact_crashes1 (
# MAGIC     collision_id STRING,
# MAGIC     date_key BIGINT,
# MAGIC     factor_key BIGINT,
# MAGIC     vehicle_key BIGINT,
# MAGIC     location_key BIGINT,
# MAGIC     number_of_persons_injured INT,
# MAGIC     number_of_persons_killed INT,
# MAGIC     crash_month STRING,
# MAGIC     year INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (crash_month)
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = '1',
# MAGIC     'delta.minWriterVersion' = '2'
# MAGIC );
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO fact_crashes1 AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         s.collision_id,
# MAGIC         d.date_key,
# MAGIC         f.surrogate_key AS factor_key,
# MAGIC         v.surrogate_key AS vehicle_key,
# MAGIC         l.surrogate_key AS location_key,
# MAGIC         s.number_of_persons_injured,
# MAGIC         s.number_of_persons_killed,
# MAGIC         s.crash_month,
# MAGIC         d.year,
# MAGIC         TRIM(CAST(s.latitude AS STRING)) || '_' || TRIM(CAST(s.longitude AS STRING)) AS log_lat
# MAGIC     FROM fat_view s
# MAGIC     LEFT JOIN dim_date d 
# MAGIC         ON CAST(s.crash_date AS DATE) = CAST(d.full_date AS DATE)
# MAGIC     LEFT JOIN dim_factor f 
# MAGIC         ON s.contributing_factor_vehicle_1 = f.factor AND f.is_current = 1
# MAGIC     LEFT JOIN dim_vehicle v 
# MAGIC         ON s.vehicle_type_code1 = v.vehicle_type AND v.is_current = 1
# MAGIC     LEFT JOIN dim_location1 l 
# MAGIC         ON (TRIM(CAST(s.latitude AS STRING)) || '_' || TRIM(CAST(s.longitude AS STRING))) = l.log_lat
# MAGIC            AND l.is_current = 1
# MAGIC ) AS source
# MAGIC ON target.collision_id = source.collision_id
# MAGIC 
# MAGIC WHEN MATCHED AND (
# MAGIC     target.date_key != source.date_key OR
# MAGIC     target.factor_key != source.factor_key OR
# MAGIC     target.vehicle_key != source.vehicle_key OR
# MAGIC     target.location_key != source.location_key OR
# MAGIC     target.number_of_persons_injured != source.number_of_persons_injured OR
# MAGIC     target.number_of_persons_killed != source.number_of_persons_killed OR
# MAGIC     target.crash_month != source.crash_month OR
# MAGIC     target.year != source.year
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     date_key = source.date_key,
# MAGIC     factor_key = source.factor_key,
# MAGIC     vehicle_key = source.vehicle_key,
# MAGIC     location_key = source.location_key,
# MAGIC     number_of_persons_injured = source.number_of_persons_injured,
# MAGIC     number_of_persons_killed = source.number_of_persons_killed,
# MAGIC     crash_month = source.crash_month,
# MAGIC     year = source.year
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         collision_id,
# MAGIC         date_key,
# MAGIC         factor_key,
# MAGIC         vehicle_key,
# MAGIC         location_key,
# MAGIC         number_of_persons_injured,
# MAGIC         number_of_persons_killed,
# MAGIC         crash_month,
# MAGIC         year
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.collision_id,
# MAGIC         source.date_key,
# MAGIC         source.factor_key,
# MAGIC         source.vehicle_key,
# MAGIC         source.location_key,
# MAGIC         source.number_of_persons_injured,
# MAGIC         source.number_of_persons_killed,
# MAGIC         source.crash_month,
# MAGIC         source.year
# MAGIC     );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM extraction_watermark


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
