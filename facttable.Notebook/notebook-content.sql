-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "466e2184-c19b-4669-a1ea-0398d3683846",
-- META       "default_lakehouse_name": "Vscode",
-- META       "default_lakehouse_workspace_id": "36952dd4-29ea-4c9a-85fa-b35583ab33cc",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "466e2184-c19b-4669-a1ea-0398d3683846"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

DROP VIEW IF EXISTS fact_view;

CREATE VIEW fact_view AS
SELECT 
    *,
    CONCAT(CAST(latitude AS STRING), '_', CAST(longitude AS STRING)) AS log_lat
FROM silver_nyc_crashes;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************






CREATE TABLE IF NOT EXISTS fact_crashes (
    collision_id STRING,
    date_key BIGINT,
    factor_key BIGINT,
    vehicle_key BIGINT,
    location_key BIGINT,
    number_of_persons_injured INT,
    number_of_persons_killed INT,
    crash_month STRING,
    year INT
)
USING DELTA
PARTITIONED BY (year)
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
);



MERGE INTO fact_crashes AS target
USING (
    SELECT 
        s.collision_id,
        d.date_key,
        f.surrogate_key AS factor_key,
        v.surrogate_key AS vehicle_key,
        l.surrogate_key AS location_key,
        s.number_of_persons_injured,
        s.number_of_persons_killed,
        s.crash_month,
        d.year,
        TRIM(CAST(s.latitude AS STRING)) || '_' || TRIM(CAST(s.longitude AS STRING)) AS log_lat
    FROM fact_view s
    LEFT JOIN dim_date d 
        ON CAST(s.crash_date AS DATE) = CAST(d.full_date AS DATE)
    LEFT JOIN dim_factor f 
        ON s.contributing_factor_vehicle_1 = f.factor AND f.is_current = 1
    LEFT JOIN dim_vehicle v 
        ON s.vehicle_type_code1 = v.vehicle_type AND v.is_current = 1
    LEFT JOIN dim_location l 
        ON (TRIM(CAST(s.latitude AS STRING)) || '_' || TRIM(CAST(s.longitude AS STRING))) = l.log_lat
           AND l.is_current = 1
) AS source
ON target.collision_id = source.collision_id

WHEN MATCHED AND (
    target.date_key != source.date_key OR
    target.factor_key != source.factor_key OR
    target.vehicle_key != source.vehicle_key OR
    target.location_key != source.location_key OR
    target.number_of_persons_injured != source.number_of_persons_injured OR
    target.number_of_persons_killed != source.number_of_persons_killed OR
    target.crash_month != source.crash_month OR
    target.year != source.year
)
THEN UPDATE SET
    date_key = source.date_key,
    factor_key = source.factor_key,
    vehicle_key = source.vehicle_key,
    location_key = source.location_key,
    number_of_persons_injured = source.number_of_persons_injured,
    number_of_persons_killed = source.number_of_persons_killed,
    crash_month = source.crash_month,
    year = source.year

WHEN NOT MATCHED THEN
    INSERT (
        collision_id,
        date_key,
        factor_key,
        vehicle_key,
        location_key,
        number_of_persons_injured,
        number_of_persons_killed,
        crash_month,
        year
    )
    VALUES (
        source.collision_id,
        source.date_key,
        source.factor_key,
        source.vehicle_key,
        source.location_key,
        source.number_of_persons_injured,
        source.number_of_persons_killed,
        source.crash_month,
        source.year
    );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
