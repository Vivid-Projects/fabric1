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

-- Welcome to your new notebook
-- Type here in the cell editor to add code!
SELECT * FROM nyc_crashes

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS dim_location (
  surrogate_key BIGINT,
  log_lat STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  borough STRING,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current INT
);


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DROP VIEW IF EXISTS locate_view;

CREATE VIEW locate_view AS
SELECT 
    CONCAT(latitude, '_', longitude) AS log_lat,
    latitude,
    longitude,
    borough
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY latitude, longitude ORDER BY borough) AS rn
    FROM silver_nyc_crashes
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
) AS dedup
WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Step 1: Find current max surrogate_key
WITH max_key AS (
  SELECT COALESCE(MAX(surrogate_key), 0) AS max_sk FROM dim_location
),

-- Step 2: Prepare new distinct locations from source, add surrogate keys here
new_locations AS (
  SELECT
    CONCAT(latitude, '_', longitude) AS log_lat,
    latitude,
    longitude,
    borough,
    ROW_NUMBER() OVER (ORDER BY log_lat)AS rn
  FROM locate_view
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
),

-- Step 3: Assign surrogate keys using max_sk + rn
staging AS (
  SELECT
    max_key.max_sk + new_locations.rn AS surrogate_key,
    new_locations.log_lat,
    new_locations.latitude,
    new_locations.longitude,
    new_locations.borough,
    CURRENT_TIMESTAMP() AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    1 AS is_current
  FROM new_locations CROSS JOIN max_key
)

-- Step 4: Now MERGE from staging
MERGE INTO dim_location AS target
USING staging AS source
ON target.log_lat = source.log_lat AND target.is_current = 1

WHEN MATCHED AND (
  NOT (target.latitude <=> source.latitude) OR
  NOT (target.longitude <=> source.longitude) OR
  NOT (target.borough <=> source.borough)
)
THEN UPDATE SET
  end_date = CURRENT_TIMESTAMP,
  is_current = 0

WHEN NOT MATCHED BY TARGET THEN
  INSERT (surrogate_key, log_lat, latitude, longitude, borough, effective_date, end_date, is_current)
  VALUES (source.surrogate_key, source.log_lat, source.latitude, source.longitude, source.borough, source.effective_date, source.end_date, source.is_current);


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS dim_vehicle (
    surrogate_key BIGINT,
    vehicle_type STRING,
    category STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current INT
)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DROP VIEW IF EXISTS view_vehicle_type;

CREATE VIEW view_vehicle_type AS
SELECT
    vehicle_type_code1 AS vehicle_type,
    category,
    CURRENT_TIMESTAMP() AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    1 AS is_current
FROM (
    SELECT DISTINCT 
        vehicle_type_code1,
        CASE 
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'sedan', '4 dr sedan', '2 dr sedan', 'convertible', 'station wagon/sport utility vehicle', 
                'suv', 'car/suv', 'sw/suv', '4door', 'suburban', 'surburban', 'vagon', 'wagon', 
                'passenger', 'passanger', 'pas', 'mini van', 'vanette', 'mini vahn', 'van camper', 
                'honda hrv', 'mercedes', 'nissan', 'ford', 'volks', 'chevy'
            ) THEN 'Passenger Vehicles'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'pick up tr', 'pick-up truck', 'pickup tru', 'pick up', 'pick-up', 'pkup', 'pick', 
                'box truck', 'box van', 'van truck', 'flat bed', 'flatbed', 'flatbed tr', 'flat rack', 
                'truck', 'tanker', 'tank', 'tank truck', 'tractor truck diesel', 'tractor truck gasoline', 
                'tractor', 'tractor tr', 'delivery', 'delivery t', 'delv', 'delivry tr', 'del truck', 
                'courier', 'courier va', 'refrigerated van', 'cargo van', 'freight va', 'freight tr', 
                'freigt van', 'amazon tru', 'amazon del', 'u-haul', 'u-haul tru', 'uhaul', 'mack truck', 
                'macktruck', 'fuso', 'fuso truck', 'garbage or refuse', 'garbage tr', 'garbage st', 
                'waste truc', 'beverage truck', 'bread truc', 'concrete mixer', 'cement tru', 'mixer', 
                'dump', 'dump truck', 'dumb truck', 'dump truk', 'semi truck', '18 wheeler', 
                'car carrie', 'trailer', 'tralier', 'trlr', 'comm trail', 'panel van', 'open body', 
                'stake or rack', 'livestock rack', 'horse trai', 'food trail', 'food truck', 'lunch wagon', 
                'pallet', 'commerial', 'commericia', 'commerical', 'commercial', 'comm', 'x com', 
                'usps', 'usps truck', 'usps van', 'usps deliv', 'usps vehic', 'usps mail', 'usps #6530', 
                'us postal', 'postal tru', 'postal box', 'moving tru', 'utility tr', 'utility', 'util', 
                'work van', 'ford couri', 'delv wh', 'transport', 'transporte', 'truck/bus', 'truck/van', 
                'boxtruck', 'heavy truc', 'flatbed to', 'ref box tr', 'u-haul tru', 'delivery v', 
                'ford ec2', 'ford f-150', 'ford f150', 'dodge ram', 'pick-up tr', 'pickup tow', 
                'g psd', 'govenment', 'goverment', 'govt p/u t', 'city of ne', 'freightlin'
            ) THEN 'Commercial Vehicles'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'ambulance', 'ambu', 'ambulanve', 'ambulence', 'ambalance', 'amulance', 'x amb', 'g amb', 
                'nys ambula', 'pvt ambula', 'ford ambul', 'ambulances', 'ems', 'ems bus', 'ems truck', 
                'ems fdny', 'ems -bus', 'fdny ems', 'fdny ems v', 'fdny ems#1', 'fdny ambul', 
                'fdny mecha', 'nyc fdny #', 'nyfd ambul', 'emt ambula', 'fd ambulan', 'fire truck', 
                'firetruck', 'fire', 'fire engin', 'fire appar', 'fdny fire', 'fdny firet', 'fdnytruckf', 
                'fdny engin', 'fdny ladde', 'fire ladde', 'ladder', 'ladder tru', 'ladder 169', 'engine 26', 
                'fdny 245 e', 'nyc fire t', 'fdny vehic', 'fdny insur', 'fdny utili', 'rmp', 'r.m.p.', 
                'marked rmp', 'nypd traff', 'government'
            ) THEN 'Emergency Vehicles'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'bus', 'mta bus', 'mta bus co', 'city bus', 'omnibus', 'mta', 'metro tran', 'mta transi', 
                'nyc mta bu', 'nyc transi', 'school bus', 'yellow bus', 'yellow sch', 'yellowscho', 
                'schoolbus', 'schoo lbus', 'nyc school', 'small bus', 'mini bus', 'small scho', 'charter bu', 
                'access-a-r', 'access a r', 'van bus', 'transit va', '10 paaseng', 'seniorcare', 'dollar van'
            ) THEN 'Public Transit'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'bike', 'bicycle', 'e-bike', 'ebike', 'citi bike', 'pedal bike', 'motorcycle', 'motorbike', 
                'mc', 'mcy', 'e motorcyc', 'scooter', 'e scooter', 'escooter', 'motorscooter', 'motorscoot', 
                'motor scoo', 'gas scoote', 'moped', 'mo-ped', 'mopped', 'mopd', 'revel mope', 'red moped', 
                'black scoo', 'lime scoot', 'gas moped', 'boom moped', 'razor scoo', 'minicycle', 'minibike', 
                'gas dirt b', 'skateboard', 'unicycle', 'ele. unicy', 'seated sco', 'stand up s', 'standing s', 
                'two wheel', 'manual sco', 'hoverboard', 'one wheel', 'pedicab', 'moped scoo', 'motorized', 
                'motorizeds'
            ) THEN 'Two-Wheeled Vehicles'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'forklift', 'fork lift', 'yale fork', 'excavator', 'bulldozer', 'crane', 'truck cran', 
                'bobcat', 'utv bobcat', 'skid steer', 'lift boom', 'boom lift', 'power shov', 'backhoe', 
                'kubota bac', 'toolcat', 'john deere', 'well driller', 'asphalt pa', 'lawn mower', 'tractor cr'
            ) THEN 'Heavy Machinery'
            WHEN TRIM(LOWER(vehicle_type_code1)) IN (
                'unknown', 'unk', 'unkwown', 'unknown ve', 'other', 'omr', 'omt', 'oms', 'a', 'd', 'd1', 'g', 
                'spc', 'rd/s', 'tl', 'tr', 'reg', 'can', 'cot', 'glben', 'leu', 'subn', '5', '4', 'x trl', 
                'хр06', 'ren', 'rgs', 'pas', 'psd', 'com', 'tk', 'trk', 'trk-m1', 'r/v', 'rv', 'ss', 
                'liabitiy', 'old republ', 'self insur', 'self-insur', 'usaa', 'geico', 'pv holding', 
                'progressiv', 'kw', 'mdx', 'riv 2022', 'ford ff', 'all terrai', 'hopper', 'grain', 
                'motorhome', 'motorized home', 'motor home', 'winniebago', 'rec vehicl', 'van', 'pass van', 
                'van (trans', 'sprinter', 'sprinter v', 'lsv', 'golf cart', 'golfcart', 'food cart', 
                'house on w', 'psvan', 'tlc', 'x com', 'attechment', 'emi- trail', 'c1', 'for', 'stak', 
                'landscapin', 'manpower s', 'electric m', 'electric c', 'electric s', 'electric b', '4dsd', 
                'flywing mo', 'ice cream', 'collection', 'nyc', 'nyc sanita', 'dsny sweep', 'road sweep', 
                'sweeper tr', 'street swe', 'plow', 'snow plow', 'dept tow t', 'traffic to', 'utility ve', 
                'multi-wheeled vehicle', 'horse draw', 'piggy back', 'box car', 'g psd', 'g amb', 
                'constructi', 'trailer ca', 'livestock rack', 'comm'
            ) THEN 'Other/Unspecified'
            ELSE 'Uncategorized'
        END AS category,
        ROW_NUMBER() OVER (PARTITION BY TRIM(LOWER(vehicle_type_code1)) ORDER BY vehicle_type_code1) AS rn
    FROM silver_nyc_crashes
    WHERE vehicle_type_code1 IS NOT NULL
) AS unique_vehicles
WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH Max_key AS (
    SELECT COALESCE(MAX(surrogate_key), 0) AS mk_key 
    FROM dim_vehicle
),
New_vehicle_table AS (
    SELECT 
        TRIM(LOWER(vehicle_type)) AS vehicle_type_key,
        vehicle_type AS vehicle_type,
        category,
        effective_date,
        end_date,
        is_current,
        ROW_NUMBER() OVER (ORDER BY TRIM(LOWER(vehicle_type))) AS rn
    FROM view_vehicle_type
    WHERE vehicle_type IS NOT NULL
    GROUP BY TRIM(LOWER(vehicle_type)), vehicle_type, category, effective_date, end_date, is_current
),
Staging AS (
    SELECT 
        (m.mk_key + n.rn) AS surrogate_key,
        n.vehicle_type,
        n.category,
        CURRENT_TIMESTAMP() AS effective_date,
        CAST(NULL AS TIMESTAMP) AS end_date,
        1 AS is_current
    FROM New_vehicle_table n
    CROSS JOIN Max_key m
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_vehicle d
        WHERE TRIM(LOWER(d.vehicle_type)) = n.vehicle_type_key 
        AND d.is_current = 1 
        AND d.category = n.category
    ) -- Only include new vehicle types or those with changed categories
)
MERGE INTO dim_vehicle AS target
USING Staging AS source
ON TRIM(LOWER(source.vehicle_type)) = TRIM(LOWER(target.vehicle_type)) AND target.is_current = 1
WHEN MATCHED AND source.category != target.category THEN
    UPDATE SET
        end_date = CURRENT_TIMESTAMP,
        is_current = 0
WHEN NOT MATCHED THEN
    INSERT (surrogate_key, vehicle_type, category, effective_date, end_date, is_current)
    VALUES (
        source.surrogate_key,
        source.vehicle_type,
        source.category,
        source.effective_date,
        source.end_date,
        source.is_current
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS dim_factor (
    surrogate_key int,
    factor string,
    category string,
    effective_date timestamp,
    end_date timestamp,
    is_current int

)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

DROP VIEW IF EXISTS view_vehicle;

CREATE VIEW  view_vehicle AS
SELECT
  contributing_factor_vehicle_1 AS FACTOR,
  category,
  CURRENT_TIMESTAMP() AS effective_date,
  CAST(NULL AS TIMESTAMP) AS end_date,
  1 AS is_current
FROM (
  SELECT DISTINCT 
    contributing_factor_vehicle_1,
    CASE 
        WHEN contributing_factor_vehicle_1 IN (
            'Following Too Closely', 'Failure To Yield Right-of-way', 'Fatigued/drowsy',
            'Using On Board Navigation Device', 'Aggressive Driving/road Rage', 'Unsafe Speed',
            'Driver Inexperience', 'Eating Or Drinking', 'Illnes', 'Drugs (illegal)',
            'Cell Phone (hand-held)', 'Prescription Medication', 'Lost Consciousness',
            'Fell Asleep', 'Outside Car Distraction', 'Passing Or Lane Usage Improper',
            'Reaction To Uninvolved Vehicle', 'Pedestrian/bicyclist/other Pedestrian Error/confusion',
            'Unsafe Lane Changing', 'Passenger Distraction', 'Alcohol Involvement',
            'Driver Inattention/distraction', 'Backing Unsafely', 'Failure To Keep Right',
            'Turning Improperly', 'Passing Too Closely', 'Physical Disability',
            'Texting', 'Listening/using Headphones'
        ) THEN 'Driver Behavior'
        WHEN contributing_factor_vehicle_1 IN (
            'Accelerator Defective', 'Tow Hitch Defective', 'Tire Failure/inadequate',
            'Brakes Defective', 'Headlights Defective', 'Steering Failure',
            'Windshield Inadequate', 'Other Lighting Defects', 'Tinted Windows',
            'Oversized Vehicle', 'Driverless/runaway Vehicle'
        ) THEN 'Vehicle Issues'
        WHEN contributing_factor_vehicle_1 IN (
            'Lane Marking Improper/inadequate', 'Pavement Defective', 'Obstruction/debris',
            'Pavement Slippery', 'Glare', 'Shoulders Defective/improper',
            'Traffic Control Device Improper/non-working'
        ) THEN 'Environmental Factors'
        WHEN contributing_factor_vehicle_1 IN ('Vehicle Vandalism', 'Animals Action') THEN 'External Factors'
        WHEN contributing_factor_vehicle_1 IN ('Unspecified', 'Unknown', 'Other Vehicular', 'Other Electronic Device') THEN 'Other/Unspecified'
        ELSE 'Uncategorized'
    END AS category,
    ROW_NUMBER() OVER (PARTITION BY contributing_factor_vehicle_1 ORDER BY contributing_factor_vehicle_1) AS rn
    FROM silver_nyc_crashes
    WHERE contributing_factor_vehicle_1 is not null
) AS unique_vehicles
WHERE rn=1;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH Max_key AS (
    SELECT COALESCE(MAX(surrogate_key), 0) AS mk_key 
    FROM dim_factor
),
New_factor_table AS (
    SELECT 
        TRIM(LOWER(factor)) AS factor_key,
        factor AS contributing_factor,
        category,
        effective_date,
        end_date,
        is_current,
        ROW_NUMBER() OVER (ORDER BY TRIM(LOWER(factor))) AS rn
    FROM view_vehicle
    WHERE factor IS NOT NULL
    GROUP BY TRIM(LOWER(factor)), factor, category, effective_date, end_date, is_current
),
Staging AS (
    SELECT 
        (m.mk_key + n.rn) AS surrogate_key,
        n.contributing_factor AS factor,
        n.category,
        CURRENT_TIMESTAMP() AS effective_date,
        CAST(NULL AS TIMESTAMP) AS end_date,
        1 AS is_current
    FROM New_factor_table n
    CROSS JOIN Max_key m
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_factor d
        WHERE TRIM(LOWER(d.factor)) = n.factor_key 
        AND d.is_current = 1 
        AND d.category = n.category
    ) -- Only include new factors or those with changed categories
)
MERGE INTO dim_factor AS target
USING Staging AS source
ON TRIM(LOWER(source.factor)) = TRIM(LOWER(target.factor)) AND target.is_current = 1
WHEN MATCHED AND source.category != target.category THEN
    UPDATE SET
        end_date = CURRENT_TIMESTAMP,
        is_current = 0
WHEN NOT MATCHED THEN
    INSERT (surrogate_key, factor, category, effective_date, end_date, is_current)
    VALUES (
        source.surrogate_key,
        source.factor,
        source.category,
        source.effective_date,
        source.end_date,
        source.is_current
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Step 1: Create the dim_date table
CREATE TABLE IF NOT EXISTS dim_date (
    date_key BIGINT,
    full_date TIMESTAMP,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    day_name STRING,
    day_of_week INT,
    is_weekend INT,
    is_holiday INT,
    holiday_name STRING,
    fiscal_year INT,
    fiscal_quarter INT
)
USING DELTA
TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
);

-- Step 2: Populate the dim_date table
INSERT INTO dim_date
SELECT 
    (year(full_date) * 10000 + month(full_date) * 100 + day(full_date)) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    CASE 
        WHEN month(full_date) = 1 THEN 'January'
        WHEN month(full_date) = 2 THEN 'February'
        WHEN month(full_date) = 3 THEN 'March'
        WHEN month(full_date) = 4 THEN 'April'
        WHEN month(full_date) = 5 THEN 'May'
        WHEN month(full_date) = 6 THEN 'June'
        WHEN month(full_date) = 7 THEN 'July'
        WHEN month(full_date) = 8 THEN 'August'
        WHEN month(full_date) = 9 THEN 'September'
        WHEN month(full_date) = 10 THEN 'October'
        WHEN month(full_date) = 11 THEN 'November'
        WHEN month(full_date) = 12 THEN 'December'
    END AS month_name,
    day(full_date) AS day,
    CASE 
        WHEN dayofweek(full_date) = 1 THEN 'Sunday'
        WHEN dayofweek(full_date) = 2 THEN 'Monday'
        WHEN dayofweek(full_date) = 3 THEN 'Tuesday'
        WHEN dayofweek(full_date) = 4 THEN 'Wednesday'
        WHEN dayofweek(full_date) = 5 THEN 'Thursday'
        WHEN dayofweek(full_date) = 6 THEN 'Friday'
        WHEN dayofweek(full_date) = 7 THEN 'Saturday'
    END AS day_name,
    dayofweek(full_date) AS day_of_week,
    CASE 
        WHEN dayofweek(full_date) IN (1, 7) THEN 1 
        ELSE 0 
    END AS is_weekend,
    CASE 
        WHEN month(full_date) = 1 AND day(full_date) = 1 THEN 1
        WHEN month(full_date) = 7 AND day(full_date) = 4 THEN 1
        WHEN month(full_date) = 12 AND day(full_date) = 25 THEN 1
        ELSE 0 
    END AS is_holiday,
    CASE 
        WHEN month(full_date) = 1 AND day(full_date) = 1 THEN 'New Year''s Day'
        WHEN month(full_date) = 7 AND day(full_date) = 4 THEN 'Independence Day'
        WHEN month(full_date) = 12 AND day(full_date) = 25 THEN 'Christmas Day'
        ELSE NULL 
    END AS holiday_name,
    CASE 
        WHEN month(full_date) >= 7 THEN year(full_date) + 1 
        ELSE year(full_date) 
    END AS fiscal_year,
    CASE 
        WHEN month(full_date) IN (7, 8, 9) THEN 1
        WHEN month(full_date) IN (10, 11, 12) THEN 2
        WHEN month(full_date) IN (1, 2, 3) THEN 3
        WHEN month(full_date) IN (4, 5, 6) THEN 4
    END AS fiscal_quarter
FROM (
    SELECT explode(sequence(to_timestamp('2010-01-01'), to_timestamp('2030-12-31'), interval 1 day)) AS full_date
) date_sequence;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark",
-- META   "frozen": true,
-- META   "editable": false
-- META }

-- CELL ********************

SELECT * FROM DIM_DATE
ORDER BY YEAR,QUARTER,MONTH,DAY

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT * FROM EXTRACTION_WATERMARK

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    d.year,
    v.category,
    f.category,
    l.borough,
    COUNT(*) AS crash_count
FROM silver_nyc_crashes c
JOIN dim_date d ON CAST(c.crash_date AS DATE) = CAST(d.full_date AS DATE)
JOIN dim_factor f ON c.contributing_factor_vehicle_1 = f.factor AND f.is_current = 1
JOIN dim_vehicle v ON c.vehicle_type_code1 = v.vehicle_type AND v.is_current = 1
JOIN dim_location l ON c.borough = l.borough AND l.is_current = 1
GROUP BY d.year, v.category, f.category,l.borough;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    l.borough,
    COUNT(*) AS crash_count
FROM silver_nyc_crashes c
JOIN dim_location l ON c.borough = l.borough AND l.is_current = 1
GROUP BY l.borough;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

Select count(*) c,borough from silver_nyc_crashes
Group By borough
order by c desc

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT 
    d.year,
    d.month_name,
    f.category AS factor_category,
    v.category AS vehicle_category,
    l.borough,
    COUNT(*) AS crash_count
FROM silver_nyc_crashes s
JOIN dim_date d ON CAST(s.crash_date AS DATE) = CAST(d.full_date AS DATE)
JOIN dim_factor f ON s.contributing_factor_vehicle_1 = f.factor AND f.is_current = 1
JOIN dim_vehicle v ON s.vehicle_type_code1 = v.vehicle_type AND v.is_current = 1
JOIN DIM_LOCATION l ON CONCAT(CAST(s.latitude AS STRING), '_', CAST(s.longitude AS STRING)) = l.log_lat AND l.is_current = 1
GROUP BY d.year, d.month_name, f.category, v.category, l.borough
ORDER BY CRASH_COUNT DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

Select * from dim_location

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE TABLE IF NOT EXISTS nyc_crashes1 AS
-- MAGIC SELECT 
-- MAGIC     n.*, 
-- MAGIC     COALESCE(l.on_street_name, 'Unknown') AS street_name
-- MAGIC FROM 
-- MAGIC     silver_nyc_crashes n
-- MAGIC LEFT JOIN 
-- MAGIC     street_name l 
-- MAGIC ON 
-- MAGIC     n.collision_id = l.collision_id
-- MAGIC WHERE 
-- MAGIC     n.collision_id IS NOT NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE IF NOT EXISTS dim_location1 (
-- MAGIC   surrogate_key BIGINT,
-- MAGIC   log_lat STRING,
-- MAGIC   latitude DOUBLE,
-- MAGIC   longitude DOUBLE,
-- MAGIC   street_name STRING,
-- MAGIC   borough STRING,
-- MAGIC   effective_date TIMESTAMP,
-- MAGIC   end_date TIMESTAMP,
-- MAGIC   is_current INT
-- MAGIC );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC 
-- MAGIC DROP VIEW IF EXISTS loc_view;
-- MAGIC 
-- MAGIC CREATE VIEW loc_view AS
-- MAGIC SELECT 
-- MAGIC     CONCAT(latitude, '_', longitude) AS log_lat,
-- MAGIC     latitude,
-- MAGIC     longitude,
-- MAGIC     borough,
-- MAGIC     street_name
-- MAGIC FROM (
-- MAGIC     SELECT *,
-- MAGIC            ROW_NUMBER() OVER (PARTITION BY latitude, longitude ORDER BY borough) AS rn
-- MAGIC     FROM nyc_crashes1
-- MAGIC     WHERE latitude IS NOT NULL AND longitude IS NOT NULL
-- MAGIC ) AS dedup
-- MAGIC WHERE rn = 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- Step 1: Find current max surrogate_key
-- MAGIC WITH max_key AS (
-- MAGIC   SELECT COALESCE(MAX(surrogate_key), 0) AS max_sk FROM dim_location1
-- MAGIC ),
-- MAGIC 
-- MAGIC -- Step 2: Prepare new distinct locations from source, add surrogate keys here
-- MAGIC new_locations AS (
-- MAGIC   SELECT
-- MAGIC     CONCAT(latitude, '_', longitude) AS log_lat,
-- MAGIC     latitude,
-- MAGIC     longitude,
-- MAGIC     street_name,
-- MAGIC     borough,
-- MAGIC     ROW_NUMBER() OVER (ORDER BY CONCAT(latitude, '_', longitude)) AS rn
-- MAGIC   FROM loc_view
-- MAGIC   WHERE latitude IS NOT NULL AND longitude IS NOT NULL
-- MAGIC ),
-- MAGIC 
-- MAGIC -- Step 3: Assign surrogate keys using max_sk + rn
-- MAGIC staging AS (
-- MAGIC   SELECT
-- MAGIC     max_key.max_sk + new_locations.rn AS surrogate_key,
-- MAGIC     new_locations.log_lat,
-- MAGIC     new_locations.latitude,
-- MAGIC     new_locations.longitude,
-- MAGIC     new_locations.street_name,
-- MAGIC     new_locations.borough,
-- MAGIC     CURRENT_TIMESTAMP() AS effective_date,
-- MAGIC     CAST(NULL AS TIMESTAMP) AS end_date,
-- MAGIC     1 AS is_current
-- MAGIC   FROM new_locations
-- MAGIC   CROSS JOIN max_key
-- MAGIC )
-- MAGIC 
-- MAGIC -- Step 4: Now MERGE from staging
-- MAGIC MERGE INTO dim_location1 AS target
-- MAGIC USING staging AS source
-- MAGIC ON target.log_lat = source.log_lat AND target.is_current = 1
-- MAGIC 
-- MAGIC WHEN MATCHED AND (
-- MAGIC   NOT (target.latitude <=> source.latitude) OR
-- MAGIC   NOT (target.longitude <=> source.longitude) OR
-- MAGIC   NOT (target.borough <=> source.borough) OR
-- MAGIC   NOT (target.street_name <=> source.street_name)
-- MAGIC )
-- MAGIC THEN UPDATE SET
-- MAGIC   target.end_date = CURRENT_TIMESTAMP,
-- MAGIC   target.is_current = 0
-- MAGIC 
-- MAGIC WHEN NOT MATCHED BY TARGET THEN
-- MAGIC   INSERT (
-- MAGIC     surrogate_key,
-- MAGIC     log_lat,
-- MAGIC     latitude,
-- MAGIC     longitude,
-- MAGIC     street_name,
-- MAGIC     borough,
-- MAGIC     effective_date,
-- MAGIC     end_date,
-- MAGIC     is_current
-- MAGIC   )
-- MAGIC   VALUES (
-- MAGIC     source.surrogate_key,
-- MAGIC     source.log_lat,
-- MAGIC     source.latitude,
-- MAGIC     source.longitude,
-- MAGIC     source.street_name,
-- MAGIC     source.borough,
-- MAGIC     source.effective_date,
-- MAGIC     source.end_date,
-- MAGIC     source.is_current
-- MAGIC   );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
