-- Databricks notebook source
SELECT
    (SELECT COUNT(*) FROM uber.bronze.bulk_rides) AS bulk_rides,
    (SELECT COUNT(*) FROM uber.bronze.map_cities) AS cities,
    (SELECT COUNT(*) FROM uber.bronze.map_vehicle_types) AS vehicle_types,
    (SELECT COUNT(*) FROM uber.bronze.map_payment_methods) AS payment_methods,
    (SELECT COUNT(*) FROM uber.bronze.map_vehicle_makes) AS vehicle_makes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Real-time ingestion

-- COMMAND ----------

SELECT *
FROM uber.bronze.rides_raw
ORDER BY timestamp DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testing Streaming Table

-- COMMAND ----------

SELECT
    ride_id,
    confirmation_number,
    passenger_name,
    booking_timestamp,
    total_fare
FROM uber.bronze.stg_rides
ORDER BY booking_timestamp DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testing Silver OBT

-- COMMAND ----------

SELECT
    ride_id,
    passenger_name,
    vehicle_type,
    payment_method,
    pickup_city,
    state,
    region,
    total_fare
FROM uber.bronze.silver_obt
ORDER BY booking_timestamp DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testing Gold Layer

-- COMMAND ----------

SELECT
    (SELECT COUNT(*) FROM uber.bronze.dim_passenger) AS passengers,
    (SELECT COUNT(*) FROM uber.bronze.dim_driver) AS drivers,
    (SELECT COUNT(*) FROM uber.bronze.dim_vehicle) AS vehicles,
    (SELECT COUNT(*) FROM uber.bronze.dim_payment) AS payments,
    (SELECT COUNT(*) FROM uber.bronze.dim_booking) AS bookings,
    (SELECT COUNT(*) FROM uber.bronze.dim_location) AS locations,
    (SELECT COUNT(*) FROM uber.bronze.fact) AS fact_rides;

-- COMMAND ----------

SELECT *
FROM uber.bronze.fact
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Slowly Changing Dimensions (SCD2)

-- COMMAND ----------

SELECT *
FROM uber.bronze.dim_location
ORDER BY pickup_city_id, __START_AT;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data-Quality Checks

-- COMMAND ----------

SELECT
    (SELECT COUNT(*)
     FROM uber.bronze.silver_obt
     WHERE ride_id IS NULL) AS null_ride_ids,

    (SELECT COUNT(*)
     FROM (
         SELECT ride_id
         FROM uber.bronze.silver_obt
         GROUP BY ride_id
         HAVING COUNT(*) > 1
     )) AS duplicate_ride_ids,

    (SELECT COUNT(*)
     FROM uber.bronze.silver_obt
     WHERE total_fare < 0) AS negative_fares,

    (SELECT COUNT(*)
     FROM uber.bronze.silver_obt
     WHERE rating IS NOT NULL
       AND (rating < 1 OR rating > 5)) AS invalid_ratings,

    (SELECT COUNT(*)
     FROM uber.bronze.silver_obt
     WHERE vehicle_type_id IS NOT NULL
       AND vehicle_type IS NULL) AS missing_vehicle_types;