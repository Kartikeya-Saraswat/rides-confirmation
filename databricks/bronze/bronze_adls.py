# Databricks notebook source
import pandas as pd

# These values should be supplied through Databricks configuration.
ADLS_BASE_URL = spark.conf.get("adls_base_url")
ADLS_SAS_TOKEN = spark.conf.get("adls_sas_token")

files = [
    {"file": "map_cancellation_reasons"},
    {"file": "map_cities"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]

for file in files:

    url = f"{ADLS_BASE_URL}/{file['file']}.json?{ADLS_SAS_TOKEN}"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)

    # Writing data into bronze layer
    df_spark.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"uber.bronze.{file['file']}")


# COMMAND ----------

# Initial bulk load
bulk_url = f"{ADLS_BASE_URL}/bulk_rides.json?{ADLS_SAS_TOKEN}"

df = pd.read_json(bulk_url)
df_spark = spark.createDataFrame(df)

if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("uber.bronze.bulk_rides")

    print("Initial bulk load completed.")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.bulk_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.map_cities