# Databricks notebook source
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, col, current_timestamp
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE VOLUME IF NOT EXISTS workspace.default.ny_taxi_bronze
# MAGIC

# COMMAND ----------

path = "/Volumes/workspace/default/ny_taxi_bronze/yellow_tripdata_2015-01.csv"

# COMMAND ----------

bronze = spark.read.csv(path, header = True, inferSchema = True)


# COMMAND ----------

bronze.show(10)

# COMMAND ----------

bronze = bronze.withColumn('ingested_at', current_timestamp())

# COMMAND ----------

bronze.show(5)

# COMMAND ----------

bronze.write.mode('overwrite').format('delta').option("overwriteSchema", 'True').saveAsTable('NY_trip_bronze')