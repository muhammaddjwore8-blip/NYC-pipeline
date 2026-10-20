# Databricks notebook source
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, col, current_timestamp, expr
from pyspark.sql.types import TimestampType

# COMMAND ----------

silver = spark.read.table('NY_trip_bronze')

# COMMAND ----------

#ry casting 
silver = silver.withColumn("tpep_pickup_datetime", expr("try_cast(tpep_pickup_datetime as timestamp)")).withColumn("tpep_dropoff_datetime", expr("try_cast(tpep_dropoff_datetime as timestamp)"))

# COMMAND ----------

silver.show(5)

# COMMAND ----------

silver.columns

# COMMAND ----------

silver = silver.filter(col('tpep_dropoff_datetime').isNotNull()).filter(col('tpep_pickup_datetime').isNotNull()).filter(col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))

# COMMAND ----------

silver = silver.filter(col('passenger_count').between(1,6)).filter(col('trip_distance').between (1, 100)).filter((col('fare_amount') > 0)  & (col('fare_amount') < col('total_amount'))).filter(col('trip_distance').between(0.5,100)).filter((col("total_amount") > 0) & (col('total_amount') < 500))

# COMMAND ----------

#fixing GPS FOR NY

# COMMAND ----------

silver = silver.filter((col("dropoff_latitude") != 0) & (col("dropoff_longitude") != 0) & (col("pickup_latitude") != 0) & (col("pickup_longitude") != 0))



# COMMAND ----------

silver = silver.dropDuplicates(subset=['tpep_pickup_datetime','tpep_dropoff_datetime','VendorID'])

# COMMAND ----------

silver = silver.withColumn("trip_duration", F.round((col('tpep_dropoff_datetime').cast('long') - col('tpep_pickup_datetime').cast('long')).cast('long')/60 , 2 ))

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS ny_silver')

# COMMAND ----------

silver.write.mode("overwrite").option("overwriteSchema", 'True').format("delta").saveAsTable("NY_silver")