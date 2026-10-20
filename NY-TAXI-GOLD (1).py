# Databricks notebook source
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, col, current_timestamp, expr, count,avg,round, sum, hour,dayname, day, weekday,max, desc
from pyspark.sql.types import TimestampType

# COMMAND ----------

gold = spark.read.table("NY_silver")

# COMMAND ----------

gold.show(5)

# COMMAND ----------

#Hourly Trips & Revenue - total trips, avg fare, total revenue per hour


# COMMAND ----------

gpld_avr_trip = gold.groupBy(col('VendorID')).agg(count("*").alias('total_trips1'),round(avg(col('fare_amount')),2).alias('avg_fare'))

# COMMAND ----------

#average fare
avra_fare_df =gpld_avr_trip.write.mode("overwrite").format("delta").saveAsTable("average_fare")


# COMMAND ----------

#total revenue per hour

gold_hour_rev = gold.groupBy(hour('tpep_pickup_datetime').alias('hour')).agg(count('*').alias('total_pick_up') , round(sum(col('total_amount')), 2).alias('revenue_per_hour')).orderBy('hour')


# COMMAND ----------

gold_hour_rev.write.mode("overwrite").format("delta").saveAsTable("average_hour_rev")

# COMMAND ----------

#PEAK DEMAND by hour
gold_hour_peak = gold.groupby(hour('tpep_pickup_datetime')).agg(count("*").alias('high_demand')).orderBy('high_demand', ascending=False)

gold_hour_peak.write.mode('overwrite').format('delta').saveAsTable('peak_hour')


# COMMAND ----------

#trip and revenue by cash or card
gold_cash_card = gold.withColumn("payment_type", F.when(col('payment_type') == 1, 'card').otherwise('cash')).groupBy("payment_type").agg(round(avg(col('total_amount')),2).alias('amount'))

# COMMAND ----------

gold_cash_card.write.mode('overwrite').format('Delta').saveAsTable('cash_or_card_payment')

# COMMAND ----------

#vendor performance
gold_ven_perfo = gold.groupBy("VendorID").agg(count('*').alias('total_trips'), round(avg(col('tpep_dropoff_datetime').cast("long") - col('tpep_pickup_datetime').cast('long'))/60,2).alias('trip_duration_avg_in_minutes'),round(avg(col('fare_amount')),2).alias('average_fare'))


# COMMAND ----------

gold_ven_perfo.write.mode('overwrite').format('delta').saveAsTable('performance')