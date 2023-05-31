# Databricks notebook source
data = spark.read.table("flights.flights_data_landing")

# COMMAND ----------

from pyspark.sql.functions import col, when, count

# COMMAND ----------

data.count()

# COMMAND ----------

data.limit(1).display()

# COMMAND ----------

data.select([count(when(col(c).isNull(), c)).alias(c) for c in data.columns]).display()

# COMMAND ----------

data = data.dropna()

# COMMAND ----------

data.count()

# COMMAND ----------

data.write.format('delta').saveAsTable('flights.flights_data')

# COMMAND ----------


