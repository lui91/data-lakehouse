# Databricks notebook source
data = spark.read.table("flights.flights_data")
data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove target data from dataframe

# COMMAND ----------

dep_cols = [x for x in data.columns if "Dep" in x]
arr_cols = [x for x in data.columns if "Arr" in x]
time_columns = dep_cols + arr_cols +  ['AirTime', 'ActualElapsedTime', '__index_level_0__']
time_columns

# COMMAND ----------

data_drop = data.drop(*time_columns)

# COMMAND ----------

data_drop.describe

# COMMAND ----------

str_to_encode = [col for col, dtype in data_drop.dtypes if dtype == "string"]
str_to_encode

# COMMAND ----------

out_str_to_encode = ["enc_" + col for col in str_to_encode]
out_str_to_encode

# COMMAND ----------

# MAGIC %md
# MAGIC # ML data preparation

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## String encoding

# COMMAND ----------

str_ind = StringIndexer(inputCols=str_to_encode, outputCols=out_str_to_encode)

# COMMAND ----------

airline_model = str_ind.fit(data_drop)

# COMMAND ----------

air_enc = airline_model.transform(data_drop)

# COMMAND ----------

data = air_enc.drop(*str_to_encode)

# COMMAND ----------

data.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data scale

# COMMAND ----------

set(dtype for col, dtype in data.dtypes)

# COMMAND ----------

num_to_scale = [col for col, dtype in data_drop.dtypes if dtype in ['bigint', 'double']]
num_to_scale

# COMMAND ----------

assembler = VectorAssembler(inputCols=num_to_scale, outputCol='c_vec')
scaler = MinMaxScaler(inputCol="c_vec", outputCol="c_scaled")
scaler_pipeline = Pipeline(stages=[assembler, scaler])
scaler_model = scaler_pipeline.fit(data)
scaled_data = scaler_model.transform(data)
scaled_data.limit(1).display()

# COMMAND ----------

dataset = scaled_data.select('FlightDate', 'Cancelled', 'Diverted', 'c_scaled')
dataset = dataset.withColumnRenamed('c_scaled', 'vec')
dataset.limit(1).display()

# COMMAND ----------

dataset.write.format('delta').saveAsTable('flights.dataset')
