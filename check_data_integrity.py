# Databricks notebook source
service_credential = dbutils.secrets.get(scope="tweets",key="SP")
storage_account = dbutils.secrets.get(scope="tweets",key="storage-account")
app_id = dbutils.secrets.get(scope="tweets",key="app-id")
dir_id = dbutils.secrets.get(scope="tweets",key="dir-id")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type." + storage_account + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storage_account + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storage_account + ".dfs.core.windows.net", app_id)
spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_account + ".dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_account + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + dir_id +"/oauth2/token")

# COMMAND ----------

files = dbutils.fs.ls('abfss://airlines@syntweetsstorage.dfs.core.windows.net/')
parquet_files = [file.path for file in files if 'Flights' in file.path]
parquet_files

# COMMAND ----------

data_2018 = spark.read.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/Combined_Flights_2018.parquet')
data_2019 = spark.read.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/Combined_Flights_2019.parquet')
data_2020 = spark.read.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/Combined_Flights_2020.parquet')
data_2021 = spark.read.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/Combined_Flights_2021.parquet')
data_2022 = spark.read.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/Combined_Flights_2022.parquet')

# COMMAND ----------

data_2018.dtypes

# COMMAND ----------

data_2019.dtypes

# COMMAND ----------

data_2020.dtypes

# COMMAND ----------

data_2021.dtypes

# COMMAND ----------

data_2022.dtypes

# COMMAND ----------

dtype_2018 = [dtype for _, dtype in data_2018.dtypes]
dtype_2019 = [dtype for _, dtype in data_2019.dtypes]
dtype_2020 = [dtype for _, dtype in data_2020.dtypes]
dtype_2021 = [dtype for _, dtype in data_2021.dtypes]
dtype_2022 = [dtype for _, dtype in data_2022.dtypes]

# COMMAND ----------

dtype_2018 == dtype_2019, dtype_2018 == dtype_2020, dtype_2018 == dtype_2021, dtype_2018 == dtype_2022


# COMMAND ----------

[(data_2018.dtypes[i] , data_2019.dtypes[i]) for i in range(0, len(dtype_2018)) if dtype_2018[i] != dtype_2019[i]]

# COMMAND ----------

[(data_2018.dtypes[i] , data_2022.dtypes[i]) for i in range(0, len(dtype_2018)) if dtype_2018[i] != dtype_2022[i]]

# COMMAND ----------

data_2018.limit(5).display()

# COMMAND ----------

data_2019.limit(5).display()

# COMMAND ----------

data_2022.limit(5).display()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

cast_data_2019 = data_2019.withColumn("DivAirportLandings", col("DivAirportLandings").cast("double"))
cast_data_2022 = data_2022.withColumn("DivAirportLandings", col("DivAirportLandings").cast("double"))

# COMMAND ----------

cast_data_2019.dtypes

# COMMAND ----------

cast_data_2022.dtypes

# COMMAND ----------

cast_dtype_2019 = [dtype for _, dtype in cast_data_2019.dtypes]
[(data_2018.dtypes[i] , cast_data_2019.dtypes[i]) for i in range(0, len(dtype_2018)) if dtype_2018[i] != cast_dtype_2019[i]]

# COMMAND ----------

cast_dtype_2022 = [dtype for _, dtype in cast_data_2022.dtypes]
[(data_2018.dtypes[i] , data_2022.dtypes[i]) for i in range(0, len(dtype_2018)) if dtype_2018[i] != cast_dtype_2022[i]]

# COMMAND ----------

dtype_2018 == cast_dtype_2019, dtype_2018 == dtype_2020, dtype_2018 == dtype_2021, dtype_2018 == cast_dtype_2022

# COMMAND ----------

cast_data_2019.write.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/c_Combined_Flights_2019.parquet')

# COMMAND ----------

cast_data_2022.write.parquet('abfss://airlines@syntweetsstorage.dfs.core.windows.net/c_Combined_Flights_2022.parquet')

# COMMAND ----------


