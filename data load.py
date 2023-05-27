# Databricks notebook source
# MAGIC %md
# MAGIC # Set connection to blob storage

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="tweets",key="SP")

spark.conf.set("fs.azure.account.auth.type.syntweetsstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.syntweetsstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.syntweetsstorage.dfs.core.windows.net", "4ced5214-f937-4cef-b680-5395a174647c")
spark.conf.set("fs.azure.account.oauth2.client.secret.syntweetsstorage.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.syntweetsstorage.dfs.core.windows.net", "https://login.microsoftonline.com/6497bc6d-7e82-48c2-b571-9e9489ce1a4c/oauth2/token")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# %sql
# USE flights;
# CREATE TABLE flights.flights_data_landing
# USING PARQUET
# LOCATION 'abfss://airlines@syntweetsstorage.dfs.core.windows.net/'

# COMMAND ----------

# %sql
# DESCRIBE DETAIL flights.flights_data_landing

# COMMAND ----------

# MAGIC %md
# MAGIC # Create and load data with copy into

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flights;
# MAGIC CREATE TABLE IF NOT EXISTS flights_data_landing;
# MAGIC
# MAGIC COPY INTO flights_data_landing
# MAGIC FROM 'abfss://airlines@syntweetsstorage.dfs.core.windows.net/*.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'header' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flights.flights_data_landing
