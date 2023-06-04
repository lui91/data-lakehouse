# Databricks notebook source
# MAGIC %md
# MAGIC # Set connection to blob storage

# COMMAND ----------

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
# MAGIC FROM 'abfss://airlines@' + storage_account + '.dfs.core.windows.net/*.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true',
# MAGIC                 'header' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flights.flights_data_landing
