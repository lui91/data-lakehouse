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

# COMMAND ----------

landingZoneLocation = "/tmp/flights/landingZone"
spark.conf.set("c.ingestDatabase", "flights")
spark.conf.set("c.ingestLandingZone", "dbfs:" + landingZoneLocation)
# Solve decimal convertion loading problem
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${c.ingestDatabase};
# MAGIC CREATE TABLE ${c.ingestDatabase}.flights_data_landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE ${c.ingestDatabase}.flights_data_landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${c.ingestDatabase};
# MAGIC CREATE TABLE ${c.ingestDatabase}.flights_data_landing
# MAGIC USING PARQUET
# MAGIC LOCATION 'abfss://airlines@syntweetsstorage.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ${c.ingestDatabase}.flights_data_landing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ${c.ingestDatabase}.flights_data_landing; 

# COMMAND ----------


