# Databricks notebook source
service_credential = dbutils.secrets.get(scope="tweets",key="SP")

spark.conf.set("fs.azure.account.auth.type.syntweetsstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.syntweetsstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.syntweetsstorage.dfs.core.windows.net", "4ced5214-f937-4cef-b680-5395a174647c")
spark.conf.set("fs.azure.account.oauth2.client.secret.syntweetsstorage.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.syntweetsstorage.dfs.core.windows.net", "https://login.microsoftonline.com/6497bc6d-7e82-48c2-b571-9e9489ce1a4c/oauth2/token")
# Solve decimal column loading problem
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

# COMMAND ----------

data = spark.sql("SELECT * FROM flights.flights_data_landing;")

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


