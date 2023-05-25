# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text("basePath",defaultValue="/tmp/flights", label="1 basePath")
# MAGIC dbutils.widgets.text("database",defaultValue="flights", label="2 database")

# COMMAND ----------

basePath = dbutils.widgets.get("basePath")
landingZoneLocation = basePath + "/landingZone"
landingZoneLocation

# COMMAND ----------

database = dbutils.widgets.get("database")
databasePath = basePath + "/flightsdata"
sqlDatabasePath = "dbfs:" + databasePath
sqlDatabasePath

# COMMAND ----------

try:
  dbutils.fs.ls(basePath)
except:
  dbutils.fs.mkdirs(basePath)
  dbutils.fs.mkdirs(landingZoneLocation)
else:
  raise Exception("The folder " + basePath + " already exists, this notebook will remove it at the end, please change the basePath or remove the folder first")

print("basePath " + basePath + " created")

# COMMAND ----------

createDbSql = "CREATE DATABASE " + database + " LOCATION '" + sqlDatabasePath + "';"
try:
  spark.sql(createDbSql)
except:
  raise Exception("The database " + database + " already exists")
  
print("database " + database + " created")

# COMMAND ----------

# create configurations that can be called from SQL
spark.conf.set("c.ingestDatabase", database)
spark.conf.set("c.ingestLandingZone", "dbfs:" + landingZoneLocation)
spark.conf.set("c.ingestCopyIntoTablePath", sqlDatabasePath + "/copyIntoTable")

# COMMAND ----------


