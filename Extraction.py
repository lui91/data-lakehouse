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
# MAGIC USE ${c.ingestDatabase};
# MAGIC CREATE OR REPLACE TABLE flights_data_landing
# MAGIC (
# MAGIC   FlightDate timestamp,
# MAGIC   Airline STRING,
# MAGIC   Origin STRING,
# MAGIC   Dest STRING,
# MAGIC   Cancelled BOOLEAN,
# MAGIC   Diverted BOOLEAN,
# MAGIC   CRSDepTime LONG,
# MAGIC   DepTime DOUBLE,
# MAGIC   DepDelayMinutes DOUBLE,
# MAGIC   DepDelay DOUBLE,
# MAGIC   ArrTime DOUBLE,
# MAGIC   ArrDelayMinutes DOUBLE,
# MAGIC   AirTime DOUBLE,
# MAGIC   CRSElapsedTime DOUBLE,
# MAGIC   ActualElapsedTime DOUBLE,
# MAGIC   Distance DOUBLE,
# MAGIC   Year LONG,
# MAGIC   Quarter LONG,
# MAGIC   Month LONG,
# MAGIC   DayofMonth LONG,
# MAGIC   DayOfWeek LONG,
# MAGIC   Marketing_Airline_Network STRING,
# MAGIC   Operated_or_Branded_Code_Share_Partners STRING,
# MAGIC   DOT_ID_Marketing_Airline LONG,
# MAGIC   IATA_Code_Marketing_Airline STRING,
# MAGIC   Flight_Number_Marketing_Airline LONG,
# MAGIC   Operating_Airline STRING,
# MAGIC   DOT_ID_Operating_Airline LONG,
# MAGIC   IATA_Code_Operating_Airline STRING,
# MAGIC   Tail_Number STRING,
# MAGIC   Flight_Number_Operating_Airline LONG,
# MAGIC   OriginAirportID LONG,
# MAGIC   OriginAirportSeqID LONG,
# MAGIC   OriginCityMarketID LONG,
# MAGIC   OriginCityName STRING,
# MAGIC   OriginState STRING,
# MAGIC   OriginStateFips LONG,
# MAGIC   OriginStateName STRING,
# MAGIC   OriginWac LONG,
# MAGIC   DestAirportID LONG,
# MAGIC   DestAirportSeqID LONG,
# MAGIC   DestCityMarketID LONG,
# MAGIC   DestCityName STRING,
# MAGIC   DestState STRING,
# MAGIC   DestStateFips LONG,
# MAGIC   DestStateName STRING,
# MAGIC   DestWac LONG,
# MAGIC   DepDel15 DOUBLE,
# MAGIC   DepartureDelayGroups DOUBLE,
# MAGIC   DepTimeBlk STRING,
# MAGIC   TaxiOut DOUBLE,
# MAGIC   WheelsOff DOUBLE,
# MAGIC   WheelsOn DOUBLE,
# MAGIC   TaxiIn DOUBLE,
# MAGIC   CRSArrTime LONG,
# MAGIC   ArrDelay DOUBLE,
# MAGIC   ArrDel15 DOUBLE,
# MAGIC   ArrivalDelayGroups DOUBLE,
# MAGIC   ArrTimeBlk STRING,
# MAGIC   DistanceGroup LONG,
# MAGIC   DivAirportLandings DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data to delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO ${c.ingestDatabase}.flights_data_landing
# MAGIC FROM 'abfss://airlines@syntweetsstorage.dfs.core.windows.net/*.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true', 'header' = 'true')

# COMMAND ----------


