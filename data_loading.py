# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS COVID")

# COMMAND ----------

service_credential = "99b8Q~ubw8MvRwCEzKtU-Z8NEYkjxgieq1AB8afA"

spark.conf.set("fs.azure.account.auth.type.syntweetsstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.syntweetsstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.syntweetsstorage.dfs.core.windows.net", "4ced5214-f937-4cef-b680-5395a174647c")
spark.conf.set("fs.azure.account.oauth2.client.secret.syntweetsstorage.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.syntweetsstorage.dfs.core.windows.net", "https://login.microsoftonline.com/6497bc6d-7e82-48c2-b571-9e9489ce1a4c/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://covid@syntweetsstorage.dfs.core.windows.net/")

# COMMAND ----------

file_location = "abfss://covid@syntweetsstorage.dfs.core.windows.net/"
# file_location = "wasbs://covid"

# COMMAND ----------

df = spark.read.csv(file_location, inferSchema=True, header=True)

# COMMAND ----------

df.count()

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.columns

# COMMAND ----------

type(df)

# COMMAND ----------

from pyspark.sql.functions import col, when, count, to_timestamp

# COMMAND ----------

df.select([count(when(col(loop_col).isNull(), loop_col)).alias(loop_col) for loop_col in df.columns]).display()

# COMMAND ----------

df = df.withColumn('Last_Update', to_timestamp('Last_Update'))
df = df.withColumn('Lat', col('Lat').astype('float'))
df = df.withColumn('Long_', col('Long_').astype('float'))
df = df.withColumn('Confirmed', col('Confirmed').astype('int'))
df = df.withColumn('Deaths', col('deaths').astype('int'))
df = df.withColumn('Recovered', col('Recovered').astype('int'))
df = df.withColumn('Active', col('Active').astype('int'))
df = df.withColumn('Incident_Rate', col('Incident_Rate').astype('float'))
df = df.withColumn('Case_Fatality_Ratio', col('Case_Fatality_Ratio').astype('float'))
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create delta table

# COMMAND ----------

df.write.format("delta").saveAsTable('covid.covid_landing')

# COMMAND ----------

spark.read.json()
