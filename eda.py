# Databricks notebook source
df = spark.read.table('covid.covid_landing')

# COMMAND ----------

from pyspark.sql.functions import col, when, count, min, to_date

# COMMAND ----------

df.select([count(when(col(loop_col).isNull(), loop_col)).alias(loop_col) for loop_col in df.columns]).display()

# COMMAND ----------

df = df.dropna(subset=['Lat', 'Long_', 'Country_Region', 'Confirmed', 'Deaths', 'Recovered'])

# COMMAND ----------

df.select([count(when(col(loop_col).isNull(), loop_col)).alias(loop_col) for loop_col in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Find the country with more deaths 

# COMMAND ----------

from pyspark.sql.functions import desc, sum, asc

# COMMAND ----------

df.groupBy('Country_region').agg(sum("Deaths").alias("Deaths")).sort(desc("Deaths")).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Country with less deaths

# COMMAND ----------

df.groupBy("Country_region").agg(sum('Deaths').alias('Deaths')).where(col("Country_region") == "Nauru").sort(asc('Deaths')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Country with more confirmed cases 

# COMMAND ----------

df.groupBy('Country_region').agg(sum('Confirmed').alias('Confirmed')).sort(desc('Confirmed')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Country with less confirmed cases

# COMMAND ----------

df.groupBy('Country_region').agg(sum('Confirmed').alias('Confirmed')).sort(asc('Confirmed')).limit(5).display()

# COMMAND ----------

df.select('Confirmed').summary().show()

# COMMAND ----------

df.columns

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import lit, avg
df_test = df.withColumn('test-data', lit(1))

# COMMAND ----------

df_test.columns

# COMMAND ----------

df_test = df_test.withColumnRenamed('test-data', 'test_data')

# COMMAND ----------

df_test.test_data

# COMMAND ----------

top_five = df.groupBy('Country_region').agg(avg('Deaths').alias('Deaths')).sort(desc('Deaths')).limit(5)
top_five.display()

# COMMAND ----------

death_avg = df.agg(avg('Deaths'))
death_avg.display()

# COMMAND ----------

df.filter("Last_Update = YEAR(2020)")

# COMMAND ----------

df.agg(min('Last_Update')).show()

# COMMAND ----------

df.filter(to_date('Last_Update', "yyyy-MM-dd").between("2020-01-01", "2020-12-31")).limit(5).display()

# COMMAND ----------

df.filter(col('Country_region') == 'US').count()

# COMMAND ----------

df.select([count(when(col('Country_region') == loop_col, loop_col)).alias(loop_col) for loop_col in ['US', 'Mexico']]).display()

# COMMAND ----------


