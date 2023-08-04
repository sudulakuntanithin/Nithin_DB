# Databricks notebook source
# MAGIC %md
# MAGIC # Dead Data DataSet

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

# COMMAND ----------

data.cache()  # Cache data for faster reuse

data = data.dropna()   # Remove Missing values

# COMMAND ----------

# MAGIC %python
# MAGIC data.take(5)
# MAGIC display(data)

# COMMAND ----------

data.createOrReplaceTempView("data_geo")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_geo

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming DataSet

# COMMAND ----------

file_path = "/databricks-datasets/structured-streaming/events"
checkpoint_path= "/tmp/ss-tutorial/_checkpoint"

raw_df =(spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("cloudFiles.schemaLocation",checkpoint_path).load(file_path))

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp
transformed_df = (
    raw_df.select("*",
                  col("_metadata.file_path").alias("source_file"),
                  current_timestamp().alias("processing_time")
                  )
    )

# COMMAND ----------

target_path = "/tmp/ss-tutorial/"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

transformed_df.writeStream.trigger(availableNow=True).option("checkpointLocation",checkpoint_path).option("path",target_path).start()

# COMMAND ----------
