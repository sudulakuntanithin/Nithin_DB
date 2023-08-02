# Databricks notebook source
df_remote_table = (spark.read
                   .format("sqlserver")
                   .option("host", "dbservernithin19.database.windows.net")
                   .option("port","1433")
                   .option("user", "admin123")
                   .option("password", "Admin@123")
                   .option("database", "nithindatabase")
                   .option("dbtable", "dbo.iris_data")
                   .load()
                   )

# COMMAND ----------

df_remote_table.show(5)

# COMMAND ----------


