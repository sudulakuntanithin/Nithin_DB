# Databricks notebook source
storage_account="saprojectgroup"
container_name="data"
source_url= "wasbs://{0}@{1}.blob.core.windows.net".format(container_name,storage_account)
access_key = dbutils.secrets.get(scope= "storage-account",key= "sa-key")
mount_point_url = "/mnt/mountsa"
extra_configs_key= f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
extra_configs_value=access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs= extra_configs_dict)


dbutils.fs.ls(mount_point_url)

# COMMAND ----------

customer_data = spark.read.csv("dbfs:/mnt/mountsa/customer_data.csv", header=True, inferSchema=True)

customer_data.show()

# COMMAND ----------

product_data = spark.read.csv("dbfs:/mnt/mountsa/product_data.csv", header=True, inferSchema=True)
product_data.show()

# COMMAND ----------

weblogs_data = spark.read.option("multiline","true").json("dbfs:/mnt/mountsa/web_logs.json")

# COMMAND ----------

weblogs_data.show(5)

# COMMAND ----------

from pyspark.sql.types import IntegerType
weblogs_tmp = weblogs_data.withColumn('product_id', weblogs_data['product_id'].substr(-3,3).cast(IntegerType()))
weblogs_tmp.show(5)

# COMMAND ----------

products_table = product_data.createOrReplaceTempView('Products')
web_logs_table = weblogs_data.createOrReplaceTempView('Logs')
customer_table = customer_data.createOrReplaceTempView('Customer')

# COMMAND ----------

full_data = spark.sql("SELECT L.log_id, L.timestamp, L.customer_id, L.product_id, L.action, L.quantity, p.product_name, p.category, p.price, c.first_name, c.last_name, c.email, c.phone FROM Products p join Logs L on p.product_id = L.product_id join Customer c  on L.customer_id = c.customer_id")

# COMMAND ----------

full_data.show(5)

# COMMAND ----------


