# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.put('/tmp/abc.txt', 'Welcome to Databricks File System', True)

# COMMAND ----------

access_key = "youraccesskey"
secret_key = "yoursecretaccesskey"

#Mount bucket on databricks
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "bkt-nithin-04aug"
mount_name = "nithinawss3"
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

mount_name = "mountname"
file_name="iris.csv"
df = spark.read.format("csv").load("/mnt/%s/%s" % (mount_name , file_name))
df.show()
#spark.read.format("csv").load("s3://bkt-nithin-04aug/iris.csv")
