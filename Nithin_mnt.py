# Databricks notebook source
storage_account="v1safordatabricks"
container_name="container1"
source_url= "wasbs://{0}@{1}.blob.core.windows.net".format(container_name,storage_account)
access_key="removed_my_access_key"
mount_point_url = "/mnt/iris.csv"
extra_configs_key= f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
extra_configs_value=access_key
extra_configs_dict = {extra_configs_key:extra_configs_value}

# COMMAND ----------

dbutils.fs.mount(source = source_url,
                 mount_point = mount_point_url,
                 extra_configs= extra_configs_dict)

# COMMAND ----------

dbutils.fs.ls(mount_point_url)
