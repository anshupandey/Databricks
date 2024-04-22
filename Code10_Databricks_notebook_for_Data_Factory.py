# Databricks notebook source
# create folder
dbutils.widgets.text("folder", "mydata")

#get the foldername
folder = dbutils.widgets.get("folder")

# COMMAND ----------

import urllib3

# download some data and save that data into the specified folder


url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
response = urllib3.PoolManager().request('GET',url)
data = response.data.decode("utf-8")

#save data
path = "dbfs:/{0}/products.csv".format(folder)
dbutils.fs.put(path,data,True)

# COMMAND ----------

df = spark.sql("select * from insurance;")
df.count()

# COMMAND ----------


