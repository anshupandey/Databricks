# Databricks notebook source
df = spark.createDataFrame([(101,"Anshu",21,"male"), (102,"Paras",31,"male"),
                            (103,"Shubham",25,"male"), (104,"Deepak",32,"male"),],["ID","Name","Age","Gender"])

display(df)

# COMMAND ----------

import os
# Initialize a new directory
os.makedirs("/dbfs/delta/mydata",exist_ok=True)

# COMMAND ----------

delta_dir = "/dbfs/delta/mydata"

# COMMAND ----------

df.write.format("delta").saveAsTable("People3")

# COMMAND ----------

df.write.format("delta").save(delta_dir)

# COMMAND ----------

for f in os.listdir("/dbfs/delta/mydata"):
    print(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modify Delta Tables

# COMMAND ----------

from delta.tables import *

#load the table
dt = DeltaTable.forPath(spark,"/dbfs/delta/mydata/")

# COMMAND ----------

display(dt)

# COMMAND ----------

# update operation

dt.update(condition="ID == 101", 
          set = {"Age":"44"})


# COMMAND ----------

for f in os.listdir("/dbfs/delta/mydata"):
    print(f)

# COMMAND ----------

# update operation

dt.update(condition="ID == 101", 
          set = {"Name":"'Donald Trump'"})


# COMMAND ----------

for f in os.listdir("/dbfs/delta/mydata"):
    print(f)

# COMMAND ----------

!ls /dbfs/delta/mydata

# COMMAND ----------

# update operation

dt.update(condition="ID == 104", 
          set = {"Name":"'Donald Trump'"})


# COMMAND ----------

dfV0 = spark.read.format("delta").option("versionOf",0).load("/dbfs/delta/mydata")
dfV0.show()

# COMMAND ----------

dfV1 = spark.read.format("delta").option("versionOf",1).load("/dbfs/delta/mydata")
dfV1.show()

# COMMAND ----------

dfV3 = spark.read.format("delta").option("versionOf",3).load("/dbfs/delta/mydata")
dfV3.show()

# COMMAND ----------

dfV4 = spark.read.format("delta").option("versionOf",4).load("/dbfs/delta/mydata")
dfV4.show()

# COMMAND ----------

# update operation

dt.update(condition="ID == 104", 
          set = {"Name":"'Joe Biden'"})


# COMMAND ----------

dfV5 = spark.read.format("delta").option("versionOf",5).load("/dbfs/delta/mydata")
dfV5.show()

# COMMAND ----------

# update operation

dt.update(condition="ID == 104", 
          set = {"Age":"67"})


# COMMAND ----------

dfV6 = spark.read.format("delta").option("versionOf",6).load("/dbfs/delta/mydata")
dfV6.show()

# COMMAND ----------

dt.history(10).show(10,False,True)

# COMMAND ----------


