# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Exploration
# MAGIC
# MAGIC ### Million Song Dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC ls "/databricks-datasets/songs"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls "/databricks-datasets/songs/data-001"

# COMMAND ----------

# MAGIC %fs head --maxBytes=5000 "/databricks-datasets/songs/README
# MAGIC .md"

# COMMAND ----------

path = "/databricks-datasets/songs/data-001/part-00004"
df = spark.read.format("csv").option("sep","\t").load(path)
display(df)

# COMMAND ----------


