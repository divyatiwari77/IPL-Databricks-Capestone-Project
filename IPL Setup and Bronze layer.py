# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/ipl/"))

# COMMAND ----------

# 2. Database (Schema) set
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("USE workspace.default")

# 3. Reading Matches File (Volume Path)
df_matches = spark.read.csv("/Volumes/workspace/default/ipl/matches.csv", header=True, inferSchema=True)
df_matches.write.mode("overwrite").saveAsTable("bronze_matches")

# 4. Reading Deliveries File
df_deliveries = spark.read.csv("/Volumes/workspace/default/ipl/deliveries.csv", header=True, inferSchema=True)
df_deliveries.write.mode("overwrite").saveAsTable("bronze_deliveries")

print("Tables are ready in Unity Catalog!")

# COMMAND ----------

