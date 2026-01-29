# Databricks notebook source
from pyspark.sql.functions import col, to_date, when, lit

# 1. Bronze Table load 
df_matches = spark.table("workspace.default.bronze_matches")

# 2. Data Transformation
df_matches_silver = df_matches \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumnRenamed("id", "match_id") \
    .withColumn("city", when(col("city").isNull(), "Unknown").otherwise(col("city"))) \
    .withColumn("winner", when(col("winner").isNull(), "No Result").otherwise(col("winner"))) \
    .withColumn("player_of_match", when(col("player_of_match").isNull(), "None").otherwise(col("player_of_match")))

# 3. Data Checking
display(df_matches_silver)

# COMMAND ----------

# 1. Bronze Deliveries load 
df_deliveries = spark.table("workspace.default.bronze_deliveries")

# 2. Transformations
# Kabhi kabhi 'id' column match_id hota hai, use rename karte hain taaki join karne me aasani ho
df_deliveries_silver = df_deliveries \
    .withColumnRenamed("id", "match_id") \
    .withColumn("total_runs", col("total_runs").cast("integer")) \
    .withColumn("is_wicket", col("is_wicket").cast("integer")) \
    .withColumn("over", col("over").cast("integer")) \
    .withColumn("ball", col("ball").cast("integer"))

# 3. Data Check
display(df_deliveries_silver)

# COMMAND ----------

# Silver Matches
df_matches_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_matches")

# Silver Deliveries
df_deliveries_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_deliveries")

print("🥈 Silver Layer Tables created successfully!")

# COMMAND ----------

