# Databricks notebook source

from pyspark.sql.functions import sum, count, col, round, avg, when

# 1. Silver Deliveries load
df_deliveries = spark.table("workspace.default.silver_deliveries")

# 2. Batting Stats Calculate
player_stats = df_deliveries.groupBy("batter").agg(
    sum("batsman_runs").alias("total_runs"),
    count("ball").alias("balls_faced"),
    count(when(col("batsman_runs") == 4, True)).alias("fours"),
    count(when(col("batsman_runs") == 6, True)).alias("sixes")
)

# 3. Strike Rate add
gold_player_stats = player_stats.withColumn(
    "strike_rate", 
    round((col("total_runs") / col("balls_faced")) * 100, 2)
).orderBy(col("total_runs").desc())

# 4. Save as Gold Table
gold_player_stats.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_player_stats")

display(gold_player_stats)

# COMMAND ----------

# 1. Silver Matches load 
df_matches = spark.table("workspace.default.silver_matches")

# 2. Select only important columns
# We want to predict 'Result' (Winner)n
ml_features_df = df_matches.select(
    "match_id",
    "season",
    "city",
    "venue",
    "team1",
    "team2",
    "toss_winner",
    "toss_decision",
    "winner"  # Our Target (Label)
)

# 3. Removing Null values 
ml_features_df = ml_features_df.dropna()

# 4. Save as Gold Table for ML
ml_features_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.gold_match_features")

print("Gold Tables ready! Data is now ready for AI/ML.")
display(ml_features_df)

# COMMAND ----------

