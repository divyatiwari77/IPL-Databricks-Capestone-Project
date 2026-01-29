# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     batter, 
# MAGIC     total_runs, 
# MAGIC     strike_rate 
# MAGIC FROM workspace.default.gold_player_stats 
# MAGIC ORDER BY total_runs DESC 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     toss_decision, 
# MAGIC     count(*) as count 
# MAGIC FROM workspace.default.gold_match_features 
# MAGIC GROUP BY toss_decision

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     season, 
# MAGIC     team1, 
# MAGIC     team2, 
# MAGIC     winner, 
# MAGIC     venue 
# MAGIC FROM workspace.default.gold_match_features 
# MAGIC ORDER BY season DESC 
# MAGIC LIMIT 20

# COMMAND ----------

