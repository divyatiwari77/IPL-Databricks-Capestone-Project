# Databricks notebook source
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# 1. Gold Data Load
df = spark.table("workspace.default.gold_match_features")

# 2. Text Columns to Numbers (Encoding)
categorical_cols = ["team1", "team2", "toss_winner", "toss_decision", "venue", "city"]
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in categorical_cols]

# Target Variable (Winner) to number
label_indexer = StringIndexer(inputCol="winner", outputCol="label", handleInvalid="keep")

# 3.Vector Assembler
feature_cols = [col+"_index" for col in categorical_cols]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 4. Create Pipeline
pipeline = Pipeline(stages=indexers + [label_indexer, assembler])

# Data transform
model_data = pipeline.fit(df).transform(df)

# Check Data
display(model_data.select("features", "label"))

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os

# 1. Data Split
train_data, test_data = model_data.randomSplit([0.8, 0.2], seed=42)

# 2. Setting the Path
volume_path = "/Volumes/workspace/default/ipl/mlflow_temp"

# 3. MLflow Experiment Start
with mlflow.start_run(run_name="IPL_Winner_Prediction"):
    
    # A. Model Defining (maxBins=100 wala fix bhi included hai)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxBins=100)
    
    # B. Model Training
    rf_model = rf.fit(train_data)
    
    # C. Prediction
    predictions = rf_model.transform(test_data)
    
    # D. Accuracy Check
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    
    print(f"Model Accuracy: {accuracy*100:.2f}%")
    
    # E. Log Model 
    mlflow.log_metric("accuracy", accuracy)
    mlflow.spark.log_model(rf_model, "ipl_rf_model", dfs_tmpdir=volume_path)

print("Training Complete! Model saved successfully in Volume.")

# COMMAND ----------

