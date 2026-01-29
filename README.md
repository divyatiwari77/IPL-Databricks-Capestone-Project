# IPL-Databricks-Capestone-Project
Domain: Social Media & Content (Sports Analytics)
Challenge: IPL Pulse: End-to-End Analytics & Prediction Engine
Author: [Divya Tiwari]

**1. 🎯 Problem Statement**
__The Uncertainty in Cricket Analytics_

The Indian Premier League (IPL) is a high-stakes ecosystem involving team owners, broadcasters, and millions of fantasy league players. However, decision-making in cricket is often driven by intuition rather than hard data.

For Fantasy Players: Creating a winning team is difficult due to the sheer volume of historical stats (Venue factors, Toss impact, Player form).
For Team Strategists: Predicting match outcomes requires analyzing complex, non-linear relationships between variables like toss decisions, home-ground advantage, and toss winners.
The Objective:
To build a scalable Lakehouse Solution that ingests raw ball-by-ball data, processes it into a reliable source of truth, and leverages Machine Learning to predict match winners, thereby reducing uncertainty and providing actionable insights.

**2. 🛠️ The Toolkit (Tech Stack)**
To solve this problem, I utilized the Databricks Lakehouse Platform to unify Data Engineering and Data Science.

Compute & Processing: Apache Spark (PySpark) & Spark SQL for distributed data processing.
Storage & Format: Delta Lake (for ACID transactions, schema enforcement, and time travel).
Governance: Unity Catalog (Volumes) for managing raw data access and permissions.
Orchestration: Databricks Workflows (Jobs) to automate the pipeline from ingestion to model training.
Machine Learning: Spark MLlib (Random Forest Classifier) & MLflow (for experiment tracking and model registry).
Visualization: Databricks SQL Dashboard.

**3. ⚙️ The Process: From Raw Data to Intelligence**
I implemented the industry-standard Medallion Architecture (Multi-hop Architecture) to ensure data quality and lineage.

__Step 1: Ingestion (The Bronze Layer)_
Source: Raw CSV files (matches.csv, deliveries.csv) covering IPL seasons 2008–2024.
Action: Uploaded data to Unity Catalog Volumes and ingested it into Delta Tables.
Outcome: Raw tables (bronze_matches, bronze_deliveries) mirroring the source system with no data loss.
__Step 2: Transformation & Cleaning (The Silver Layer)_
Logic:
Cleaned column names (standardized naming conventions).
Parsed dates from string formats to proper Date types.
Handled missing values (e.g., winner, city) to ensure data integrity.
Removed duplicate records.
Outcome: Refined tables (silver_matches, silver_deliveries) ready for downstream analytics.
__Step 3: Aggregation & Feature Engineering (The Gold Layer)_
Logic: Created business-level aggregates.
Player Stats: Aggregated runs, boundaries, and strike rates.
Match Features: Joined match and delivery data to create feature vectors (Team A, Team B, Toss Winner, Venue, Day/Night).
Outcome: A final gold_match_features table optimized for the Machine Learning model.
__Step 4: Orchestration_
A Databricks Job was created to run these notebooks in sequence:
**Ingestion → Transformation → Aggregation → Model_Training**

**4. 🧠 The Solution: AI & Insights**
__A. Machine Learning Model__
Task: Classification (Predicting: Will Team A win or Team B?)
Algorithm: Random Forest Classifier.
__Why?__ It handles categorical variables (Stadium names, Team names) excellently and prevents overfitting better than a single Decision Tree.
Workflow:
Vector Assembly: Converted categorical features into numerical vectors using StringIndexer and VectorAssembler.
Training: Split data (80% Train, 20% Test) and trained the model.
MLflow Integration: Logged parameters, accuracy metrics, and the model artifact for version control.
Result: The model successfully predicts match outcomes with an accuracy of ~52-55% (on historical data), providing a baseline for probability assessment.
__B. Key Business Insights (Dashboard)__
Using Databricks SQL, I derived the following insights:

Toss Advantage: At specific venues (e.g., Chinnaswamy Stadium), teams winning the toss have a significantly higher win percentage (Chasing bias).
Player Impact: Identified "High Impact" players who have high strike rates in death overs, crucial for Fantasy Captain selection.
Venue Patterns: Average scores vary drastically by venue, aiding in score prediction.

__5. 🚀 How to Run This Project__
Clone the Repo: Import the code into your Databricks Workspace.
Setup Data:
Create a volume: /Volumes/workspace/default/ipl_data.
Upload matches.csv and deliveries.csv.
Run the Pipeline:
Navigate to Workflows in Databricks.
Create a Job that runs the notebooks in order: _01_Bronze -> 02_Silver -> 03_Gold -> 04_ML_Training__
Explore: Check the generated Delta tables in the Catalog Explorer and view the MLflow experiment run.

**6. 🔮 Future Scope**
Real-time Inference: Implement Spark Structured Streaming to predict win probability ball-by-ball during a live match.
Hyperparameter Tuning: Use Hyperopt to improve model accuracy beyond 60%.
📌 Note for Evaluators
This project demonstrates a complete lifecycle: Data Engineering (ETL), Data Governance (Unity Catalog), and Data Science (MLflow) working in harmony on the Databricks Lakehouse.
