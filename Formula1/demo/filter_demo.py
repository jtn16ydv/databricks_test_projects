# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered__race_years_df = races_df.filter("race_year= 2019")

# COMMAND ----------

display(races_filtered__race_years_df)

# COMMAND ----------

races_filtered_round_df = races_df.filter("race_year= 2019 and round <=5")

# COMMAND ----------

display(races_filtered_round_df)

# COMMAND ----------

races_python_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"]<=5))

# COMMAND ----------

display(races_python_df)
