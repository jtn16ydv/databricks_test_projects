# Databricks notebook source
# MAGIC %md
# MAGIC #####Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# circuits_df = spark.read.parquet("/mnt/f1datalake16/processed/circuits")\
#     .withColumnRenamed("name", "circuits_name")

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/f1datalake16/processed/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name", "circuits_name")

# COMMAND ----------

races_df = spark.read.parquet("/mnt/f1datalake16/processed/races").filter("race_year = 2019")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "inner")\
    .select(circuits_df.circuits_name,circuits_df.location,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## left outer join

# COMMAND ----------

race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "left")\
    .select(circuits_df.circuits_name,circuits_df.location,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## right outer join

# COMMAND ----------

race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "right")\
    .select(circuits_df.circuits_name,circuits_df.location,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## full outer join

# COMMAND ----------

race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "full")\
    .select(circuits_df.circuits_name,circuits_df.location,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Joins

# COMMAND ----------

#same ike inner join but you can select only left columns


race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "semi")\
    .select(circuits_df.circuits_name,circuits_df.location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti Joins

# COMMAND ----------

# hhis is oppaosite of anti you will not find macthing columns but the simmilarity is that you will select left side of the data frame
race_circuits_df =  circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_id , "anti")\
    .select(circuits_df.circuits_name,circuits_df.location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Join
