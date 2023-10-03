# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Read the json file using dataframe reader api

# COMMAND ----------

constructors_schema = ("constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING")

# COMMAND ----------

# DBTITLE 1,Read the json file
df = spark.read.schema(constructors_schema).json('/mnt/f1datalake16/raw/constructors.json')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Remove url column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructors_remove_col_df = df.drop(col('url'))

# COMMAND ----------

construct_final_df = constructors_remove_col_df.withColumnRenamed('constructorId','constructor_id')\
                          .withColumnRenamed('constructorRef','constructor_ref')\
                          .withColumn('ingestion_time',current_timestamp())

# COMMAND ----------

display(construct_final_df)

# COMMAND ----------

construct_final_df.write.mode('overwrite').parquet('/mnt/f1datalake16/processed/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/mnt/f1datalake16/processed/constructors"
