# Databricks notebook source
dbutils.fs.ls("/mnt/f1datalake16/raw")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import col

# COMMAND ----------

race_schema = StructType([StructField("raceId",IntegerType(),False),StructField("year",IntegerType(),True),StructField("round",IntegerType(),True),StructField("circuitId",IntegerType(),True),StructField("name",StringType(),True),StructField("date",DateType(),True),StructField("time",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

races_df = spark.read.option("header",True)\
    .schema(race_schema)\
    .csv("/mnt/f1datalake16/raw/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_selected_col_df = races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time')) 

# COMMAND ----------

races_transformed_df = races_selected_col_df.withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("year","race_year")\
                    .withColumnRenamed("circuitId","circuit_id")


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, current_timestamp

# COMMAND ----------

races_timestamp_tranformation_df = races_transformed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')).withColumn("ingestion_time",current_timestamp())


# COMMAND ----------

display(races_timestamp_tranformation_df)

# COMMAND ----------

races_final_tranformation_df = races_timestamp_tranformation_df.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),col('name'),col('race_timestamp'),col('ingestion_time'))

# COMMAND ----------

display(races_final_tranformation_df)

# COMMAND ----------

races_final_tranformation_df.write.mode('overwrite').parquet('/mnt/f1datalake16/processed/races')
