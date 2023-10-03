# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId",IntegerType(),False),
                                        StructField("raceId",IntegerType(),True),
                                        StructField("driverId",IntegerType(),True),
                                        StructField("constructorId",IntegerType(),True),
                                        StructField("number",IntegerType(),True),
                                        StructField("position",IntegerType(),True),
                                        StructField("q1",StringType(),True),
                                        StructField("q2",StringType(),True),
                                        StructField("q3",StringType(),True)
                                        ])

# COMMAND ----------

qualifying_df= spark.read.schema(qualifying_schema)\
                .option('multiLine',True)\
                .json('/mnt/f1datalake16/raw/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("raceId","race_id")\
        .withColumnRenamed("driverId","driver_id")\
            .withColumnRenamed("constructorId","constructo_id")\
                .withColumn("ingstion_time",current_timestamp())


# COMMAND ----------

final_df.write.mode('overwrite').parquet('/mnt/f1datalake16/processed/qualifying')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/f1datalake16/processed/qualifying'
