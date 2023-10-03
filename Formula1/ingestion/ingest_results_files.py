# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId",IntegerType(),False),
                                      StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("constructorId",IntegerType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("grid",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("positionText",StringType(),True),
                                      StructField("positionOrder",IntegerType(),True),
                                      StructField("points",FloatType(),True),
                                      StructField("laps",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True),
                                      StructField("fastestLap",IntegerType(),True),
                                      StructField("rank",IntegerType(),True),
                                      StructField("fastestLapTime",StringType(),True),
                                      StructField("fastestLapSpeed",StringType(),True),
                                      StructField("statusId",IntegerType(),True)]) 

# COMMAND ----------

results_df =spark.read.schema(results_schema)\
                      .json("/mnt/f1datalake16/raw/results.json")

# COMMAND ----------

results_final_df = results_df.withColumnRenamed('resultId','result_id')\
                       .withColumnRenamed('raceId','race_id')\
                       .withColumnRenamed('driverId','driver_id')\
                       .withColumnRenamed('constructorId','constructor_id')\
                       .withColumnRenamed('positionText','position_text')\
                       .withColumnRenamed('positionOrder','position_order')\
                       .withColumnRenamed('fastestLap','fastest_lap')\
                       .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                       .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                       .withColumn('ingestion_time',current_timestamp())\
                       .drop('statusId')        


                                 

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.partitionBy('race_id').mode('overwrite').parquet('/mnt/f1datalake16/processed/results')
