# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("stop",IntegerType(),False),
                                      StructField("lap",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pit_stops_df =spark.read.schema(pit_stops_schema)\
                      .option("multiLine",True)\
                      .json("/mnt/f1datalake16/raw/pit_stops.json")

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId','race_id')\
                       .withColumnRenamed('driverId','driver_id')\
                        .withColumn('ingestion_time', current_timestamp())

                                 

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet('/mnt/f1datalake16/processed/pit_stops')
