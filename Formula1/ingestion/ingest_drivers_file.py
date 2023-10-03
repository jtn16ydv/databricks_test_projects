# Databricks notebook source
# MAGIC %md
# MAGIC ###### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Read json file using spark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, concat , current_timestamp, current_timestamp,concat,lit

# COMMAND ----------

name_schema = StructType(fields =[StructField('forename',StringType(),True),
                                    StructField('surname',StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                  StructField("driverRef",StringType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("code",StringType(),True),
                                  StructField("name",name_schema),
                                  StructField("dob",StringType(),True),
                                  StructField("nationality",StringType(),True),
                                  StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json('/mnt/f1datalake16/raw/drivers.json')

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                   .withColumnRenamed("driverRef","driver_ref")\
                                   .withColumn("ingestion_date",current_timestamp())\
                                   .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))    

# COMMAND ----------

display(drivers_with_column_df)

# COMMAND ----------

drivers_final_df =drivers_with_column_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.parquet("/mnt/f1datalake16/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/f1datalake16/processed/drivers"
