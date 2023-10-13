# Databricks notebook source
# MAGIC %md
# MAGIC #####Ingest circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC ######Read the CSV file using spark dataframe reader

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/circuits.csv"))

# COMMAND ----------

from pyspark.sql.types import StringType,StructField, StructType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId",IntegerType(),False),
                              StructField("circuitRef",StringType(),True),
                              StructField("name",StringType(),True),
                              StructField("location",StringType(),True),
                              StructField("lat",DoubleType(),True),
                              StructField("lng",DoubleType(),True),
                              StructField("alt",IntegerType(),True),
                              StructField("url",StringType(),True)])

# COMMAND ----------

# circuits_df = spark.read\
#     .option("header",True)\
#     .option("inferSchema",True)\
#     .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df = spark.read\
    .option("header",True)\
    .schema(circuits_schema)\
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Select only the required columns 
# MAGIC ###### need to drop url column
# MAGIC

# COMMAND ----------

circuits_select_df = circuits_df.select("circuitId","circuitRef","name","location","lat","lng","alt") 

# COMMAND ----------

circuits_select_df2 = circuits_df.select(circuits_df.circuit_Id,circuits_df.circuit_Ref,circuits_df.name,circuits_df.location,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

circuits_select_df3 =circuits_df.select(circuits_df["circuit_Id"],circuits_df['circuit_Ref'],circuits_df['name'],circuits_df['location'],circuits_df['lat'],circuits_df['lng'])

# COMMAND ----------

from pyspark.sql.functions import col
circuits_select_df4 =circuits_df.select( col("circuit_Id"),col("circuit_Ref").alias("Ref") , col("name"),col("location"),col("lat"),col("lng"),col("lng"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step-3 Rename the columns using Dataframe API withcolumn

# COMMAND ----------

# Creating a column source type source type just to get we can get vlues via widgets
dbutils.widgets.text("p_data_source_type","")
v_datasource =ddbutils.widgets.get("p_data_source_type")

# you can add this to datafram using withcolumn now
#circuits_renamed_df = circuits_select_df.withColumn("datasource",lit(v_datasource))

# COMMAND ----------

circuits_renamed_df = circuits_select_df.withColumnRenamed("circuitId","circuit_Id")\
                                        .withColumnRenamed("circuitRef","circuit_Ref")\
                                        .withColumnRenamed("lat","latitude")\
                                        .withColumnRenamed("lng","longitude")\
                                        .withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add the ingested date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = add_ingestion_date(circuits_renamed_df)
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

#Suppose we want to add another coulmn where we pass the value manually it won't work like te way above worked because current_timestamp gives us column object
# Below example will give you error
# circuits_final_test_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()\
#                                             .withColumn("env","Production")

##to resolve this we use lit
circuits_final_test_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())\
                                             .withColumn("env",lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step-5 Write the df to  parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
