# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    TimestampType,
    IntegerType,
)
from pyspark.sql import DataFrame
import dlt

# COMMAND ----------

spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", True)

# COMMAND ----------

source_bucket = "s3://bronze-316962180131"

# COMMAND ----------

def flatten(df: DataFrame) -> DataFrame:
   """
   Function flatten the complex data, which explodes the Arrays and Expand all Structs schema
   
   Paramater: Dataframe to be exploded
   eg: flatten(df)
   """
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   
   print(len(complex_fields))
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         
         df=df.select("*", f"{col_name}.*").drop(col_name)
         print(f"dropped column : {col_name}")
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,F.explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

@dlt.table(comment="Raw data from participant attributes")
def bronze_participant_attributes():
  df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiline",True)
        .option("inferSchema",True)
        .load(source_bucket + '/PARTICIPANT_ATTRIBUTES/catalog_year=2023/catalog_month=09/catalog_day=25/*')
        .withColumnRenamed("conversationId","conversation_Id"))

  data = (df
     .withColumn("data", F.explode("participantData"))
    .withColumn("divisionIds", F.explode("divisionIds"))
    .withColumn("sessionIds", F.explode("data.sessionIds")).select(
    "conversation_Id",
    "organizationId",
    "data.participantAttributes.*",
    "data.participantId",
    "data.participantPurpose",
    "_type",
    "startTime",
    "_metadata.*")
    .drop("Queue_Deflection_Status")
    .withColumnRenamed("ivr.Priority", "ivrPriority")
    .withColumnRenamed("ivr.Skills", "ivrSkills"))
    
  final = data.select([F.col(column).alias(column.replace('.', '_').replace(' ', '_')) for column in data.columns])

  return(final.distinct().select("*",F.current_timestamp().alias("processing_time")))


dlt.create_streaming_table("stg_silver_participant_attributes")

dlt.apply_changes(
  target = "stg_silver_participant_attributes",
  source = "bronze_participant_attributes",
  keys = ["conversationId","participantId"],
  sequence_by = F.col("file_modification_time"),
  stored_as_scd_type = "2"
)

