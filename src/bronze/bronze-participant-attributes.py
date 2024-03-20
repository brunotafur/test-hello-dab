# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
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
import dlt

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

source_bucket = "s3://bronze-316962180131"

# Define the schema for the JSON data
schema = StructType(
    [
        StructField("organizationId", StringType(), True),
        StructField("conversationId", StringType(), True),
        StructField("startTime", TimestampType(), True),
        StructField("endTime", TimestampType(), True),
        StructField("divisionIds", ArrayType(StringType()), True),
        StructField(
            "participantData",
            ArrayType(
                StructType(
                    [
                        StructField("participantPurpose", StringType(), True),
                        StructField(
                            "participantAttributes",
                            StructType(
                                [
                                    StructField("Log", StringType(), True),
                                    StructField("NTLogin", StringType(), True),
                                    StructField("VDN", StringType(), True),
                                    StructField("DNIS", StringType(), True),
                                    StructField("Skill", StringType(), True),
                                    StructField("custCLI", StringType(), True),
                                    StructField("Schedule", StringType(), True),
                                    StructField("Whisper ", StringType(), True),
                                    StructField("scriptId", StringType(), True),
                                    StructField("CallType ", StringType(), True),
                                    StructField("DNIS Name", StringType(), True),
                                    StructField("Priority ", StringType(), True),
                                    StructField("QueueName", StringType(), True),
                                    StructField("SalesType", StringType(), True),
                                    StructField("TargetType", StringType(), True),
                                    StructField("CountryCode", StringType(), True),
                                    StructField("Flow Status", StringType(), True),
                                    StructField("LOG_QUEUEID", StringType(), True),
                                    StructField("SubMenuName ", StringType(), True),
                                    StructField("NoSurveyOptIn", StringType(), True),
                                    StructField("PlanningUnit ", StringType(), True),
                                    StructField("ScreenPopName", StringType(), True),
                                    StructField("AlbaniaXferMsg", StringType(), True),
                                    StructField("Closed Message", StringType(), True),
                                    StructField("Schedule Group", StringType(), True),
                                    StructField("DisconnectAudio", StringType(), True),
                                    StructField("Emergency Group", StringType(), True),
                                    StructField("Holiday Message", StringType(), True),
                                    StructField("MenuTargetValue ", StringType(), True),
                                    StructField("Survey Workflow", StringType(), True),
                                    StructField("Welcome Message", StringType(), True),
                                    StructField("Deflection Status", StringType(), True),
                                    StructField(
                                        "Deflection Message", StringType(), True
                                    ),
                                    StructField(
                                        "ExternalXferNumber ", StringType(), True
                                    ),
                                    StructField(
                                        "Routing_Decision_DT", StringType(), True
                                    ),
                                    StructField(
                                        "Albania Xfer Message", StringType(), True
                                    ),
                                    StructField(
                                        "External Xfer Number", StringType(), True
                                    ),
                                    StructField(
                                        "CallRecording Message", StringType(), True
                                    ),
                                    StructField(
                                        "Information Message 2", StringType(), True
                                    ),
                                    StructField(
                                        "Informational Message", StringType(), True
                                    ),
                                    StructField(
                                        "Emergency Announcement", StringType(), True
                                    ),
                                    StructField(
                                        "Tirana Emergency Check", StringType(), True
                                    ),
                                    StructField(
                                        "varRoutingDecisionName", StringType(), True
                                    ),
                                    StructField(
                                        "Queue Deflection Status", StringType(), True
                                    ),
                                    StructField(
                                        "Queue_Deflection_Status", StringType(), True
                                    ),
                                    StructField(
                                        "Queue Deflection Message", StringType(), True
                                    ),
                                    StructField(
                                        "Deflection Schedule Group", StringType(), True
                                    ),
                                    StructField(
                                        "Log_SurveyWorkflowStartTime",
                                        StringType(),
                                        True,
                                    ),
                                    StructField(
                                        "Queue Deflection Schedule Group",
                                        StringType(),
                                        True,
                                    ),
                                    StructField("ivr.Priority", StringType(), True),
                                    StructField("ivr.Skills", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("participantId", StringType(), True),
                        StructField("sessionIds", ArrayType(StringType()), True),
                    ]
                ),
                True,
            ),
        ),
        StructField("_type", StringType(), True),
    ]
)

# COMMAND ----------

@dlt.table(comment="Raw data from participant attributes")
def bronze_participant_attributes():
    df =  (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline",True)
        .option("inferSchema", True)
        .load(source_bucket + "/PARTICIPANT_ATTRIBUTES/*/*/*/*")
        .select("*","_metadata.file_name", "_metadata.file_path","_metadata.file_modification_time"))

    return(df)

dlt.create_streaming_table("stg_silver_participant_attributes")

dlt.apply_changes(
  target = "stg_silver_participant_attributes",
  source = "bronze_participant_attributes",
  keys = ["conversationId"],
  sequence_by = F.col("file_modification_time"),
  stored_as_scd_type = "2"
)
