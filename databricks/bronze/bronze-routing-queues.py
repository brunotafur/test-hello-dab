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
import dlt

# COMMAND ----------

source_bucket = "s3://bronze-316962180131"

# Define the schema for the JSON structure
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("division", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("selfUri", StringType(), True)
    ]), True),
    StructField("dateCreated", TimestampType(), True),
    StructField("dateModified", TimestampType(), True),
    StructField("modifiedBy", StringType(), True),
    StructField("createdBy", StringType(), True),
    StructField("memberCount", StringType(), True),
    StructField("userMemberCount", StringType(), True),
    StructField("joinedMemberCount", StringType(), True),
    StructField("mediaSettings", StructType([
        StructField("call", StructType([
            StructField("alertingTimeoutSeconds", DoubleType(), True),
            StructField("serviceLevel", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("durationMs", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("callback", StructType([
            StructField("alertingTimeoutSeconds", DoubleType(), True),
            StructField("serviceLevel", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("durationMs", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("chat", StructType([
            StructField("alertingTimeoutSeconds", DoubleType(), True),
            StructField("serviceLevel", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("durationMs", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("email", StructType([
            StructField("alertingTimeoutSeconds", DoubleType(), True),
            StructField("serviceLevel", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("durationMs", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("message", StructType([
            StructField("alertingTimeoutSeconds", DoubleType(), True),
            StructField("serviceLevel", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("durationMs", DoubleType(), True)
            ]), True)
        ]), True)
    ]), True),
    StructField("predictiveRouting", StructType([
        StructField("enableConversationScoreBiasing", BooleanType(), True)
    ]), True),
    StructField("acwSettings", StructType([
        StructField("wrapupPrompt", StringType(), True),
        StructField("timeoutMs", DoubleType(), True)
    ]), True),
    StructField("skillEvaluationMethod", StringType(), True),
    StructField("autoAnswerOnly", BooleanType(), True),
    StructField("enableTranscription", BooleanType(), True),
    StructField("enableManualAssignment", BooleanType(), True),
    StructField("agentOwnedRouting", StructType([
        StructField("enableAgentOwnedCallbacks", BooleanType(), True),
        StructField("maxOwnedCallbackHours", DoubleType(), True),
        StructField("maxOwnedCallbackDelayHours", DoubleType(), True)
    ]), True),
    StructField("callingPartyNumber", StringType(), True),
    StructField("defaultScripts", StructType({}), True),
    StructField("suppressInQueueCallRecording", BooleanType(), True),
    StructField("selfUri", StringType(), True)
])

# COMMAND ----------

@dlt.table(name="bronze_routing_queues", comment="Raw data from routing queues" )
def bronze_routing_queues():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .load(source_bucket + "/ROUTING_QUEUES/*/*/*/*")
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
        )
    )

dlt.create_streaming_table("stg_silver_routing_queues")

dlt.apply_changes(
  target = "stg_silver_routing_queues",
  source = "bronze_routing_queues",
  keys = ["id"],
  sequence_by = F.col("file_modification_time"),
  stored_as_scd_type = "2"
)
