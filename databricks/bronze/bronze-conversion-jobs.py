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
# Define the schema for the JSON data

schema = StructType([
    StructField("conversationEnd", StringType(), True),
    StructField("conversationId", StringType(), True),
    StructField("conversationStart", StringType(), True),
    StructField("divisionIds", ArrayType(StringType()), True),
    StructField("mediaStatsMinConversationMos", DoubleType(), True),
    StructField("mediaStatsMinConversationRFactor", DoubleType(), True),
    StructField("originatingDirection", StringType(), True),
    StructField("participants", ArrayType(StructType([
        StructField("attributes", StructType({
            StructField("AgentNTLogin", StringType(),True),
            StructField("CBEWTSetting", StringType(),True),
            StructField("CallbackEnabled", StringType(),True),
            StructField("ComfortMsg1",StringType(),True),
            StructField("CustomerANI",StringType(),True),
            StructField("DDI",StringType(),True),
            StructField("DNIS",StringType(),True),
            StructField("FoundQueueName",StringType(),True),
            StructField("FoundSkillName",StringType(),True),
            StructField("HoldMusic",StringType(),True),
            StructField("LandlineNoInptHD",StringType(),True),
            StructField("LegId",StringType(),True),
            StructField("Location",StringType(),True),
            StructField("Log_LegWorkflowStart",StringType(),True),
            StructField("NTLogin",StringType(),True),
            StructField("NoMobOffPromptSD",StringType(),True),
            StructField("Priority",StringType(),True),
            StructField("SMSDeflectionOffer",StringType(),True),
            StructField("SMSLandlineMxg",StringType(),True),
            StructField("SMSMsgMobileIVR",StringType(),True),
            StructField("SMSNoMobMsgIVR",StringType(),True),
            StructField("SMSOOHMobileAudio",StringType(),True),
            StructField("SMSOOHNoMobileAudio",StringType(),True),
            StructField("SMSOptInPrompt",StringType(),True),
            StructField("ScheduleGroup",StringType(),True),
            StructField("ScreenPopName",StringType(),True),
            StructField("Skill",StringType(),True),
            StructField("TIQCheck",StringType(),True),
            StructField("TIQValue",StringType(),True),
            StructField("VDN",StringType(),True),
            StructField("WhatsDeflectionSuccessMsg",StringType(),True),
            StructField("Whisper",StringType(),True),
            StructField("custCLI",StringType(),True),
            StructField("ivr.Priority",StringType(),True),
            StructField("ivr.Skills",StringType(),True),
            StructField("scriptId",StringType(),True),
            StructField("SurveyQueue",StringType(),True),
            StructField("SpainSurveryFlow",StringType(),True),
            StructField("SurveyStartTime",StringType(),True),
            StructField("SurveyConversationId",StringType(),True),
            StructField("SurveyedAgent",StringType(),True),
            StructField("CustSurveyNumber",StringType(),True),
            StructField("SurveyOrigDNIS",StringType(),True),
            StructField("Survey",StringType(),True),
            StructField("SurveyFCRScore",StringType(),True),
            StructField("SurveyCSATScore",StringType(),True),
            StructField("abandonMilliseconds",StringType(),True),
            StructField("dialerContactId",StringType(),True),
            StructField("dialerContactListId",StringType(),True),
            StructField("dialerCampaignId",StringType(),True),
            StructField("dialerInteractionId",StringType(),True),
            # StructField("scriptId",StringType(),True),
            # StructField("scriptId",StringType(),True),
        }), True),
        StructField("externalContactId", StringType(), True),
        StructField("participantId", StringType(), True),
        StructField("participantName", StringType(), True),
        StructField("purpose", StringType(), True),
        StructField("sessions", ArrayType(StructType([
            StructField("activeSkillIds", ArrayType(StringType()), True),
            StructField("ani", StringType(), True),
            StructField("destinationAddresses", ArrayType(StringType()), True),
            StructField("direction", StringType(), True),
            StructField("dnis", StringType(), True),
            StructField("edgeId", StringType(), True),
            StructField("flow", StructType([
                StructField("endingLanguage", StringType(), True),
                StructField("entryReason", StringType(), True),
                StructField("entryType", StringType(), True),
                StructField("exitReason", StringType(), True),
                StructField("flowId", StringType(), True),
                StructField("flowName", StringType(), True),
                StructField("flowType", StringType(), True),
                StructField("flowVersion", StringType(), True),
                StructField("startingLanguage", StringType(), True),
                StructField("transferTargetAddress", StringType(), True),
                StructField("transferTargetName", StringType(), True),
                StructField("transferType", StringType(), True),
            ]), True),
            StructField("flowInType", StringType(), True),
            StructField("mediaEndpointStats", ArrayType(StructType([
                StructField("codecs", ArrayType(StringType()), True),
                StructField("discardedPackets", DoubleType(), True),
                StructField("eventTime", StringType(), True),
                StructField("maxLatencyMs", DoubleType(), True),
                StructField("minMos", DoubleType(), True),
                StructField("minRFactor", DoubleType(), True),
                StructField("receivedPackets", DoubleType(), True),
            ]), True), True),
            StructField("mediaType", StringType(), True),
            StructField("metrics", ArrayType(StructType([
                StructField("emitDate", StringType(), True),
                StructField("name", StringType(), True),
                StructField("value", DoubleType(), True),
            ]), True), True),
            StructField("peerId", StringType(), True),
            StructField("protocolCallId", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("recording", BooleanType(), True),
            StructField("remote", StringType(), True),
            StructField("remoteNameDisplayable", StringType(), True),
            StructField("requestedRoutings", ArrayType(StringType()), True),
            StructField("segments", ArrayType(StructType([
                StructField("conference", BooleanType(), True),
                StructField("disconnectType", StringType(), True),
                StructField("q850ResponseCodes", ArrayType(DoubleType()), True),
                StructField("queueId", StringType(), True),
                StructField("requestedRoutingSkillIds", ArrayType(StringType()), True),
                StructField("segmentEnd", StringType(), True),
                StructField("segmentStart", StringType(), True),
                StructField("segmentType", StringType(), True),
                StructField("sipResponseCodes", ArrayType(DoubleType()), True),
                StructField("wrapUpCode", StringType(), True),
            ]), True), True),
            StructField("selectedAgentId", StringType(), True),
            StructField("sessionDnis", StringType(), True),
            StructField("sessionId", StringType(), True),
            StructField("usedRouting", StringType(), True),
        ]), True), True)
    ]), True), True),
    StructField("userId", StringType(), True)
])

# COMMAND ----------

@dlt.table(
    name = "bronze_conversation_job",
    comment = "Raw data from conversation job"
)
def bronze_conversation_job():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(source_bucket + "/CONVERSATION_JOB/*/*/*/*")
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
        )
    )

dlt.create_streaming_table("stg_conversation_job")

dlt.apply_changes(
  target = "stg_conversation_job",
  source = "bronze_conversation_job",
  keys = ["conversationId"],
  sequence_by = F.col("file_modification_time"),
  stored_as_scd_type = "2"
)
