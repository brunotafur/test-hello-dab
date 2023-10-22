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
                                    StructField("DDI", StringType(), True),
                                    StructField("DNIS", StringType(), True),
                                    StructField("GDPR", StringType(), True),
                                    StructField("Menu", StringType(), True),
                                    StructField("Skill", StringType(), True),
                                    StructField("Whisper", StringType(), True),
                                    StructField("custCLI", StringType(), True),
                                    StructField("CallType", StringType(), True),
                                    StructField("Priority", StringType(), True),
                                    StructField("TIQCheck", StringType(), True),
                                    StructField("TIQValue", StringType(), True),
                                    StructField("scriptId", StringType(), True),
                                    StructField("HoldMusic", StringType(), True),
                                    StructField("IVRSpare1", StringType(), True),
                                    StructField("IVRSpare2", StringType(), True),
                                    StructField("IVRSpare3", StringType(), True),
                                    StructField("IVRSpare4", StringType(), True),
                                    StructField("Deflection", StringType(), True),
                                    StructField("ClientBrand", StringType(), True),
                                    StructField("ComfortMsg1", StringType(), True),
                                    StructField("CountryCode", StringType(), True),
                                    StructField("CustomerANI", StringType(), True),
                                    StructField("CBEWTSetting", StringType(), True),
                                    StructField("PlanningUnit", StringType(), True),
                                    StructField("NoSurveyOptIn", StringType(), True),
                                    StructField("ScheduleGroup", StringType(), True),
                                    StructField("ScreenPopName", StringType(), True),
                                    StructField("CommercialType", StringType(), True),
                                    StructField("FoundQueueName", StringType(), True),
                                    StructField("FoundSkillName", StringType(), True),
                                    StructField("SMSLandlineMxg", StringType(), True),
                                    StructField("SMSNoMobMsgIVR", StringType(), True),
                                    StructField("SMSOptInPrompt", StringType(), True),
                                    StructField("SMSMsgMobileIVR", StringType(), True),
                                    StructField("varEndpointName", StringType(), True),
                                    StructField("NoMobOffPromptSD", StringType(), True),
                                    StructField("VDN", StringType(), True),
                                    StructField("LegId", StringType(), True),
                                    StructField("ivr.Skills", StringType(), True),
                                    StructField("ivr.Priority", StringType(), True),
                                    StructField(
                                        "Log_LegWorkflowStart", StringType(), True
                                    ),
                                    StructField(
                                        "Log_ConversationCheck", StringType(), True
                                    ),
                                    StructField(
                                        "Log_LegWorkflowComplete", StringType(), True
                                    ),
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
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(source_bucket + "/PARTICIPANT_ATTRIBUTES/*/*/*/*")
        .select("*","_metadata.file_name", "_metadata.file_path","_metadata.file_modification_time"))
