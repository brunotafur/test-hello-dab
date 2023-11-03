# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import dlt

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Variables
metrics_col_oldname = [
    "nConnected",
    "tIvr",
    "nOffered",
    "tAcd",
    "tWait",
    "nOverSla",
    "tAnswered",
    "tAbandoned",
    "tShortAbandon",
    "tAgentResponseTime",
    "tAlert",
    "tTalk",
    "tHeld",
    "tAcw",
    "tHandle",
    "tDialing",
    "tContacting",
    "tMonitoring",
    "tNotResponding",
    "tUserResponseTime",
    "tVoicemail",
    "nTransferred",
    "nBlindTransferred",
    "nConsultTransferred",
    "nCobrowseSessions",
    "nConsult",
]


metrics_col_newname = [
    "Connected",
    "IVR_Time",
    "Offered",
    "ACD_Time",
    "Wait_Time",
    "Over_SLA",
    "Answered_Time",
    "Abandon_Time",
    "Short_Abandon_Time",
    "Agent_Response_Time",
    "Alert_Time",
    "Talk_Time",
    "Held_Time",
    "ACW_Time",
    "Handle_Time",
    "Dialing_Time",
    "Contacting_Time",
    "Monitoring_Time",
    "Not_Responding_Time",
    "User_Response_Time",
    "Voice_Mail_Time",
    "Transferred",
    "Transferred_Blind",
    "Transferred_Consult",
    "CoBrowse",
    "Consult",
]

# COMMAND ----------

# DBTITLE 1,Utility Functions
def metricsExtract(df: DataFrame, col_old: list, col_new: list) -> DataFrame:
    """
    Function expands the metrics names with its values
    """
    for i in range(len(col_old)):
            df = df.withColumn(col_new[i], F.when(F.col("name") == col_old[i], F.col("value")).otherwise(None))
    return df

def flatten(df):
   """
   Function flatten the complex data, which explodes the Arrays and Expand all Structs schema
   
   Paramater: Dataframe to be exploded
   eg: flatten(bronze_conversation_jobs)
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

# DBTITLE 1,Participant Attributes
def participantAttributesExtract(participantAttributesDF):
    PA = flatten(participantAttributesDF)
    PA = (
        PA.withColumnRenamed("participantId", "participant_Id")
        .withColumnRenamed("CountryCode", "Country_Code")
        .withColumnRenamed("ClientBrand", "Client_Brand")
        .withColumnRenamed("CallType", "Call_Type")
        .withColumnRenamed("CommercialType", "Commercial_Type")
        .withColumnRenamed("PlanningUnit", "Planning_Unit")
    )

    return (
        PA.select(
            "LegId",
            "conversationId",
            "Country_Code",
            "Client_Brand",
            "Commercial_Type",
            "Call_Type",
            "Planning_Unit",
        )
        .filter(F.col("LegId").isNotNull())
        .distinct()
    )

# COMMAND ----------

# DBTITLE 1,silver_conv_snapshot
def silver_conv_snapshot(df):
    cols_to_drop_after_explode = [
        "file_path",
        "file_name",
        "file_size",
        "file_modification_time",
        "mediaStatsMinConversationMos",
        "externalContactId",
        "mediaStatsMinConversationRFactor",
        "__START_AT",
        "__END_AT",
        "divisionIds",
        "processing_time",
        "source_file",
        "participantName",
        "SMSOOHMobileAudio",
        "SMSLandlineMxg",
        "custCLI",
        "dialerContactListId",
        "Skill",
        "dialerContactListId",
        "TIQCheck",
        "ScheduleGroup",
        "FoundQueueName",
        "HoldMusic",
        "FoundSkillName",
        "scriptId",
        "SMSDeflectionOffer",
        "ScreenPopName",
        "SMSOptInPrompt",
        "CBEWTSetting",
        "dialerContactId",
        "dialerInteractionId",
        "dialerCampaignId",
        "Log_LegWorkflowStart",
        "Location",
        "LandlineNoInptHD",
        "SMSOOHNoMobileAudio",
        "Whisper",
        "NoMobOffPromptSD",
        "TIQValue",
        "peerId",
        "protocolCallId",
        "provider",
        "recording",
        "recording",
        "remote",
        "remoteNameDisplayable",
        "usedRouting",
        "endingLanguage",
        "entryReason",
        "entryType",
        "exitReason",
        "flowId",
        "flowName",
        "flowType",
        "flowVersion",
        "startingLanguage",
        "codecs",
        "discardedPackets",
        "eventTime",
        "maxLatencyMs",
        "minMos",
        "minRFactor",
        "receivedPackets",
        "emitDate",
        "sipResponseCodes",
        "wrapUpCode",
        "DNIS"
    ]
    final_col_to_return = [
        "Conversation_Id",
        "Leg_Id",
        "primary_division_id",
        "participantId",
        "Queue",
        "Skill",
        "originatingDirection",
        "Transfer_Leg",
        "conversationStart",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "Conversation_Date",
        "Conversation_End_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "Leg_Start_Time",
        "Leg_Start_Key",
        "Leg_End_Time",
        "Leg_End_Key",
        "ANI",
        "DDI",
        "NTLOGIN",
        "Connected",
        "IVR_Time",
        "Offered",
        "ACD_Time",
        "Wait_Time",
        "Over_SLA",
        "Answered_Time",
        "Abandon_Time",
        "Short_Abandon_Time",
        "Agent_Response_Time",
        "Alert_Time",
        "Talk_Time",
        "Held_Time",
        "destinationAddresses",
        "ACW_Time",
        "Handle_Time",
        "Dialing_Time",
        "Contacting_Time",
        "Monitoring_Time",
        "Not_Responding_Time",
        "User_Response_Time",
        "Voice_Mail_Time",
        "Disconnect_Type",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Conference",
        "CoBrowse",
        "Consult",
        "SurveyConversationId",
        "SurveyStartTime",
        "AgentNTLogin",
        "CustomerANI",
        "SurveyCSATScore",
        "SurveyFCRScore",
        "ComfortMsg1",
        "DNIS",
        "sessionDnis",
        "Media_Type",
        "Purpose",
        "sessionId",
        "segmentStart",
        "segmentEnd",
        "requestedRoutings",
        "selectedAgentId",
        "direction"
    ]
    data_with_primary_division = (df.withColumn("primary_division_id", F.col("divisionIds")[0]))
    
    conversation_job_flattened = (
        flatten(data_with_primary_division)
        .withColumnRenamed("ivr.Priority", "ivr_Priority")
        .withColumnRenamed("ivr.Skills", "ivr_Skills")
        .drop(*cols_to_drop_after_explode)
    )

    Conversation_jobs = (
        conversation_job_flattened.withColumn(
            "conversationStart",
            F.to_timestamp(F.col("conversationStart"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn(
            "conversationEnd",
            F.to_timestamp(F.col("conversationEnd"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumnRenamed("conversationId", "Conversation_Id")
        .withColumnRenamed("NTLogin", "NTLOGIN")
        .withColumn("Conversation_Date", F.to_date(F.col("conversationStart")))
        .withColumn("Conversation_End_Date", F.to_date(F.col("conversationEnd")))
        .withColumn(
            "Conversation_Date_Key",
            F.concat_ws(
                "",
                F.date_format(F.col("conversationStart"), "yyyy"),
                F.date_format(F.col("conversationStart"), "MM"),
                F.date_format(F.col("conversationStart"), "dd"),
            ).cast("integer"),
        )
        .withColumn(
            "Conversation_Start_Time",
            F.date_format(F.col("conversationStart"), "HH:mm:ss:SSS"),
        )
        .withColumn(
            "Conversation_Start_Key",
            F.concat_ws(
                "",
                F.date_format(F.col("conversationStart"), "HH"),
                F.date_format(F.col("conversationStart"), "mm"),
                F.date_format(F.col("conversationStart"), "ss"),
                F.date_format(F.col("conversationStart"), "SSS"),
            ).cast("integer"),
        )
        .withColumn(
            "Conversation_End_Time",
            F.date_format(F.col("conversationEnd"), "HH:mm:ss:SSS"),
        )
        .withColumn(
            "Conversation_End_Key",
            F.concat_ws(
                "",
                F.date_format(F.col("conversationEnd"), "HH"),
                F.date_format(F.col("conversationEnd"), "mm"),
                F.date_format(F.col("conversationEnd"), "ss"),
                F.date_format(F.col("conversationEnd"), "SSS"),
            ).cast("integer")
        )
        .withColumnRenamed("LegId", "Leg_Id")
        .withColumn(
            "Leg_Ordinal_value", F.substring(F.col("Leg_Id"), -1, 1)
        )  # Transfer_Leg
        .withColumn(
            "Leg_Ordinal",
            F.when(
                F.col("Leg_Ordinal_value").cast("int").isNotNull(),
                F.col("Leg_Ordinal_value").cast("int"),
            ).otherwise(None),
        )
        .withColumn(
            "Alternate_Leg_Flag",
            F.when(
                F.col("Leg_Ordinal_value").cast("int").isNull(),
                F.col("Leg_Ordinal_value"),
            ).otherwise(None),
        )
        .withColumn(
            "Transfer_Leg", F.when(F.col("Leg_Ordinal") > 1, True).otherwise(False)
        )
        .withColumn(
            "Leg_Start_Time", F.date_format(F.col("segmentStart"), "HH:mm:ss:SSS")
        )
        .withColumn("Leg_End_Time", F.date_format(F.col("segmentEnd"), "HH:mm:ss:SSS"))
        .withColumn(
            "Leg_Start_Key",
            F.concat_ws(
                "",
                F.date_format(F.col("segmentStart"), "HH"),
                F.date_format(F.col("segmentStart"), "mm"),
                F.date_format(F.col("segmentStart"), "ss"),
                F.date_format(F.col("segmentStart"), "SSS"),
            ).cast("integer")
        )
        .withColumn(
            "Leg_End_Key",
            F.concat_ws(
                "",
                F.date_format(F.col("segmentStart"), "HH"),
                F.date_format(F.col("segmentStart"), "mm"),
                F.date_format(F.col("segmentStart"), "ss"),
                F.date_format(F.col("segmentStart"), "SSS"),
            ).cast("integer")
        )
        .withColumn("ANI", F.when(F.col("ani").startswith("tel:+"),
                                  F.regexp_replace(F.col("ani"), "[^0-9]", "").cast("long")).otherwise(None))
        .withColumn("DDI", F.when(F.col("DDI").startswith("tel:+"),
                                  F.regexp_replace(F.col("DDI"), "[^0-9]", "").cast("long")).otherwise(None))
        .withColumn("DNIS", F.when(F.col("dnis").startswith("tel:+"),
                                  F.regexp_replace(F.col("dnis"), "[^0-9]", "").cast("long")).otherwise(None))
        .withColumnRenamed("disconnectType", "Disconnect_Type")
        .withColumnRenamed("conference", "Conference")
        .withColumnRenamed("mediaType", "Media_Type")
        .withColumnRenamed("purpose", "Purpose")
        .withColumnRenamed("queueId", "Queue")
        .withColumnRenamed("requestedRoutingSkillIds", "Skill")
    )

    conversation_job_metrics = metricsExtract(Conversation_jobs, metrics_col_oldname, metrics_col_newname)

    return conversation_job_metrics.select(*final_col_to_return).distinct()

# COMMAND ----------

# DBTITLE 1,Inbound - Outbound Conversation Legs
def inboundOutboundLegs(
    conversation: DataFrame,
    partition_attributes: DataFrame,
    divisions: DataFrame,
    routing_queues: DataFrame,
    routing_skills: DataFrame,
    direction: str,
    column_list : list
):
    divisions = (divisions.select("id", "name", F.substring(F.col("name"), 1, 2).alias("Timezone")).distinct())
    conversation = (conversation.filter((F.col("originatingDirection") == direction) & 
                        (F.col("Media_Type") != "callback") & 
                        (F.col("Leg_Id").isNotNull())
                        )) 
    r_queue = (routing_queues.select(F.col("id").alias("Queue_ID"), F.col("name").alias("Queue_Name")).distinct())
    r_skills = (routing_skills.select(F.col("id").alias("Skill_ID"), F.col("name").alias("Skill_Name")).distinct()) 
    p_attributes = (participantAttributesExtract(partition_attributes).withColumnRenamed("LegId", "legid"))

    ani_window_spec = Window.partitionBy("ANI").orderBy("conversationStart")
    ddi_window_spec = Window.partitionBy("DDI").orderBy("conversationStart")


    conversation_leg = (conversation
                    .join(divisions, conversation.primary_division_id == divisions.id, "left")
                    .join(r_queue, conversation.Queue == r_queue.Queue_ID, "left")
                    .join(r_skills, conversation.Skill == r_skills.Skill_ID, "left")
                    .drop("Queue", "Skill")
                    )
    
    conversation_details = (
    conversation_leg.select(
        "Conversation_Id",
        "Leg_Id",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "conversationStart",
        "Conversation_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "ANI",
        "DNIS",
        "DDI",
        "AgentNTLogin",
        "Timezone"
        )
    .withColumnRenamed("Leg_Id", "LegId")
    .distinct()
    )

    leg_details = (
    conversation_leg.select(
        F.col("Conversation_Id").alias("ConversationId"),
        "Leg_Id",
        "Leg_Start_Time",
        "Leg_Start_Key",
        "Leg_End_Time",
        "Leg_End_Key",
        "Dialing_Time",
        "Contacting_Time",
        "Talk_Time",
        "Held_Time",
        "ACW_Time",
        "Handle_Time",
        "Monitoring_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Conference",
        "CoBrowse",
        "Consult",
        "Transferred_Consult",
        "Disconnect_Type",
        "Connected",
        "Answered_Time",
        "Wait_Time",
        "Short_Abandon_Time",
        "Agent_Response_Time",
        "Not_Responding_Time",
        "User_Response_Time",
        "Abandon_Time",
        "IVR_Time",
        "ACD_Time",
        "Over_SLA",
        "Offered",
        "Alert_Time",
        "Disconnect_Type",
        "Queue_Name",
        "Skill_Name",
        "NTLOGIN",
        "destinationAddresses"
    )
    .fillna(
        0,
        subset=[
            "Dialing_Time",
            "Contacting_Time",
            "Talk_Time",
            "Held_Time",
            "ACW_Time",
            "Handle_Time",
            "Monitoring_Time",
            "Voice_Mail_Time",
            "Transferred",
            "Transferred_Blind",
            "CoBrowse",
            "Consult",
            "Transferred_Consult",
            "Connected",
            "Answered_Time",
            "Wait_Time",
            "Short_Abandon_Time",
            "Agent_Response_Time",
            "Not_Responding_Time",
            "User_Response_Time",
            "Abandon_Time",
            "ACD_Time",
            "Over_SLA",
            "Offered",
            "Alert_Time"
        ],
    )
    .distinct())

    leg_details_agg = leg_details.groupBy("Leg_Id", "ConversationId").agg(
        F.min("Leg_Start_Time").alias("Leg_Start_Time"),
        F.min("Leg_Start_Key").alias("Leg_Start_Key"),
        F.max("Leg_End_Time").alias("Leg_End_Time"),
        F.max("Leg_End_Key").alias("Leg_End_Key"),
        F.sum("Dialing_Time").alias("Dialing_Time"),
        F.sum("Contacting_Time").alias("Contacting_Time"),
        F.sum("Talk_Time").alias("Talk_Time"),
        F.sum("ACW_Time").alias("ACW_Time"),
        F.sum("Handle_Time").alias("Handle_Time"),
        F.sum("Monitoring_Time").alias("Monitoring_Time"),
        F.sum("Voice_Mail_Time").alias("Voice_Mail_Time"),
        F.sum("Held_Time").alias("Held_Time"),
        F.max("Transferred").alias("Transferred"),
        F.max("Transferred_Blind").alias("Transferred_Blind"),
        F.max("Conference").alias("Conference"),
        F.max("CoBrowse").alias("CoBrowse"),
        F.max("Consult").alias("Consult"),
        F.max("Transferred_Consult").alias("Transferred_Consult"),
        F.max("Connected").alias("Connected"),
        F.sum("Answered_Time").alias("Answered_Time"),
        F.sum("Abandon_Time").alias("Abandon_Time"),
        F.sum("Short_Abandon_Time").alias("Short_Abandon_Time"),
        F.sum("Agent_Response_Time").alias("Agent_Response_Time"),
        F.sum("Not_Responding_Time").alias("Not_Responding_Time"),
        F.sum("User_Response_Time").alias("User_Response_Time"),
        F.sum("IVR_Time").alias("IVR_Time"),
        F.last("Disconnect_Type").alias("Disconnect_Type"),
        F.sum("Wait_Time").alias("Wait_Time"),
        F.sum("ACD_Time").alias("ACD_Time"),
        F.max("Offered").alias("Offered"),
        F.max("Over_SLA").alias("Over_SLA"),
        F.sum("Alert_Time").alias("Alert_Time"),
        F.last("Queue_Name").alias("Queue"),
        F.last("Skill_Name").alias("Skill"),
        F.last("NTLOGIN").alias("NTLOGIN"),
        F.last("destinationAddresses").alias("destinationAddresses")
    )
    if direction == "inbound":

        final = (conversation_details.join(leg_details_agg, 
                                        conversation_details.LegId == leg_details_agg.Leg_Id, "left")
                .join(p_attributes, conversation_details.LegId == p_attributes.legid, "left")
        .withColumn(
            "Transfer_Queue",
            F.when(F.col("Queue").isNotNull(), F.col("Queue")).otherwise(None),
        )
        .withColumn(
            "Transfer_DDI",
            F.when(
                F.col("destinationAddresses").cast("double").isNotNull(),
                F.col("destinationAddresses").cast("double"),
            ).otherwise(None),
        )
        .withColumn(
            "Transfer_Agent_NTLOGIN",
            F.when(
                F.col("destinationAddresses").rlike("[a-zA-Z]"), F.col("AgentNTLogin")
            ).otherwise(None),
        )
        .withColumn(
            "Handled",
            F.when(
                (F.col("Answered_Time").isNull()) | (F.col("Answered_Time") == 0), 0
            ).otherwise(1),
        )
        .withColumn(
            "Abandoned",
            F.when((F.col("Abandon_Time").isNull()) | (F.col("Abandon_Time") == 0), 0)
            .otherwise(1))
            .withColumn("Customer_Contact_Sequence", (F.when(F.col("ANI").isNotNull(), F.row_number().over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.row_number().over(ddi_window_spec)).otherwise(None)))
                        )
            .withColumn("Previous_Contact_DateTime", (F.when(F.col("ANI").isNotNull(), F.lag("conversationStart", 1).over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.lag("conversationStart", 1).over(ddi_window_spec)
                                                                        ).otherwise(None)))
            )
            .withColumn(
                "Delta_Contact_DateTime",
                F.col("conversationStart").cast("long")
                - F.col("Previous_Contact_DateTime").cast("long")
            )
            .withColumn("Callback_Request", F.lit("null"))
            .withColumn("Callback_Handled", F.lit("null"))
            .withColumn("Callback_No_Answer", F.lit("null"))
            .withColumn("Callback_Abandoned", F.lit("null"))
            .withColumn("Pre_Request_Wait_Time", F.lit("null"))
            .withColumn("Callback_Wait_Time", F.lit("null"))
        )
        return final.select(*column_list)
    
    if direction == "outbound":

        final = (conversation_details
                .join(leg_details_agg, conversation_details.LegId == leg_details_agg.Leg_Id, "left")
                .join(p_attributes, conversation_details.LegId == p_attributes.legid, "left")
        .withColumn(
            "Transfer_Queue",
            F.when(F.col("Queue").isNotNull(), F.col("Queue")).otherwise(None),
        )
        .withColumn(
            "Transfer_DDI",
            F.when(
                F.col("destinationAddresses").cast("double").isNotNull(),
                F.col("destinationAddresses").cast("double"),
            ).otherwise(None),
        )
        .withColumn(
            "Transfer_Agent_NTLOGIN",
            F.when(
                F.col("destinationAddresses").rlike("[a-zA-Z]"), F.col("AgentNTLogin")
            ).otherwise(None),
        )
        .withColumn(
            "Handled",
            F.when(
                (F.col("Answered_Time").isNull()) | (F.col("Answered_Time") == 0), 0
            ).otherwise(1),
        )
        .withColumn(
            "Abandoned",
            F.when((F.col("Abandon_Time").isNull()) | (F.col("Abandon_Time") == 0), 0)
            .otherwise(1))
            .withColumn("Customer_Contact_Sequence", (F.when(F.col("ANI").isNotNull(), F.row_number().over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.row_number().over(ddi_window_spec)).otherwise(None)))
                        )
            .withColumn("Previous_Contact_DateTime", (F.when(F.col("ANI").isNotNull(), F.lag("conversationStart", 1).over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.lag("conversationStart", 1).over(ddi_window_spec)
                                                                        ).otherwise(None)))
                        )
            .withColumn(
                "Delta_Contact_DateTime",
                F.col("conversationStart").cast("long")
                - F.col("Previous_Contact_DateTime").cast("long")
            )
            .withColumn("ACD_OB_Attempt", F.lit(1))
            .withColumn(
            "ACD_OB_Connected", F.when(F.col("Talk_Time").isNotNull(), 1).otherwise(0)
        )
        )

        return final.select(*column_list)

# COMMAND ----------

# DBTITLE 1,Silver Callback Conversations
def callBackConversations(conversation_leg, participant_attribute, queues, skills, user):

    column_list = [
    "Conversation_Id",
    "Conversation_Date",
    "Conversation_Date_Key",
    "Conversation_Start_Time",
    "Conversation_Start_Key",
    "Conversation_End_Time",
    "Conversation_End_Key",
    "Callback_Date",
    "Callback_Date_Key",
    "Callback_Time",
    "Callback_Time_Key",
    "ANI",
    "DNIS",
    "Country_Code",
    "Client_Brand",
    "Commercial_Type",
    "Call_Type",
    "Planning_Unit",
    "Queue",
    "Skill",
    "NTLOGIN",
    "Pre_Request_IVR_Time",
    "Pre_Request_ACD_Time",
    "Callback_Attempt_Wait_Time",
    "Handled",
    "No_Answer",
    "Abandoned",
    "Dialing_Time",
    "Contacting_Time",
    "Alert_Time",
    "Talk_Time",
    "Held_Time",
    "ACW_Time",
    "Handle_Time",
    "Disconnect_Type"
]
    window_spec = Window.partitionBy("Conversation_Id").orderBy("segmentStart")

    call_back_sessions = (
        conversation_leg
        .filter(F.col("Media_Type") == "callback")
        .select(
            "Conversation_Id",
            "Conversation_Date",
            "Conversation_Date_Key",
            "Conversation_Start_Time",
            "Conversation_Start_Key",
            "Conversation_End_Time",
            "Conversation_End_Key"
        )
        .distinct()
    )

    callback_datetime = conversation_leg.withColumn(
    "last_media_type", F.lag("Media_Type").over(window_spec)
    ).select(
    "Conversation_Id",
    "Purpose",
    "Media_Type",
    "direction",
    "last_media_type",
    "segmentStart"
    ).distinct()

    callback_datetime = (callback_datetime
                         .filter((F.col("Purpose") == "agent") &
             (F.col("Media_Type") == "voice") &
             (F.col("last_media_type") == "callback") &
             (F.col("direction") == "outbound")
             )
                         .withColumnRenamed("Conversation_Id", "callback_conv_id")
                        .withColumn("Callback_Date", F.to_date(F.col("segmentStart")))
                        .withColumn("Callback_Date_Key",F.regexp_replace(F.col("Callback_Date"), "-", "").cast("long"))
                        .withColumn("Callback_Time", F.date_format(F.col("segmentStart"), "HH:mm:ss:SSS"))
                        .withColumn("Callback_Time_Key",F.regexp_replace(F.col("Callback_Time"), ":", "").cast("long"))
                        .select("callback_conv_id", "Callback_Date", "Callback_Date_Key", "Callback_Time", "Callback_Time_Key")
                        .distinct()
     )

    queue_skill_details = conversation_leg.select(
        "Conversation_Id", "Queue", "Skill"
    ).distinct()

    queue_skill = (
        queue_skill_details.join(queues, queue_skill_details.Queue == queues.id, "left")
        .join(skills, queue_skill_details.Skill == skills.skillId, "Left")
        .select(F.col("Conversation_Id").alias("conv_id"), "queue_name", "skill_name")
        .distinct()
    )

    pa_data = (
        participantAttributesExtract(participant_attribute)
        .withColumnRenamed("conversationId", "conversationid")
        .drop("LegId")
        .distinct()
    )

    callback_metrics = (
        conversation_leg.select(
            "Conversation_Id",
            "IVR_Time",
            "ACD_Time",
            "Dialing_Time",
            "Contacting_Time",
            "Alert_Time",
            "Talk_Time",
            "Held_Time",
            "ACW_Time",
            "Handle_Time",
            "NTLOGIN",
            "Disconnect_Type",
            "Abandon_Time",
            "Answered_Time",
            "ANI",
            "DNIS"
        )
        .fillna(
            0,
            subset=[
                "IVR_Time",
                "ACD_Time",
                "Dialing_Time",
                "Contacting_Time",
                "Alert_Time",
                "Talk_Time",
                "Held_Time",
                "ACW_Time",
                "Handle_Time",
                "Abandon_Time",
                "Answered_Time",
            ],
        )
        .distinct()
    )

    conversation_metricks_callback = (
        callback_metrics.groupBy("Conversation_Id", "NTLOGIN", "Disconnect_Type")
        .agg(
            F.sum(F.col("IVR_Time")).alias("IVR_Time"),
            F.sum(F.col("ACD_Time")).alias("ACD_Time"),
            F.sum(F.col("Dialing_Time")).alias("Dialing_Time"),
            F.sum(F.col("Contacting_Time")).alias("Contacting_Time"),
            F.sum(F.col("Alert_Time")).alias("Alert_Time"),
            F.sum(F.col("Talk_Time")).alias("Talk_Time"),
            F.sum(F.col("Held_Time")).alias("Held_Time"),
            F.sum(F.col("ACW_Time")).alias("ACW_Time"),
            F.sum(F.col("Handle_Time")).alias("Handle_Time"),
            F.sum(F.col("Answered_Time")).alias("Answered_Time"),
            F.sum(F.col("Abandon_Time")).alias("Abandon_Time"),
            F.max(F.col("ANI")).alias("ANI"),
            F.max(F.col("DNIS")).alias("DNIS")
        )
        .withColumnRenamed("Conversation_Id", "ConversationId")
    )

    final_callback = (
        call_back_sessions.join(
            conversation_metricks_callback,
            call_back_sessions.Conversation_Id
            == conversation_metricks_callback.ConversationId,
            "left",
        )
        .join(
            pa_data,
            call_back_sessions.Conversation_Id == pa_data.conversationid,
            "left",
        )
        .join(
            queue_skill,
            call_back_sessions.Conversation_Id == queue_skill.conv_id,
            "left",
        )
        .join(
            callback_datetime,
            call_back_sessions.Conversation_Id == callback_datetime.callback_conv_id,
            "inner"
        )
    )

    callback_data = (final_callback
        .withColumnRenamed("queue_name", "Queue")
        .withColumnRenamed("skill_name", "Skill")
        .withColumnRenamed("IVR_Time","Pre_Request_IVR_Time")
        .withColumnRenamed("ACD_Time","Pre_Request_ACD_Time")
        .withColumn("Callback_Attempt_Wait_Time", F.col("Pre_Request_ACD_Time") - F.col("Pre_Request_IVR_Time"))
        .withColumn("Handled",F.when((F.col("Answered_Time").isNull())| (F.col("Answered_Time") == 0),0).otherwise(1))
        .withColumn("Abandoned",F.when((F.col("Abandon_Time").isNull())| (F.col("Abandon_Time") == 0),0).otherwise(1))
        .withColumn("No_Answer",F.when((F.col("Talk_Time").isNull())| (F.col("Talk_Time") == 0),1).otherwise(0))
        .select(*column_list)
        .distinct()
        )
    
    return callback_data

# COMMAND ----------

# DBTITLE 1,Silver Conversation Participant Attributes
def silverConversationParticipantAttributes(df):

    # try:
    pa = (
    (
    df.withColumnRenamed("conversationId", "Conversation_Id").withColumn(
    "participant_data_extract", F.explode(F.col("participantData"))
    )
    )
    .select(
    "Conversation_Id", "participant_data_extract.participantAttributes.*"
    )
    .withColumnRenamed("LegId", "Leg_Id")
    .withColumnRenamed("ivr.Skills", "ivr_Skills")
    .withColumnRenamed("ivr.Priority", "ivr_Priority")
    .filter(F.col("Leg_Id").isNotNull())
    )

    participant_attributes = pa.groupBy("Conversation_Id", "Leg_Id").agg(
    F.max("CustomerANI").alias("CustomerANI"),
    F.max("CallType").alias("CallType"),
    F.max("CountryCode").alias("CountryCode"),
    F.max("ivr_Priority").alias("ivr_Priority"),
    F.max("IVRSpare4").alias("IVRSpare4"),
    F.max("Deflection").alias("Deflection"),
    F.max("SMSMsgMobileIVR").alias("SMSMsgMobileIVR"),
    F.max("IVRSpare2").alias("IVRSpare2"),
    F.max("FoundQueueName").alias("FoundQueueName"),
    F.max("Log").alias("Log"),
    F.max("ClientBrand").alias("ClientBrand"),
    F.max("Skill").alias("Skill"),
    F.max("custCLI").alias("custCLI"),
    F.max("ivr_Skills").alias("ivr_Skills"),
    F.max("Whisper").alias("Whisper"),
    F.max("DNIS").alias("DNIS"),
    F.max("GDPR").alias("GDPR"),
    F.max("Menu").alias("Menu"),
    F.max("SMSNoMobMsgIVR").alias("SMSNoMobMsgIVR"),
    F.max("ComfortMsg1").alias("ComfortMsg1"),
    F.max("IVRSpare3").alias("IVRSpare3"),
    F.max("ScheduleGroup").alias("ScheduleGroup"),
    F.max("Log_LegWorkflowComplete").alias("Log_LegWorkflowComplete"),
    F.max("PlanningUnit").alias("PlanningUnit"),
    F.max("Priority").alias("Priority"),
    F.max("HoldMusic").alias("HoldMusic"),
    F.max("FoundSkillName").alias("FoundSkillName"),
    F.max("Log_ConversationCheck").alias("Log_ConversationCheck"),
    F.max("SMSLandlineMxg").alias("SMSLandlineMxg"),
    F.max("SMSOptInPrompt").alias("SMSOptInPrompt"),
    F.max("CBEWTSetting").alias("CBEWTSetting"),
    F.max("DDI").alias("DDI"),
    F.max("TIQValue").alias("TIQValue"),
    F.max("IVRSpare1").alias("IVRSpare1"),
    F.max("CommercialType").alias("CommercialType"),
    F.max("TIQCheck").alias("TIQCheck"),
    F.max("NoMobOffPromptSD").alias("NoMobOffPromptSD"),
    F.max("varEndpointName").alias("varEndpointName"),
    F.max("scriptId").alias("scriptId"),
    F.min("Log_LegWorkflowStart").alias("Log_LegWorkflowStart"),
    F.max("ScreenPopName").alias("ScreenPopName"),
    F.max("VDN").alias("VDN"),
    F.max("NoSurveyOptIn").alias("NoSurveyOptIn"),
    )
    
    # list of participant attribute to be pivoted
    pivot_columns = [col_name for col_name in participant_attributes.columns if col_name not in ("Conversation_Id", "Leg_Id")]  
    stack_list = [f"'{col_name}', {col_name}," for col_name in pivot_columns]  # string to pass to stack expr
    stack_string = "".join(stack_list)[:-1]

    participant_attributes_pivot = participant_attributes.selectExpr(
    "Conversation_Id",
    "Leg_Id",
    f"stack({len(pivot_columns)}, {stack_string}) as (attribute_name, attribute_value)",
    )
    return participant_attributes_pivot

# COMMAND ----------

@dlt.table(comment="Pipeline - silver_conversation_snapshot")
def silver_conversation_snapshot():

    staging_conv_jobs = dlt.read("stg_conversation_job")
    stg = (staging_conv_jobs.filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull())))
           .select("*",
                   F.current_timestamp().alias("Last_Modified")))

    return silver_conv_snapshot(stg)

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver_Outbound_Conversation_Leg")
def silver_inbound_conversation_leg():

    column_list = [
    "Conversation_Id",
    "Leg_Id",
    "Transfer_Leg",
    "Leg_Ordinal",
    "Alternate_Leg_Flag",
    "Timezone",
    "Conversation_Date",
    "Conversation_Date_Key",
    "Conversation_Start_Time",
    "Conversation_Start_Key",
    "Conversation_End_Time",
    "Conversation_End_Key",
    "Leg_Start_Time",
    "Leg_Start_Key",
    "Leg_End_Time",
    "Leg_End_Key",
    "ANI",
    "Customer_Contact_Sequence",
    "Previous_Contact_DateTime",
    "Delta_Contact_DateTime",
    "DNIS",
    "Country_Code",
    "Client_Brand",
    "Commercial_Type",
    "Call_Type",
    "Planning_Unit",
    "Queue",
    "Skill",
    "NTLOGIN",
    "Connected",
    "IVR_Time",
    "Offered",
    "ACD_Time",
    "Wait_Time",
    "Over_SLA",
    "Handled",
    "Answered_Time",
    "Abandoned",
    "Abandon_Time",
    "Short_Abandon_Time",
    "Agent_Response_Time",
    "Alert_Time",
    "Talk_Time",
    "Held_Time",
    "ACW_Time",
    "Handle_Time",
    "Dialing_Time",
    "Contacting_Time",
    "Monitoring_Time",
    "Not_Responding_Time",
    "User_Response_Time",
    "Voice_Mail_Time",
    "Disconnect_Type",
    "Transferred",
    "Transferred_Blind",
    "Transferred_Consult",
    "Transfer_Queue",
    "Transfer_DDI",
    "Transfer_Agent_NTLOGIN",
    "Conference",
    "CoBrowse",
    "Consult",
    "Callback_Request",
    "Callback_Handled",
    "Callback_No_Answer",
    "Callback_Abandoned",
    "Pre_Request_Wait_Time",
    "Callback_Wait_Time"]

    divisions = dlt.read("stg_silver_divisions")
    conversation = dlt.read("silver_conversation_snapshot")
    r_queue = dlt.read("stg_silver_routing_queues")
    r_skill = dlt.read("stg_silver_routing_skills")
    participant_attributes = dlt.read("stg_silver_participant_attributes")

    return inboundOutboundLegs(
        conversation = conversation,
        partition_attributes = participant_attributes,
        divisions = divisions,
        routing_queues = r_queue,
        routing_skills = r_skill,
        direction = "inbound",
        column_list = column_list
        ).select("*",F.current_timestamp().alias("Last_Modified"))

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver_Outbound_Conversation_Leg")
def silver_outbound_conversation_leg():

    column_list = [
        "Conversation_Id",
        "Leg_Id",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "Timezone",
        "Conversation_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "Leg_Start_Time",
        "Leg_Start_Key",
        "Leg_End_Time",
        "Leg_End_Key",
        "DDI",
        "Customer_Contact_Sequence",
        "Previous_Contact_DateTime",
        "Delta_Contact_DateTime",
        "NTLOGIN",
        "Disconnect_Type",
        "Dialing_Time",
        "Contacting_Time",
        "ACD_OB_Attempt",
        "ACD_OB_Connected",
        "Talk_Time",
        "Held_Time",
        "ACW_Time",
        "Handle_Time",
        "Monitoring_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Transfer_Queue",
        "Transfer_DDI",
        "Transfer_Agent_NTLOGIN",
        "Conference",
        "CoBrowse",
    ]
    divisions = dlt.read("stg_silver_divisions")
    conversation = dlt.read("silver_conversation_snapshot")
    r_queue = dlt.read("stg_silver_routing_queues")
    r_skill = dlt.read("stg_silver_routing_skills")
    participant_attributes = dlt.read("stg_silver_participant_attributes")

    return inboundOutboundLegs(
        conversation = conversation,
        partition_attributes = participant_attributes,
        divisions = divisions,
        routing_queues = r_queue,
        routing_skills = r_skill,
        direction = "outbound",
        column_list = column_list
        ).select("*",F.current_timestamp().alias("Last_Modified"))

# COMMAND ----------

@dlt.table(comment="Pipeline - silver_callback_conversation")
def silver_callback_conversation():

    call_back = dlt.read("silver_conversation_snapshot")
    
    participant_attribute = dlt.read("stg_silver_participant_attributes")
    
    user_details = dlt.read("stg_silver_user")
    
    queues = (dlt.read("stg_silver_routing_queues").withColumnRenamed("name", "queue_name"))
    
    skills = (dlt.read("stg_silver_routing_skills")
              .withColumnRenamed("id", "skillId")
              .withColumnRenamed("name", "skill_name"))
    
    callback_conversations = (callBackConversations(call_back, participant_attribute, queues, skills, user_details)
                              .select("*",F.current_timestamp().alias("Last_Modified")))


    return callback_conversations

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver CSAT")
def silver_csat():
    reqcols_csat = [
            "Conversation_Id",
            "Leg_Id",
            "SurveyStartTime",
            "NTLOGIN",
            "ANI",
            "SurveyCSATScore",
            "SurveyFCRScore"
        ]

    conversation_snapshot = dlt.read("silver_conversation_snapshot")
    conversation_snapshot = (conversation_snapshot
                            .select(*reqcols_csat)
                            .withColumn("LegId", 
                                        F.when(F.col("Leg_Id").isNull(),F.concat(F.col("Conversation_Id"), F.lit("-1")))
                                        .otherwise(F.col("Leg_Id")))
                            .withColumn("CSAT_Audit_Id", F.lit("Unknown"))
                            .withColumn("NPS_Score", F.lit("Unknown"))
                            .withColumn("Comment", F.lit("Unknown"))

                            )
    csat = (conversation_snapshot
                .groupBy("Conversation_Id","LegId")
                .agg(
                    F.max(F.col("CSAT_Audit_Id")).alias("CSAT_Audit_Id"),
                    F.max(F.col("SurveyStartTime")).alias("CSAT_Call_Date"),
                    F.max(F.col("NTLOGIN")).alias("NTLOGIN"),
                    F.max(F.col("ANI")).alias("ANI"),
                    F.max(F.col("SurveyCSATScore")).alias("CSAT_Score"),
                    F.max(F.col("SurveyFCRScore")).alias("FCR_Score"),
                    F.max(F.col("NPS_Score")).alias("NPS_Score"),
                    F.max(F.col("Comment")).alias("Comment")
                    )
                )

    return csat.select("*",F.current_timestamp().alias("Last_Modified"))

# COMMAND ----------

@dlt.table(comment="Pipeline - Conversation Participant Attributes")
def silver_conversation_participant_attributes():

    participant_atribute = dlt.read("stg_silver_participant_attributes")
    participant_atribute_final = silverConversationParticipantAttributes(participant_atribute)

    return (participant_atribute_final.select("*",F.current_timestamp().alias("Last_Modified")))
