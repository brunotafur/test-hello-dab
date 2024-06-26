# Databricks notebook source
import os
import sys

path = os.getcwd()
cwd = "/".join(path.split("/")[:-1])#getting file path of the utility and pipeline imports
sys.path.append(cwd)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import dlt
# from utility import *
# from silver import *
# import utility
# import silver

# COMMAND ----------

def metricsExtract(df: DataFrame, col_old: list, col_new: list) -> DataFrame:
    """
    Function expands the metrics names with its values
    To be used extract metrics(Attribute name and corresponding values)

    eg: metricsExtract(df, old_columns, new_columns)
    df
    """
    for i in range(len(col_old)):
            df = df.withColumn(col_new[i], F.when(F.col("name") == col_old[i], F.col("value")).otherwise(None))
    return df

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
   
def replaceUnsupportedColumnNames(df):
    """
    JSON data have few columns with unsupported charectors like space, dots extra now renamed those columns
    eg: replaceUnsupportedColumnNames(df)
    'ivr.Skills' will be now renamed as 'ivrSkills'
    """
    data = df #saving fo the copy of df
    for col in df.columns:
        data = data.withColumnRenamed(col,col.replace(" ", "").replace(".",""))
    return data #return dataframe renamed columns
 
def ConvertToLocaltime(column_list, data):
    try:
        for column in column_list:
            print(f"Converting UTC to local time as per the {column} value from Timezone column")
            data = (data
                    .withColumn(column,F.to_timestamp(F.col(column), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
                    .withColumn(f"{column}", 
                        F.when(F.col("Timezone") == "UK",F.from_utc_timestamp(F.col(column), "Europe/London"))
                        .when(F.col("Timezone") == "PT",F.from_utc_timestamp(F.col(column), "Europe/Lisbon"))
                        .when(F.col("Timezone") == "NL",F.from_utc_timestamp(F.col(column), "Europe/Amsterdam"))
                        .when(F.col("Timezone") == "DE",F.from_utc_timestamp(F.col(column), "Europe/Berlin"))
                        .when(F.col("Timezone") == "ES",F.from_utc_timestamp(F.col(column), "Europe/Madrid"))
                        .when(F.col("Timezone") == "IT",F.from_utc_timestamp(F.col(column), "Europe/Rome"))
                        .when(F.col("Timezone") == "BE",F.from_utc_timestamp(F.col(column), "Europe/Brussels"))
                        .otherwise(F.col(column))))
    except:
        print("Issue in the convertion os the local time")
    return data

# COMMAND ----------

# to get the required data from participant attributes json data
# to get the required data from participant attributes json data
def participantAttributesExtract(df: DataFrame) -> DataFrame:
    """
    df - The staging silver participant attributes - return a dataframe with only required data
    eg: pa = participantAttributesExtract(df)
    return: dataframe
    """

    json_schema = StructType(
        [
            StructField("participantPurpose", StringType(), True),
            StructField(
                "participantAttributes",
                StructType(
                    [
                        StructField("Log", StringType(), True),
                        StructField("LegId", StringType(), True),
                        StructField("NTLogin", StringType(), True),
                        StructField("VDN", StringType(), True),
                        StructField("DNIS", StringType(), True),
                        StructField("Skill", StringType(), True),
                        StructField("custCLI", StringType(), True),
                        StructField("Schedule", StringType(), True),
                        StructField("Whisper", StringType(), True),
                        StructField("scriptId", StringType(), True),
                        StructField("CallType", StringType(), True),
                        StructField("CommercialType", StringType(), True),
                        StructField("ClientBrand", StringType(), True),
                        StructField("DNISName", StringType(), True),
                        StructField("Priority", StringType(), True),
                        StructField("Location", StringType(), True),
                        StructField("HoldMusic", StringType(), True),
                        StructField("HolidayMsg", StringType(), True),
                        StructField("WelcomeMsg", StringType(), True),
                        StructField("CustomerANI", StringType(), True),
                        StructField("QueueName", StringType(), True),
                        StructField("SalesType", StringType(), True),
                        StructField("TargetType", StringType(), True),
                        StructField("CountryCode", StringType(), True),
                        StructField("FlowStatus", StringType(), True),
                        StructField("LOG_QUEUEID", StringType(), True),
                        StructField("SubMenuName ", StringType(), True),
                        StructField("SurveyOptIn", StringType(), True),
                        StructField("NoSurveyOptIn", StringType(), True),
                        StructField("EmergencyMsg", StringType(), True),
                        StructField("ScheduleGroup", StringType(), True),
                        StructField("SurveyNTLogin", StringType(), True),
                        StructField("SurveyCriteria", StringType(), True),
                        StructField("SurveyTimeZone", StringType(), True),
                        StructField("SurveySim", StringType(), True),
                        StructField("BusinessStatus", StringType(), True),
                        StructField("ComfortMessage1", StringType(), True),
                        StructField("ComfortMessage2", StringType(), True),
                        StructField("SurveyContactListId", StringType(), True),
                        StructField("SurveyUpdateContastList", StringType(), True),
                        StructField("PlanningUnit", StringType(), True),
                        StructField("ScreenPopName", StringType(), True),
                        StructField("AlbaniaXferMsg", StringType(), True),
                        StructField("ClosedMessage", StringType(), True),
                        StructField("ScheduleGroup", StringType(), True),
                        StructField("DisconnectAudio", StringType(), True),
                        StructField("EmergencyGroup", StringType(), True),
                        StructField("HolidayMessage", StringType(), True),
                        StructField("MenuTargetValue ", StringType(), True),
                        StructField("SurveyWorkflow", StringType(), True),
                        StructField("Survey", StringType(), True),
                        StructField("WelcomeMessage", StringType(), True),
                        StructField("DeflectionStatus", StringType(), True),
                        StructField("DeflectionMessage", StringType(), True),
                        StructField("ExternalXferNumber ", StringType(), True),
                        StructField("Routing_Decision_DT", StringType(), True),
                        StructField("AlbaniaXferMessage", StringType(), True),
                        StructField("ExternalXferNumber", StringType(), True),
                        StructField("CallRecordingMessage", StringType(), True),
                        StructField("InformationMessage 2", StringType(), True),
                        StructField("InformationalMessage", StringType(), True),
                        StructField("EmergencyAnnouncement", StringType(), True),
                        StructField("TiranaEmergencyCheck", StringType(), True),
                        StructField("varRoutingDecisionName", StringType(), True),
                        StructField("QueueDeflectionStatus", StringType(), True),
                        StructField("Queue_Deflection_Status", StringType(), True),
                        StructField("QueueDeflectionMessage", StringType(), True),
                        StructField("DeflectionScheduleGroup", StringType(), True),
                        StructField("Log_LegWorkflowStart", StringType(), True),
                        StructField("Log_ConversationCheck", StringType(), True),
                        StructField("Log_LegWorkflowComplete", StringType(), True),
                        StructField("Log_SurveyWorkflowStartTime", StringType(), True),
                        StructField(
                            "QueueDeflectionScheduleGroup", StringType(), True
                        ),
                        StructField("ivr.Priority", StringType(), True),
                        StructField("ivr.Skills", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("participantId", StringType(), True),
            StructField("sessionIds", ArrayType(StringType(), True), True),
        ]
    )

    columns_to_drop = [
        "_type",
        "divisionIds",
        "endTime",
        "organizationId",
        "participantData",
        "startTime",
        "catalog_year",
        "catalog_month",
        "catalog_day",
        "_rescued_data",
        "file_name",
        "file_path",
        "file_modification_time",
        "__START_AT",
        "__END_AT",
        "ExternalXferNumber",
        "Queue_Deflection_Status",
        "sessionIds",
        "LegId"
    ]

    pa = (df
          .withColumn("participantData", F.regexp_replace("participantData", " ", ""))
          .withColumn("participant_data_struct", F.from_json(F.col("participantData"), ArrayType(json_schema))))
    
    pa_with_legid = (flatten(pa)
                     .withColumn("Leg_Id",F.when(F.col("LegId").isNull(), 
                                                F.concat(F.col("conversationId"), F.lit("-1"))).otherwise(F.col("LegId"))
                                 )
                     )
    pa_final = replaceUnsupportedColumnNames(pa_with_legid).drop(*columns_to_drop).distinct()

    return pa_final

def silverConversationParticipantSnapshot(conversations: DataFrame, 
                                          part_attributes: DataFrame, 
                                          divisions: DataFrame) -> DataFrame:
    """
    Contains all data extracted conversation jobs - Used as the source for all other Silver table
    Returns : Dataframe
    """
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
        "Queue",
        "Skill",
        "originatingDirection",
        "conversationStart",
        "Conversation_Date",
        "Conversation_End_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "Leg_Start_Time",
        "Leg_End_Time",
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
        "direction",
        "Country_Code",
        "Client_Brand",
        "Commercial_Type",
        "Call_Type",
        "Planning_Unit",
        "Timezone"   
    ]

    conversations = (conversations.withColumn("primary_division_id", F.col("divisionIds")[0]))
    #get timezone from divisions table
    divisions = (divisions.select("id", "name", F.substring(F.col("name"), -2, 2).alias("Timezone")).drop("name").distinct())
    #join converstaion with division to get the timezone
    conversations_divisions = (conversations
                               .join(divisions, conversations.primary_division_id == divisions.id, "left"))
    pa = participantAttributesExtract(part_attributes)

    p_attrubutes_lookup = (pa
                           .withColumnRenamed("conversationId", "convId")
                            .withColumnRenamed("participantId", "participant_Id")
                            .withColumnRenamed("Legid", "Leg_Id")
                            .withColumnRenamed("CountryCode", "Country_Code")
                            .withColumnRenamed("ClientBrand", "Client_Brand")
                            .withColumnRenamed("CommercialType", "Commercial_Type")
                            .withColumnRenamed("CallType", "Call_Type")
                            .withColumnRenamed("PlanningUnit", "Planning_Unit")
                            .select("participant_Id","Leg_Id","Country_Code","Client_Brand","Call_Type","Commercial_Type","Planning_Unit")
                            .distinct()
        )
    
    conversation_job_flattened = (
        flatten(conversations_divisions)
        .withColumnRenamed("ivr.Priority", "ivr_Priority")
        .withColumnRenamed("ivr.Skills", "ivr_Skills")
        .drop(*cols_to_drop_after_explode)
        .distinct()
    )

    conversation_in_local_time = ConvertToLocaltime(["conversationStart", "conversationEnd","segmentStart","segmentEnd"],conversation_job_flattened)

    conversation_participant_attributes = (conversation_in_local_time
                    .join(p_attrubutes_lookup, 
                          conversation_job_flattened.participantId == p_attrubutes_lookup.participant_Id,"leftouter")
                    .drop("participantId", "participant_Id","LegId", "convId")
                    .distinct())

    Conversation_jobs = (
        conversation_participant_attributes
        .withColumnRenamed("conversationId", "Conversation_Id")
        .withColumn("Conversation_Date", F.to_date(F.col("conversationStart")))
        .withColumn("Conversation_End_Date", F.to_date(F.col("conversationEnd")))
        .withColumn("Conversation_Date_Key",  F.regexp_replace(F.col("Conversation_Date"), "-", "").cast("long"))
        .withColumn("Conversation_Start_Time", F.date_format(F.col("conversationStart"), "HH:mm:ss:SSS"))
        .withColumn("Conversation_Start_Key",  F.regexp_replace(F.col("Conversation_Start_Time"), ":", "").cast("long"))
        .withColumn("Conversation_End_Time",F.date_format(F.col("conversationEnd"), "HH:mm:ss:SSS"))
        .withColumn("Conversation_End_Key", F.regexp_replace(F.col("Conversation_End_Time"), ":", "").cast("long"))
        .withColumn("Leg_Start_Time", F.date_format(F.col("segmentStart"), "HH:mm:ss:SSS"))
        .withColumn("Leg_End_Time", F.date_format(F.col("segmentEnd"), "HH:mm:ss:SSS"))
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
        .withColumnRenamed("NTLogin", "NTLOGIN")
        .withColumnRenamed("requestedRoutingSkillIds", "Skill")
        .distinct()
    )


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
    "nConsult"
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
    "Consult"
    ]

    conversation_job_metrics = metricsExtract(Conversation_jobs, metrics_col_oldname, metrics_col_newname)

    return conversation_job_metrics.select(*final_col_to_return).distinct()


#Tranformation of relevent data from all source to Inbound/outbound legs
def inboundOutboundLegs(
    conversation: DataFrame,
    divisions: DataFrame,
    routing_queues: DataFrame,
    routing_skills: DataFrame,
    direction: str,
    column_list : list
    ) -> DataFrame:
    
    """
    Tranformation of relevent data from all source to Inbound/outbound legs

    Parameters: conversation: DataFrame -> silver conversation snapshot dataframe
                divisions: DataFrame -> Division dataframe
                routing_queues: DataFrame -> Routing Queues
                routing_skills: DataFrame -> Routing Skills dataframe
                direction: str -> Inbound or Outbound
                column_list : list -> Final list columns to be returned
    Returns : Dataframe with Inbound/Outbound tranformations

    """
    
    #load conversation legs after filteing out on callback and direction as required / Note all legid in null should be neglated
    conversation = (conversation.filter((F.col("originatingDirection") == direction) & 
                        (F.col("Media_Type") != "callback") & 
                        (F.col("Leg_Id").isNotNull())
                        )) 
    #get timezone from queue name from queues data
    r_queue = (routing_queues.select(F.col("id").alias("Queue_ID"), F.col("name").alias("Queue_Name")).distinct())
    #get timezone from skill name from skills data
    r_skills = (routing_skills.select(F.col("id").alias("Skill_ID"), F.col("name").alias("Skill_Name")).distinct()) 

    #window specs to get last contact details by customer
    ani_window_spec = Window.partitionBy("ANI").orderBy("segmentStart")
    ddi_window_spec = Window.partitionBy("DDI").orderBy("segmentStart")


    conversation_time_agg = (
    conversation.select(
        F.col("Leg_Id").alias("leg_id"),
        "Dialing_Time",
        "Contacting_Time",
        "Talk_Time",
        "ACW_Time",
        "Handle_Time",
        "Monitoring_Time",
        "Voice_Mail_Time",
        "Held_Time",
        "Answered_Time",
        "Abandon_Time",
        "Short_Abandon_Time",
        "Agent_Response_Time",
        "Not_Responding_Time",
        "User_Response_Time",
        "IVR_Time",
        "Wait_Time",
        "ACD_Time",
        "Alert_Time",
    )
    .distinct()
    .fillna(0)
    .groupBy("leg_id")
    .agg(
        F.sum("Dialing_Time").alias("Dialing_Time"),
        F.sum("Contacting_Time").alias("Contacting_Time"),
        F.sum("Talk_Time").alias("Talk_Time"),
        F.sum("ACW_Time").alias("ACW_Time"),
        F.sum("Handle_Time").alias("Handle_Time"),
        F.sum("Monitoring_Time").alias("Monitoring_Time"),
        F.sum("Voice_Mail_Time").alias("Voice_Mail_Time"),
        F.sum("Held_Time").alias("Held_Time"),
        F.sum("Answered_Time").alias("Answered_Time"),
        F.sum("Abandon_Time").alias("Abandon_Time"),
        F.sum("Short_Abandon_Time").alias("Short_Abandon_Time"),
        F.sum("Agent_Response_Time").alias("Agent_Response_Time"),
        F.sum("Not_Responding_Time").alias("Not_Responding_Time"),
        F.sum("User_Response_Time").alias("User_Response_Time"),
        F.sum("IVR_Time").alias("IVR_Time"),
        F.sum("Wait_Time").alias("Wait_Time"),
        F.sum("ACD_Time").alias("ACD_Time"),
        F.sum("Alert_Time").alias("Alert_Time")
        )
    )
    conversation_leg_attributes = (
    conversation.select(
        "Leg_Id",
        "Conversation_Id",
        "conversationStart",
        "Conversation_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "segmentStart",
        "Leg_Start_Time",
        "Leg_End_Time",
        "Transferred",
        "Transferred_Blind",
        "Conference",
        "CoBrowse",
        "Consult",
        "Transferred_Consult",
        "Connected",
        "Disconnect_Type",
        "Offered",
        "Over_SLA",
        "NTLOGIN",
        "destinationAddresses",
        "ANI",
        "DNIS",
        "DDI",
        "AgentNTLogin",
        "Country_Code",
        "Client_Brand",
        "Commercial_Type",
        "Call_Type",
        "Planning_Unit",
        "Queue",
        "Skill",
        "primary_division_id",
        "Timezone"
    )
    .withColumn("Transfer_Queue",F.when(F.col("Queue").isNotNull(), F.col("Queue")).otherwise(None))
    .withColumn("Transfer_DDI",
            F.when(
                F.col("destinationAddresses").cast("double").isNotNull(),
                F.col("destinationAddresses").cast("double"),
            ).otherwise(None))
    .withColumn("Transfer_Agent_NTLOGIN",
            F.when(
                F.col("destinationAddresses").startswith("sip:"), 
                F.regexp_extract(F.col("destinationAddresses"), r"sip:(.*?)%", 1)
            ).otherwise(None),
        )
    .distinct()
    .fillna(0)
    .groupBy(
        "Leg_Id",
        "Conversation_Id",
        "conversationStart",
        "Conversation_Date",
        "Conversation_Date_Key",
        "Conversation_Start_Time",
        "Conversation_Start_Key",
        "Conversation_End_Time",
        "Conversation_End_Key",
        "Timezone")
    .agg(
        F.min("segmentStart").alias("segmentStart"),
        F.min("Leg_Start_Time").alias("Leg_Start_Time"),
        F.max("Leg_End_Time").alias("Leg_End_Time"),
        F.max("Transferred").alias("Transferred"),
        F.max("Transferred_Blind").alias("Transferred_Blind"),
        F.max("Conference").alias("Conference"),
        F.max("CoBrowse").alias("CoBrowse"),
        F.max("Consult").alias("Consult"),
        F.max("Transferred_Consult").alias("Transferred_Consult"),
        F.max("Connected").alias("Connected"),
        F.last("Disconnect_Type", ignorenulls=True).alias("Disconnect_Type"),
        F.max("Offered").alias("Offered"),
        F.max("Over_SLA").alias("Over_SLA"),
        F.max("NTLOGIN").alias("NTLOGIN"),
        F.max("Transfer_Queue").alias("Transfer_Queue"),
        F.max("Transfer_DDI").alias("Transfer_DDI"),
        F.max("Transfer_Agent_NTLOGIN").alias("Transfer_Agent_NTLOGIN"),
        F.max("ANI").alias("ANI"),
        F.max("DNIS").alias("DNIS"),
        F.max("DDI").alias("DDI"),
        F.max("AgentNTLogin").alias("AgentNTLogin"),
        F.max("Country_Code").alias("Country_Code"),
        F.max("Client_Brand").alias("Client_Brand"),
        F.max("Commercial_Type").alias("Commercial_Type"),
        F.max("Call_Type").alias("Call_Type"),
        F.max("Planning_Unit").alias("Planning_Unit"),
        F.max("Queue").alias("Queue"),
        F.max("Skill").alias("Skill"),
        F.max("primary_division_id").alias("primary_division_id")
        )
    )
    conversation_agg = (conversation_leg_attributes
                .join(conversation_time_agg, conversation_leg_attributes.Leg_Id == conversation_time_agg.leg_id, "left")
                .join(r_queue, conversation_leg_attributes.Queue == r_queue.Queue_ID, "left")
                .join(r_skills, conversation_leg_attributes.Skill == r_skills.Skill_ID, "left")
                .drop("Queue", "Skill", "leg_id")
                .withColumnRenamed("Queue_Name","Queue")
                .withColumnRenamed("Skill_Name","Skill")
                )

    if direction == "inbound":

        final = (conversation_agg
        .withColumn("Leg_Ordinal_value", F.substring(F.col("Leg_Id"), -1, 1))  # Transfer_Leg
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
        .withColumn("Leg_Start_Key",F.regexp_replace(F.col("Leg_Start_Time"), ":", "").cast("long"))
        .withColumn("Leg_End_Key", F.regexp_replace(F.col("Leg_End_Time"), ":", "").cast("long"))
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
            .withColumn("Previous_Contact_DateTime", (F.when(F.col("ANI").isNotNull(), F.lag("segmentStart", 1).over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.lag("segmentStart", 1).over(ddi_window_spec)
                                                                        ).otherwise(None)))
            )
            .withColumn("last_contact_time_diff_millisec", F.col("segmentStart").cast("long") - F.col("Previous_Contact_DateTime").cast("long"))
            .withColumn("days", F.expr("floor(last_contact_time_diff_millisec / (24 * 3600000))"))
            .withColumn("hours", F.expr("floor((last_contact_time_diff_millisec % (24 * 3600000)) / 3600000)"))
            .withColumn("minutes", F.expr("floor((last_contact_time_diff_millisec % 3600000) / 60000)"))
            .withColumn("seconds", F.expr("floor((last_contact_time_diff_millisec % 60000) / 1000)"))
            .withColumn("milliseconds", F.expr("last_contact_time_diff_millisec % 1000"))
            .withColumn("Delta_Contact_DateTime",
                        F.expr("concat_ws(':', lpad(days, 3, '0'), lpad(hours, 2,0), lpad(minutes,2, 0), lpad(seconds,2, 0)) || '.' || lpad(milliseconds, 3,0)"))
            .withColumn("Callback_Request", F.lit("null"))
            .withColumn("Callback_Handled", F.lit("null"))
            .withColumn("Callback_No_Answer", F.lit("null"))
            .withColumn("Callback_Abandoned", F.lit("null"))
            .withColumn("Pre_Request_Wait_Time", F.lit("null"))
            .withColumn("Callback_Wait_Time", F.lit("null"))
        )
        return final.select(*column_list)
    
    if direction == "outbound":

        final = (conversation_agg
                 .withColumn("Leg_Ordinal_value", F.substring(F.col("Leg_Id"), -1, 1))  # Transfer_Leg
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
        .withColumn("Leg_Start_Key",F.regexp_replace(F.col("Leg_Start_Time"), ":", "").cast("long"))
        .withColumn("Leg_End_Key", F.regexp_replace(F.col("Leg_End_Time"), ":", "").cast("long"))
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
            .withColumn("last_contact_time_diff_millisec", F.col("segmentStart").cast("long") - F.col("Previous_Contact_DateTime").cast("long"))
            .withColumn("days", F.expr("floor(last_contact_time_diff_millisec / (24 * 3600000))"))
            .withColumn("hours", F.expr("floor((last_contact_time_diff_millisec % (24 * 3600000)) / 3600000)"))
            .withColumn("minutes", F.expr("floor((last_contact_time_diff_millisec % 3600000) / 60000)"))
            .withColumn("seconds", F.expr("floor((last_contact_time_diff_millisec % 60000) / 1000)"))
            .withColumn("milliseconds", F.expr("last_contact_time_diff_millisec % 1000"))
            .withColumn("Delta_Contact_DateTime",
                        F.expr("concat_ws(':', lpad(days, 3, '0'), lpad(hours, 2,0), lpad(minutes,2, 0), lpad(seconds,2, 0)) || '.' || lpad(milliseconds, 3,0)"))
            .withColumn("ACD_OB_Attempt", F.lit(1))
            .withColumn(
            "ACD_OB_Connected", F.when(F.col("Talk_Time").isNotNull(), 1).otherwise(0)
        )
        )

        return final.select(*column_list).distinct()
    
def callBackConversations(conversation_leg, queues, skills, user):
    """
    Tranformation of relevent data from all source to Call Back Conversations legs

    Parameters: conversation_leg: DataFrame -> silver conversation snapshot dataframe
                partition_attributes: DataFrame -> partition attributes dataframe
                divisions: DataFrame -> Division dataframe
                queues: DataFrame -> Routing Queues
                skills: DataFrame -> Routing Skills dataframe
                user: DataFrame -> User dataframe
    Returns : Dataframe with call back conversations tranformations

    """

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

    callback_datetime = (conversation_leg
                         .withColumn("last_media_type", F.lag("Media_Type").over(window_spec))
                         .select("Conversation_Id",
                                 "Purpose",
                                 "Media_Type",
                                 "direction",
                                 "last_media_type",
                                 "segmentStart").distinct())

    # call back date depends on purpose = 'agent' and the media_type = 'voice' this is the actual call segment where the agent speaks to the customer.
    callback_datetime = (callback_datetime
                         .filter((F.col("Purpose") == "agent") &
                                (F.col("Media_Type") == "voice") &
                                (F.col("last_media_type") == "callback") &
                                (F.col("direction") == "outbound"))
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
        .groupby("conv_id").agg(F.max("queue_name").alias("queue_name"),
                                F.max("skill_name").alias("skill_name"))
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
            "DNIS",
            "Country_Code",
            "Client_Brand",
            "Commercial_Type",
            "Call_Type",
            "Planning_Unit",
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
        callback_metrics.groupBy("Conversation_Id")
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
            F.max(F.col("DNIS")).alias("DNIS"),
            F.max("NTLOGIN").alias("NTLOGIN"),
            F.max("Country_Code").alias("Country_Code"),
            F.max("Client_Brand").alias("Client_Brand"),
            F.max("Commercial_Type").alias("Commercial_Type"),
            F.max("Call_Type").alias("Call_Type"),
            F.max("Planning_Unit").alias("Planning_Unit"),
            F.max("Disconnect_Type").alias("Disconnect_Type"))
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

def silverConversationParticipantAttributes(df):

    participant_attributes = (participantAttributesExtract(df)
    .withColumn("Leg_Id",
        F.when(F.col("Leg_Id").isNull(), F.concat(F.col("conversationId"), F.lit("-1"))).otherwise(F.col("Leg_Id")),)
    .withColumnRenamed("conversationId", "Conversation_Id")
    .drop("participantPurpose", "participantId", "LegId","ScheduleGroup"))

    pivot_columns = [col_name for col_name in participant_attributes.columns if col_name not in ("Conversation_Id", "Leg_Id")]

    participant_attributes_agg = (participant_attributes
                                  .groupBy("Conversation_Id", "Leg_Id")
                                  .agg(*[F.max(col_name).alias(col_name) for col_name in pivot_columns]))
    
    stack_list = [f"'{col_name}', {col_name}," for col_name in pivot_columns]  # string to pass to stack expr
    stack_string = "".join(stack_list)[:-1]

    participant_attributes_pivot = participant_attributes_agg.selectExpr(
    "Conversation_Id",
    "Leg_Id",
    f"stack({len(pivot_columns)}, {stack_string}) as (attribute_name, attribute_value)",
    )
    
    return participant_attributes_pivot

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Silver Conversation Snapshot
@dlt.table(comment="Pipeline - silver_conversation_snapshot")
def silver_conversation_snapshot():
    staging_conv_jobs = dlt.read("stg_conversation_job")#silver staging data
    staging_conv_jobs = (staging_conv_jobs
                         .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull())))
                         .select("*", F.current_timestamp().alias("Last_Modified")))
    participant_attributes = (dlt.read("stg_silver_participant_attributes")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    divisions = (dlt.read("stg_silver_divisions")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))

    return silverConversationParticipantSnapshot(staging_conv_jobs, participant_attributes, divisions)

# COMMAND ----------

# DBTITLE 1,Silver Inbound Conversation
@dlt.table(comment="Pipeline - Silver_Inbound_Conversation_Leg")
def silver_inbound_conversation_leg():

    #Final Columns to return
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

    divisions = (dlt.read("stg_silver_divisions")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    conversation = dlt.read("silver_conversation_snapshot")
    r_queue = (dlt.read("stg_silver_routing_queues")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    r_skill = (dlt.read("stg_silver_routing_skills")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))

    return inboundOutboundLegs(
        conversation = conversation,
        divisions = divisions,
        routing_queues = r_queue,
        routing_skills = r_skill,
        direction = "inbound",
        column_list = column_list
        ).select("*",F.current_timestamp().alias("Last_Modified"))

# COMMAND ----------

# DBTITLE 1,Silver Outbound Conversation
@dlt.table(comment="Pipeline - Silver_Outbound_Conversation_Leg")
def silver_outbound_conversation_leg():

    #Final Columns to return
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
        "Consult"
    ]
    divisions = (dlt.read("stg_silver_divisions")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    conversation = dlt.read("silver_conversation_snapshot")
    r_queue = (dlt.read("stg_silver_routing_queues")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    r_skill = (dlt.read("stg_silver_routing_skills")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    participant_attributes = (dlt.read("stg_silver_participant_attributes")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))

    return inboundOutboundLegs(
        conversation = conversation,
        divisions = divisions,
        routing_queues = r_queue,
        routing_skills = r_skill,
        direction = "outbound",
        column_list = column_list
        ).select("*",F.current_timestamp().alias("Last_Modified"))

# COMMAND ----------

# DBTITLE 1,Silver Callback Conversation
@dlt.table(comment="Pipeline - silver_callback_conversation")
def silver_callback_conversation():

    call_back = dlt.read("silver_conversation_snapshot")

    participant_attribute = dlt.read("stg_silver_participant_attributes").filter(
        ((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))
    )

    user_details = (dlt.read("stg_silver_user")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))

    queues = dlt.read("stg_silver_routing_queues").withColumnRenamed(
        "name", "queue_name"
    )

    skills = (
        dlt.read("stg_silver_routing_skills")
        .withColumnRenamed("id", "skillId")
        .withColumnRenamed("name", "skill_name")
    )

    callback_conversations = callBackConversations(
        call_back, queues, skills, user_details
    ).select("*", F.current_timestamp().alias("Last_Modified"))

    return callback_conversations

# COMMAND ----------

# DBTITLE 1,Silver CSAT
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

# DBTITLE 1,Silver Conversation Participant Attributes
@dlt.table(comment="Pipeline - Silver Conversation Participant Attributes")
def silver_conversation_participant_attributes():

    participant_atribute = dlt.read("stg_silver_participant_attributes").filter(
        ((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))
    )
    participant_atribute_final = silverConversationParticipantAttributes(
        participant_atribute
    )

    return participant_atribute_final.select(
        "*", F.current_timestamp().alias("Last_Modified")
    )
