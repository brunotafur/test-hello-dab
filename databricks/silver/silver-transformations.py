# Databricks notebook source
import os
import sys

path = os.getcwd()
cwd = "/".join(path.split("/")[:-1])#getting file path of the utility and pipeline imports
sys.path.insert(0, cwd)

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import dlt
from utility import *
from silver import *

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

    return silverConversationSnapshot(staging_conv_jobs,participant_attributes)

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
    conversation = (dlt.read("silver_conversation_snapshot")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    r_queue = (dlt.read("stg_silver_routing_queues")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    r_skill = (dlt.read("stg_silver_routing_skills")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))
    participant_attributes = (dlt.read("stg_silver_participant_attributes")
       .filter(((F.col("__START_AT").isNotNull()) & (F.col("__END_AT").isNull()))))

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

# DBTITLE 1,Silver Callback Conversation
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

    participant_atribute = dlt.read("stg_silver_participant_attributes")
    participant_atribute_final = silverConversationParticipantAttributes(
        participant_atribute
    )

    return participant_atribute_final.select(
        "*", F.current_timestamp().alias("Last_Modified")
    )
