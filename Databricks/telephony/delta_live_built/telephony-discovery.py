# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, StructType
import logging
logger = logging.getLogger("com.telephony.discovery")

# COMMAND ----------

bcj = (spark.sql("select * from telephony_ss.bronze_conversation_job")
       .drop("file_path","file_name","file_size","file_modification_time","processing_time","source_file")
       )
bd = (spark.sql("select * from telephony_ss.bronze_division").select("id","name")
      .drop("file_path","file_name","file_size","file_modification_time","processing_time","source_file"))
pa = (spark.sql("select * from telephony_ss.participant_attributes").select(
        "organizationId",
        "conversationId",
        "startTime",
        "endTime",
        "divisionIds",
        "participantData",
    ))

# COMMAND ----------

display(pa.filter(F.col("participantData").like("%name%")))

# COMMAND ----------

final_df = (bcj
            .join(bd,bcj.primary_division_id == bd.id,how="inner")
            .join(pa,bcj.conversationId == pa.conversationId,how="inner")
            .select(
            bcj["*"],
            bd["*"],
            pa["organizationId"],
            pa["startTime"],
            pa["endTime"],
            pa["participantData"])
            .withColumn("participants_extract",F.explode(F.col("participants")))
            .withColumn("Transfer_Leg",F.when(F.size(F.col("participants.sessions")) > 1, True).otherwise(False))
            .withColumn("Timezone",F.substring(F.col("name"), 1, 2))
            .withColumn("Conversation_Date", F.to_date(F.col("conversationStart")))
            .withColumn("Conversation_Date_Key", 
                        F.concat_ws("", 
                                    F.date_format(F.col("conversationStart"), "yyyy"), 
                                    F.date_format(F.col("conversationStart"), "MM"), 
                                    F.date_format(F.col("conversationStart"), "dd")).cast("integer"))
            .withColumn("Conversation_Start_Time", F.date_format(F.col("conversationStart"), "HH:mm:ss"))
            .withColumn("Conversation_Start_Key", 
                        F.concat_ws("", 
                                    F.date_format(F.col("conversationStart"), "HH"), 
                                    F.date_format(F.col("conversationStart"), "mm"), 
                                    F.date_format(F.col("conversationStart"), "ss")).cast("integer"))
            .withColumn("Conversation_End_Time", F.date_format(F.col("conversationEnd"),"HH:mm:ss"))
            .withColumn("Conversation_End_Key", 
                        F.concat_ws("", 
                                    F.date_format(F.col("conversationEnd"), "yyyy"), 
                                    F.date_format(F.col("conversationEnd"), "MM"), 
                                    F.date_format(F.col("conversationEnd"), "dd")).cast("integer"))
            .withColumn("Leg_Start_Time", F.to_date(F.col("startTime")))
            .withColumn("Leg_Start_Key", 
                        F.concat_ws("", 
                                    F.date_format(F.col("startTime"), "yyyy"), 
                                    F.date_format(F.col("startTime"), "MM"), 
                                    F.date_format(F.col("startTime"), "dd")).cast("integer"))
            .withColumn("Leg_End_Time", F.to_date(F.col("endTime")))
            .withColumn("Leg_End_Key", 
                        F.concat_ws("", 
                                    F.date_format(F.col("endTime"), "yyyy"), 
                                    F.date_format(F.col("endTime"), "MM"), 
                                    F.date_format(F.col("endTime"), "dd")).cast("integer"))
            .withColumn("ANI",F.regexp_replace(F.col("participants_extract.sessions.ani")[0],"[^0-9]", "").cast("long"))
            .withColumn("Customer_Contact_Sequence",F.size(F.col("participants.sessions")))
            .withColumn("DNIS",F.regexp_replace(F.col("participants_extract.sessions.dnis")[0],"[^0-9]", "").cast("long"))
            )

# COMMAND ----------

display(final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #Extracting Bronze Conversation Jobs

# COMMAND ----------

display(bcj.limit(10))

# COMMAND ----------

display(bcj.filter(F.col("participants") == "callBack"))

# COMMAND ----------

bcj_extract = (
    bcj.withColumn("participants_extract", F.explode(F.col("participants")))
    .withColumn(
        "disconnecttype",
        F.explode(F.col("participants_extract.sessions.segments.disconnectType")[0]),
    )
    .withColumn("metrics", F.explode(F.col("participants_extract.sessions.metrics")[0]))
    .withColumn("metrics_name",F.col("metrics.name"))
    .withColumn(
        "nConnected",
        F.when(F.col("metrics.name") == "nConnected", F.col("metrics.value")).otherwise(
            None
        ),
    )
)

# COMMAND ----------

cc = 'F.expr("map(`123`, `1233`)'

# COMMAND ----------

# aaa = bcj_extract.withColumn("1234", F.expr("map(`123`, `1233`)"))

# COMMAND ----------

display(bcj_extract.select("metrics_name").distinct()
        .filter(F.col("metrics_name").like("tUser")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Final DF BCJ

# COMMAND ----------

col_to_drop = ["conversationEnd","conversationStart","mediaStatsMinConversationMos","mediaStatsMinConversationRFactor","originatingDirection","participants","participants_extract","metrics"]

bcj_final = (
    bcj.withColumn("participants_extract", F.explode(F.col("participants")))
    .withColumn(
        "Transfer_Leg",
        F.when(F.size(F.col("participants.sessions")) > 1, True).otherwise(False),
    )
    .withColumn("Conversation_Date", F.to_date(F.col("conversationStart")))
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
        "Conversation_Start_Time", F.date_format(F.col("conversationStart"), "HH:mm:ss")
    )
    .withColumn(
        "Conversation_Start_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("conversationStart"), "HH"),
            F.date_format(F.col("conversationStart"), "mm"),
            F.date_format(F.col("conversationStart"), "ss"),
        ).cast("integer"),
    )
    .withColumn(
        "Conversation_End_Time", F.date_format(F.col("conversationEnd"), "HH:mm:ss")
    )
    .withColumn(
        "Conversation_End_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("conversationEnd"), "yyyy"),
            F.date_format(F.col("conversationEnd"), "MM"),
            F.date_format(F.col("conversationEnd"), "dd"),
        ).cast("integer"),
    )
    .withColumn(
        "ANI",
        F.regexp_replace(
            F.col("participants_extract.sessions.ani")[0], "[^0-9]", ""
        ).cast("long"),
    )
    .withColumn("Customer_Contact_Sequence", F.size(F.col("participants.sessions")))
    .withColumn(
        "DNIS",
        F.regexp_replace(
            F.col("participants_extract.sessions.dnis")[0], "[^0-9]", ""
        ).cast("long"),
    )
    .withColumn("metrics", F.explode(F.col("participants_extract.sessions.metrics")[0]))
    .withColumn(
        "Connected",
        F.when(F.col("metrics.name") == "nConnected", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "IVR_Time",
        F.when(F.col("metrics.name") == "tIvr", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Offered",
        F.when(F.col("metrics.name") == "nOffered", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "ACD_Time",
        F.when(F.col("metrics.name") == "tAcd", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Wait_Time",
        F.when(F.col("metrics.name") == "tWait", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Over_SLA",
        F.when(F.col("metrics.name") == "nOverSla", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Answered_Time",
        F.when(F.col("metrics.name") == "tAnswered", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Abandon_Time",
        F.when(F.col("metrics.name") == "tAbandoned", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Short_Abandon_Time",
        F.when(F.col("metrics.name") == "tShortAbandon", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Agent_Response_Time",
        F.when(F.col("metrics.name") == "tAgentResponseTime", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Alert_Time",
        F.when(F.col("metrics.name") == "tAlert", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Talk_Time",
        F.when(F.col("metrics.name") == "tTalk", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "Held_Time",
        F.when(F.col("metrics.name") == "tHeld", F.col("metrics.value")).otherwise(
            None
        ),
    )
    .withColumn(
        "ACW_Time",
        F.when(F.col("metrics.name") == "tAcw", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Handle_Time",
        F.when(F.col("metrics.name") == "tHandle", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Dialing_Time",
        F.when(F.col("metrics.name") == "tDialing", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Contacting_Time",
        F.when(F.col("metrics.name") == "tContacting", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Monitoring_Time",
        F.when(F.col("metrics.name") == "tMonitoring", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Not_Responding_Time",
        F.when(F.col("metrics.name") == "tNotResponding", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "User_Response_Time",
        F.when(F.col("metrics.name") == "tUserResponseTime", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Voice_Mail_Time",
        F.when(F.col("metrics.name") == "tVoicemail", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Disconnect_Type",
        F.explode(F.col("participants_extract.sessions.segments.disconnectType")[0])
    )
    .withColumn(
        "Transferred",
        F.when(F.col("metrics.name") == "nTransferred", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Transferred_Blind",
        F.when(F.col("metrics.name") == "nBlindTransferred", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Transferred_Consult",
        F.when(F.col("metrics.name") == "nConsultTransferred", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Conference",
        F.explode(F.col("participants_extract.sessions.segments.conference")[0])
    )
    .drop(*col_to_drop)
)

# COMMAND ----------

column_names = [
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
    "Callback_Pre_Request_Wait_Time",
    "Callback_Attempt_Wait_Time"
]


# COMMAND ----------

col_avbl = bcj_final.columns

# COMMAND ----------

difference1 = list(set(column_names) - set(col_avbl))

# COMMAND ----------

len(difference1)

# COMMAND ----------

display(bcj_final)#filter(F.col("tUserResponseTime").isNotNull()

# COMMAND ----------

# MAGIC %md
# MAGIC # Divisions

# COMMAND ----------

bd_final = bd.withColumn("Timezone",F.substring(F.col("name"), 1, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Participant Attributes 

# COMMAND ----------

display(pa)

# COMMAND ----------

final = (bcj_final
         .join(bd_final, bcj.primary_division_id == bd_final.id, how="inner")
         )

# COMMAND ----------

display()

# COMMAND ----------

display(bcj
        .filter((F.col("originatingDirection") == "inbound") & (F.col("conversationId") == "e578b550-3c8e-4729-9831-fa5e113c5098")))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from telephony_ss.bronze_routing_skills limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC use telephony_ss;
# MAGIC show tables;

# COMMAND ----------


