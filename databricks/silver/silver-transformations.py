# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

ani_window_spec = Window.partitionBy("ANI","Conversation_Id").orderBy("conversationStart")
dnis_window_spec = Window.partitionBy("DNIS","Conversation_Id").orderBy("conversationStart")

# COMMAND ----------

# DBTITLE 1,Conversation Jobs - Function
reqcols_coversation_jobs = [
    "Conversation_Id",
    "Leg_Id",
    "primary_division_id",
    "participantId",
    "queueId",
    "requestedRoutingSkillIds",
    "originatingDirection",
    "Transfer_Leg",
    "Leg_Ordinal",
    "Alternate_Leg_Flag",
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
    "DDI",
    "Customer_Contact_Sequence_ani",
    "Previous_Contact_DateTime_ani",
   "Delta_Contact_DateTime_ani",
   "Customer_Contact_Sequence_dnis",
    "Previous_Contact_DateTime_dnis",
   "Delta_Contact_DateTime_dnis",
    "DNIS",
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
    "Callback_Request",
    "Callback_Handled",
    "Callback_No_Answer",
    "Callback_Abandoned",
    "Callback_Pre_Request_Wait_Time",
    "Callback_Attempt_Wait_Time",
    "SurveyConversationId",
    "SurveyStartTime",
    "AgentNTLogin",
    "CustomerANI",
    "SurveyCSATScore",
    "SurveyFCRScore",
    "ComfortMsg1"]

def conversationJobExtract(bronzeConvJobsDF, columns: list):

    BCJ = (bronzeConvJobsDF
    .withColumn("conversationStart", F.to_timestamp(F.col("conversationStart"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    .withColumn("conversationEnd", F.to_timestamp(F.col("conversationEnd"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    .withColumn("primary_division_id", F.col("divisionIds")[0])).select(
        "conversationId",
        "conversationStart",
        "conversationEnd",
        "originatingDirection",
        F.col("divisionIds")[0].alias("primary_division_id"),
        F.explode("participants").alias("participants_extract"))
    
    conversation_jobs = (BCJ.select(
        "conversationId",
        "conversationStart",
        "conversationEnd",
        "primary_division_id",
        "originatingDirection",
        "participants_extract.*",
    )
                         
    .select("*", "attributes.*", F.explode("sessions").alias("sessions_explode"))
    .withColumnRenamed("conversationId", "Conversation_Id")
    .withColumnRenamed("NTLogin", "NTLOGIN")
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
    .withColumn("Conversation_Start_Time", F.date_format(F.col("conversationStart"), "HH:mm:ss:SSS"))
    .withColumn(
        "Conversation_Start_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("conversationStart"), "HH"),
            F.date_format(F.col("conversationStart"), "mm"),
            F.date_format(F.col("conversationStart"), "ss"),
            F.date_format(F.col("conversationStart"), "SSS")
        ).cast("integer"),
    )
    .withColumn(
        "Conversation_End_Time", F.date_format(F.col("conversationEnd"), "HH:mm:ss:SSS")
    )
    .withColumn(
        "Conversation_End_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("conversationEnd"), "HH"),
            F.date_format(F.col("conversationEnd"), "mm"),
            F.date_format(F.col("conversationEnd"), "ss"),
            F.date_format(F.col("conversationEnd"), "SSS")
        ).cast("integer")
    )
    .withColumnRenamed("LegId", "Leg_Id")
    .withColumn("split_Leg_Id", F.split(F.col("Leg_Id"), "-"))
    .withColumn("Leg_Ordinal_value", F.element_at(F.col("split_Leg_Id"), F.size(F.col("split_Leg_Id"))))#Transfer_Leg
    .withColumn("Leg_Ordinal",F.when(F.col("Leg_Ordinal_value").cast("double").isNotNull(), F.col("Leg_Ordinal_value").cast("int")).otherwise(None))
    .withColumn("Alternate_Leg_Flag", F.when(F.col("Leg_Ordinal_value").rlike('[a-zA-Z]'), F.col("Leg_Ordinal_value")).otherwise(None))
    .withColumn("Transfer_Leg", F.when(F.col("Leg_Ordinal") > 1, True).otherwise(False))
    .withColumn("mediaType", F.col("sessions_explode.mediaType"))#checkduplicate
    .withColumn("segment_start_time",F.col("sessions_explode.segments.segmentStart"))
    .withColumn("Leg_Start_Time", (F.col("segment_start_time")[0]).cast("timestamp"))
    .withColumn("Leg_End_Time", (F.element_at(F.col("segment_start_time"), F.size(F.col("segment_start_time")))).cast("timestamp"))
    .withColumn(
        "Leg_Start_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("Leg_Start_Time"), "HH"),
            F.date_format(F.col("Leg_Start_Time"), "mm"),
            F.date_format(F.col("Leg_Start_Time"), "ss"),
            F.date_format(F.col("Leg_End_Time"), "SSS")
        ).cast("integer")
    )
    .withColumn(
        "Leg_End_Key",
        F.concat_ws(
            "",
            F.date_format(F.col("Leg_End_Time"), "HH"),
            F.date_format(F.col("Leg_End_Time"), "mm"),
            F.date_format(F.col("Leg_End_Time"), "ss"),
            F.date_format(F.col("Leg_End_Time"), "SSS")
        ).cast("integer")
    )
    .withColumn(
        "ANI",
        F.regexp_replace(
            F.col("sessions.ani")[0], "[^0-9]", ""
        ).cast("long"),
    )
    .withColumn(
        "DNIS",
        F.regexp_replace(
            F.col("sessions.dnis")[0], "[^0-9]", ""
        ).cast("long"),
    )
    .withColumn("Customer_Contact_Sequence_ani", F.rank().over(ani_window_spec))
    .withColumn("Customer_Contact_Sequence_dnis", F.rank().over(dnis_window_spec))
    .withColumn("Previous_Contact_DateTime_ani", F.lag("conversationStart").over(ani_window_spec))
    .withColumn("Previous_Contact_DateTime_dnis", F.lag("conversationStart").over(dnis_window_spec))
    .withColumn("Delta_Contact_DateTime_ani",F.col("conversationStart").cast("long") - F.col("Previous_Contact_DateTime_ani").cast("long"))
    .withColumn("Delta_Contact_DateTime_dnis",F.col("conversationStart").cast("long") - F.col("Previous_Contact_DateTime_dnis").cast("long"))
    .withColumn("metrics", F.explode(F.col("sessions.metrics")[0]))
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
        F.explode(F.col("sessions.segments.disconnectType")[0])
    )
    .withColumn(
        "queueId",
        F.explode(F.col("sessions.segments.queueId")[0])
    )
    .withColumn(
        "requestedRoutingSkillIds",
        F.col("sessions.segments.requestedRoutingSkillIds")[0][0][0]
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
        F.explode(F.col("sessions.segments.conference")[0])
    )
    .withColumn(
        "destinationAddresses",
        F.explode(F.col("sessions.destinationAddresses")[0])
    )
    .withColumn(
        "CoBrowse",
        F.when(F.col("metrics.name") == "nCobrowseSessions", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn(
        "Consult",
        F.when(F.col("metrics.name") == "nConsult", F.col("metrics.value")).otherwise(
            None
        )
    )
    .withColumn("Handled",F.when((F.col("Answered_Time").isNull())| (F.col("Answered_Time") == 0),0).otherwise(1))
    .withColumn("Abandoned",F.when((F.col("Abandon_Time").isNull())| (F.col("Abandon_Time") == 0),0).otherwise(1))
    .withColumn("Callback_Request",F.lit("Callback_Request"))
    .withColumn("Callback_Handled",F.lit("Callback_Handled"))
    .withColumn("Callback_No_Answer",F.lit("Callback_No_Answer"))
    .withColumn("Callback_Abandoned",F.lit("Callback_Abandoned"))
    .withColumn("Callback_Pre_Request_Wait_Time",F.lit("Callback_Pre_Request_Wait_Time"))
    .withColumn("Callback_Attempt_Wait_Time",F.lit("Callback_Attempt_Wait_Time"))
    .withColumnRenamed("ivr.Priority","ivr_Priority")
    .withColumnRenamed("ivr.Skills","ivr_Skills")
    .filter((F.col("mediaType") != "callback") & (F.col("Leg_Id").isNotNull()))
    .distinct()
    )
    return conversation_jobs.select(*columns).distinct()

# COMMAND ----------

def participantAttributesExtract(participantAttributesDF):
    PA = (participantAttributesDF
    .withColumn("participantData_extract", F.explode("participantData"))
    .withColumn("participant_Id", F.col("participantData_extract.participantId"))
    .withColumn("Country_Code", F.col("participantData_extract.participantAttributes.CountryCode"))
    .withColumn("Client_Brand", F.col("participantData_extract.participantAttributes.ClientBrand"))
    .withColumn("Commercial_Type", F.col("participantData_extract.participantAttributes.CallType"))
    .withColumn("Call_Type", F.col("participantData_extract.participantAttributes.CommercialType"))
    .withColumn("Planning_Unit", F.col("participantData_extract.participantAttributes.PlanningUnit"))
    .withColumn("LegId", F.col("participantData_extract.participantAttributes.LegId"))).select(
    "LegId",
    "participant_Id",
    "Country_Code",
    "Client_Brand",
    "Commercial_Type",
    "Call_Type",
    "Planning_Unit")
    return PA

# COMMAND ----------

def silverConversationParticipantAttributes(df):

    # try:
    participant_attributes = (
    (
    df.withColumnRenamed("conversationId", "conversation_Id").withColumn(
    "participant_data_extract", F.explode(F.col("participantData"))
    )
    )
    .select(
    "conversation_Id", "participant_data_extract.participantAttributes.*"
    )
    .withColumnRenamed("LegId", "Leg_Id")
    .withColumnRenamed("ivr.Skills", "ivr_Skills")
    .withColumnRenamed("ivr.Priority", "ivr_Priority")
    .filter(F.col("Leg_Id").isNotNull())
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
    # except Exception as e:
    #     print("An unexpected error occurred:", e)

# COMMAND ----------

def silverInboundOutbound(
    moduleName,
    columns,
    bronzeConvJobs,
    bronzeParticipantAtributes,
    bronzeDivisions,
    bronzeRoutingQueue,
    bronzeRoutingSkills):
    data = (
        bronzeConvJobs.join(
            bronzeDivisions,
            bronzeConvJobs.primary_division_id == bronzeDivisions.id,
            "left_outer",
        )
        .join(
            bronzeRoutingQueue,
            bronzeConvJobs.queueId == bronzeRoutingQueue.id,
            "left_outer",
        )
        .join(
            bronzeRoutingSkills,
            bronzeConvJobs.requestedRoutingSkillIds == bronzeRoutingSkills.id,
            "left_outer",
        )
        .join(
            bronzeParticipantAtributes,
            bronzeConvJobs.participantId == bronzeParticipantAtributes.participant_Id,
            "left_outer",
        )
        .withColumn("Transfer_Queue",F.when(F.col("Queue").isNotNull(),F.col("Queue")).otherwise(None))
        .withColumn("Transfer_DDI", F.when(F.col("destinationAddresses").cast("double").isNotNull(),
                                           F.col("destinationAddresses").cast("double")).otherwise(None))
        .withColumn("Transfer_Agent_NTLOGIN", F.when(F.col("destinationAddresses").rlike('[a-zA-Z]'),
                                                     F.col("AgentNTLogin")).otherwise(None))
        .withColumn("ACD_OB_Attempt", F.lit(1))
        .withColumn("ACD_OB_Connected", F.when(F.col("Talk_Time").isNotNull(), 1).otherwise(0)))

    if moduleName == "inbound_call_logs":
        return (
            data.filter(F.col("originatingDirection") == "inbound")
            .select(*columns)
            .distinct()
        )
    elif moduleName == "outbound_call_logs":
        return (
            data.filter(F.col("originatingDirection") == "outbound")
            .select(*columns)
            .distinct()
        )
          
    else:
        print("Not able to load the data - Kindly check the inputs")

# COMMAND ----------

reqcols_inbound = [
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
    F.col("Customer_Contact_Sequence_ani").alias("Customer_Contact_Sequence"),
    F.col("Previous_Contact_DateTime_ani").alias("Previous_Contact_DateTime"),
    F.col("Delta_Contact_DateTime_ani").alias("Delta_Contact_DateTime"),
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
    "Conference",
    "CoBrowse",
    "Consult",
    "Callback_Request",
    "Callback_Handled",
    "Callback_No_Answer",
    "Callback_Abandoned",
    "Callback_Pre_Request_Wait_Time",
    "Callback_Attempt_Wait_Time"]

# COMMAND ----------

reqcols_outbound = [
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
    F.col("Customer_Contact_Sequence_dnis").alias("Customer_Contact_Sequence"),
    F.col("Previous_Contact_DateTime_dnis").alias("Previous_Contact_DateTime"),
    F.col("Delta_Contact_DateTime_dnis").alias("Delta_Contact_DateTime"),
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
    "CoBrowse"
]

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver_Inbound_Conversation_Leg")
def silver_inbound_conversation_leg():
    
    bronze_conv_jobs = conversationJobExtract(dlt.read("bronze_conversation_job"),reqcols_coversation_jobs)
    bronze_part_attributes = participantAttributesExtract(dlt.read("bronze_participant_attributes"))
    bronze_divisions = dlt.read("bronze_divisions").select("id", "name", F.substring(F.col("name"), 1, 2).alias("Timezone"))
    bronze_r_queues = dlt.read("bronze_routing_queues").select("id", F.col("name").alias("Queue"))
    bronze_r_skills = dlt.read("bronze_routing_skills").select("id", F.col("name").alias("Skill"))

    return(silverInboundOutbound(
        moduleName = "inbound_call_logs",
        columns = reqcols_inbound,
        bronzeConvJobs = bronze_conv_jobs,
        bronzeParticipantAtributes = bronze_part_attributes,
        bronzeDivisions = bronze_divisions,
        bronzeRoutingQueue = bronze_r_queues,
        bronzeRoutingSkills = bronze_r_skills
        ))

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver_Outbound_Conversation_Leg")
def silver_outbound_conversation_leg():

    bronze_conv_jobs = conversationJobExtract(dlt.read("bronze_conversation_job"), reqcols_coversation_jobs)
    bronze_part_attributes = participantAttributesExtract(
        dlt.read("bronze_participant_attributes")
    )
    bronze_divisions = dlt.read("bronze_divisions").select(
        "id", "name", F.substring(F.col("name"), 1, 2).alias("Timezone")
    )
    bronze_r_queues = dlt.read("bronze_routing_queues").select(
        "id", F.col("name").alias("Queue")
    )
    bronze_r_skills = dlt.read("bronze_routing_skills").select(
        "id", F.col("name").alias("Skill")
    )

    return silverInboundOutbound(
        moduleName = "outbound_call_logs",
        columns = reqcols_inbound,
        bronzeConvJobs = bronze_conv_jobs,
        bronzeParticipantAtributes = bronze_part_attributes,
        bronzeDivisions = bronze_divisions,
        bronzeRoutingQueue = bronze_r_queues,
        bronzeRoutingSkills = bronze_r_skills,
    )

# COMMAND ----------

@dlt.table(comment="Pipeline - Silver CSAT")
def silver_csat():

    reqcols_csat = [
        "Conversation_Id",
        "Leg_Id",
        F.col("SurveyConversationId").alias("CSAT_Audit_Id"),
        F.col("SurveyStartTime").alias("CSAT_Call_Date"),
        F.col("AgentNTLogin").alias("NTLOGIN"),
        F.col("CustomerANI").alias("ANI"),
        F.col("SurveyCSATScore").alias("CSAT_Score"),
        F.col("SurveyFCRScore").alias("FCR_Score"),
        F.lit("SurveyNPSScore").alias("NPS_Score"),
        F.col("ComfortMsg1").alias("Comment")
    ]

    return conversationJobExtract(
        dlt.read("bronze_conversation_job"), reqcols_csat
    ).distinct()

# COMMAND ----------

@dlt.table(comment="Pipeline - Conversation Participant Attributes")
def silver_conversation_participant_attributes():
    participant_atribute = dlt.read("bronze_participant_attributes")
    participant_atribute_final = silverConversationParticipantAttributes(participant_atribute)
    return (participant_atribute_final)
