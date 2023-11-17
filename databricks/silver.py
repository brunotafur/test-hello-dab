from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from utility import *
import dlt
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
                        StructField("Whisper ", StringType(), True),
                        StructField("scriptId", StringType(), True),
                        StructField("CallType ", StringType(), True),
                        StructField("CommercialType", StringType(), True),
                        StructField("ClientBrand", StringType(), True),
                        StructField("DNIS Name", StringType(), True),
                        StructField("Priority ", StringType(), True),
                        StructField("Location", StringType(), True),
                        StructField("HoldMusic", StringType(), True),
                        StructField("HolidayMsg", StringType(), True),
                        StructField("WelcomeMsg", StringType(), True),
                        StructField("CustomerANI", StringType(), True),
                        StructField("QueueName", StringType(), True),
                        StructField("SalesType", StringType(), True),
                        StructField("TargetType", StringType(), True),
                        StructField("CountryCode", StringType(), True),
                        StructField("Flow Status", StringType(), True),
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
                        StructField("Survey", StringType(), True),
                        StructField("Welcome Message", StringType(), True),
                        StructField("Deflection Status", StringType(), True),
                        StructField("Deflection Message", StringType(), True),
                        StructField("ExternalXferNumber ", StringType(), True),
                        StructField("Routing_Decision_DT", StringType(), True),
                        StructField("Albania Xfer Message", StringType(), True),
                        StructField("External Xfer Number", StringType(), True),
                        StructField("CallRecording Message", StringType(), True),
                        StructField("Information Message 2", StringType(), True),
                        StructField("Informational Message", StringType(), True),
                        StructField("Emergency Announcement", StringType(), True),
                        StructField("Tirana Emergency Check", StringType(), True),
                        StructField("varRoutingDecisionName", StringType(), True),
                        StructField("Queue Deflection Status", StringType(), True),
                        StructField("Queue_Deflection_Status", StringType(), True),
                        StructField("Queue Deflection Message", StringType(), True),
                        StructField("Deflection Schedule Group", StringType(), True),
                        StructField("Log_LegWorkflowStart", StringType(), True),
                        StructField("Log_ConversationCheck", StringType(), True),
                        StructField("Log_LegWorkflowComplete", StringType(), True),
                        StructField("Log_SurveyWorkflowStartTime", StringType(), True),
                        StructField(
                            "Queue Deflection Schedule Group", StringType(), True
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
        "sessionIds"
    ]

    pa = df.withColumn(
        "participant_data_struct",
        F.from_json(F.col("participantData"), ArrayType(json_schema)),
    )
    pa_final = replaceUnsupportedColumnNames(flatten(pa)).drop(*columns_to_drop).distinct()

    return pa_final

def silverConversationParticipantSnapshot(conversations: DataFrame, part_attributes: DataFrame) -> DataFrame:
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
        "Planning_Unit"    
    ]

    conversations_divsions = (conversations.withColumn("primary_division_id", F.col("divisionIds")[0]))
    pa = participantAttributesExtract(part_attributes)
    legid_lookup = (pa
                    .select("LegId", "participantId")
                    .withColumnRenamed("participantId", "participant_Id")
                    .withColumnRenamed("LegId", "Leg_Id")
                    .filter(F.col("Leg_Id").isNotNull())
                    .distinct()
                        )
    p_attrubutes_lookup = (
        pa
        .withColumnRenamed("conversationId", "convId")
        .groupBy("convId")
        .agg(F.max("CountryCode").alias("Country_Code"),
             F.max("ClientBrand").alias("Client_Brand"),
             F.max("CallType").alias("Call_Type"),
             F.max("CommercialType").alias("Commercial_Type"),
             F.max("PlanningUnit").alias("Planning_Unit"),
             F.max("NTLogin").alias("NTLOGIN"))
        .distinct()
        )
    p_attributes = (participantAttributesExtract(part_attributes).withColumnRenamed("LegId", "Leg_Id"))
    
    conversation_job_flattened = (
        flatten(conversations_divsions)
        .withColumnRenamed("ivr.Priority", "ivr_Priority")
        .withColumnRenamed("ivr.Skills", "ivr_Skills")
        .drop(*cols_to_drop_after_explode)
        .distinct()
    )

    conversation_part = (conversation_job_flattened
                    .join(legid_lookup, 
                          conversation_job_flattened.participantId == legid_lookup.participant_Id,"leftouter")
                    .join(p_attrubutes_lookup, 
                          conversation_job_flattened.conversationId == p_attrubutes_lookup.convId,"leftouter")
                    .drop("participantId", "participant_Id","LegId", "convId")
                    .distinct())

    Conversation_jobs = (
        conversation_part.withColumn("conversationStart",
                                     F.to_timestamp(F.col("conversationStart"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        )
        .withColumn("conversationEnd", F.to_timestamp(F.col("conversationEnd"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
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
    #get timezone from divisions table
    divisions = (divisions.select("id", "name", F.substring(F.col("name"), -2, 2).alias("Timezone")).distinct())
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
        "primary_division_id"
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
        "Conversation_End_Key")
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
        F.max("Disconnect_Type").alias("Disconnect_Type"),
        F.max("Offered").alias("Offered"),
        F.max("Over_SLA").alias("Over_SLA"),
        F.max("NTLOGIN").alias("NTLOGIN"),
        F.max("destinationAddresses").alias("destinationAddresses"),
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
                .join(divisions, conversation_leg_attributes.primary_division_id == divisions.id, "left")
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
            .withColumn("Previous_Contact_DateTime", (F.when(F.col("ANI").isNotNull(), F.lag("segmentStart", 1).over(ani_window_spec))
                                                      .otherwise(F.when(F.col("DDI").isNotNull(), F.lag("segmentStart", 1).over(ddi_window_spec)
                                                                        ).otherwise(None)))
            )
            .withColumn(
                "Delta_Contact_DateTime",
                F.col("segmentStart").cast("long")
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
        F.when(F.col("LegId").isNull(), F.concat(F.col("conversationId"), F.lit("-1"))).otherwise(F.col("LegId")),)
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