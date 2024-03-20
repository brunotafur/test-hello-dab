# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from datetime import datetime, timedelta
import dlt

# COMMAND ----------

def goldInboundConversationLeg(silver_inbound_legs, columns_to_group):
    """
    Gold Transformation definition used to get aggregates of relevant informations from silver inbound converstaion legs
    for goldInboundConversationLeg and gold_inbound_conversation_leg_interval. The parameter columns_to_group list will be the difference between goldInboundConversationLeg and gold_inbound_conversation_leg_interval.

    List of columns to group by is different for both.
    """
    # columns for which the aggregate to be taken for
    columns_to_aggregate = [
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
        "Not_Responding_Time",
        "User_Response_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Consult",
        "Callback_Request",
        "Callback_Handled",
        "Callback_No_Answer",
        "Callback_Abandoned",
        "Pre_Request_Wait_Time",
        "Callback_Wait_Time",
    ]
    time_format = (
        "yyyy-MM-dd HH:mm:ss:SSS"  # time formar used to get 15 minutes interval
    )
    # As we need to create a 15 minutes time interval based on the leg start time, a dummy date is added to start time(As start time is just time, not time timestamp). A timestamp is created, from that the interval is derive
    s_inbound = (
        silver_inbound_legs.withColumn(
            "dummy_date",
            F.concat(F.lit("1970-01-01"), F.lit(" "), F.col("Leg_Start_Time")),
        )
        .withColumn("dummy_timestamp", F.to_timestamp("dummy_date", time_format))
        .withColumn(
            "unix_time",
            F.unix_timestamp((F.col("dummy_timestamp")), format=time_format),
        )
        .withColumn(
            "quarter_interval",
            (F.expr("cast(unix_time as long) div 900") * 900).cast("timestamp"),
        )
        .withColumn("Time_Interval", F.date_format("quarter_interval", "HH:mm"))
        .withColumn("Employee_Key", F.lit("UNKNOWN"))
        .withColumn("Organisation", F.lit("UNKNOWN"))
    )
    final = (
        s_inbound.groupBy(*columns_to_group)
        .agg(*[F.sum(col_name).alias(col_name) for col_name in columns_to_aggregate])
        .withColumnRenamed("Conversation_Date", "Call_Date")
        .withColumnRenamed("Pre_Request_Wait_Time", "Callback_Pre_Request_Wait_Time")
        .withColumnRenamed("Callback_Wait_Time", "Callback_Attempt_Wait_Time")
    )
    return final

# COMMAND ----------

def goldInboundConversationLegAggregate(
    silver_inbound_legs, columns_to_group, columns_to_aggregate
):

    """
    Gold Transformation definition used to get aggregates of relevant informations from silver inbound converstaion legs
    for goldInboundConversationLeg and gold_inbound_conversation_leg_interval. The parameter columns_to_group list will be the difference between goldInboundConversationLeg and gold_inbound_conversation_leg_interval.

    List of columns to group by is different for both.
    """
    # columns for which the aggregate to be taken for

    time_format = (
        "yyyy-MM-dd HH:mm:ss:SSS"  # time formar used to get 15 minutes interval
    )

    # As we need to create a 15 minutes time interval based on the leg start time, a dummy date is added to start time(As start time is just time, not time timestamp). A timestamp is created, from that the interval is derived
    s_inbound = (
        silver_inbound_legs.withColumn(
            "dummy_date",
            F.concat(F.lit("1970-01-01"), F.lit(" "), F.col("Leg_Start_Time")),
        )
        .withColumn("dummy_timestamp", F.to_timestamp("dummy_date", time_format))
        .withColumn(
            "unix_time",
            F.unix_timestamp((F.col("dummy_timestamp")), format=time_format),
        )
        .withColumn(
            "quarter_interval",
            (F.expr("cast(unix_time as long) div 900") * 900).cast("timestamp"),
        )
        .withColumn("Time_Interval", F.date_format("quarter_interval", "HH:mm"))
        .withColumn("Employee_Key", F.lit("UNKNOWN"))
        .withColumn("Organisation", F.lit("UNKNOWN"))
    )
    final = (
        s_inbound.groupBy(*columns_to_group)
        .agg(*[F.sum(col_name).alias(col_name) for col_name in columns_to_aggregate])
        .withColumnRenamed("Conversation_Date", "Call_Date")
        .withColumnRenamed("Pre_Request_Wait_Time", "Callback_Pre_Request_Wait_Time")
        .withColumnRenamed("Callback_Wait_Time", "Callback_Attempt_Wait_Time")
    )
    return final

# COMMAND ----------

def goldOutboundConversationLeg(silver_outbound_legs):
    """
    Gold aggregate function from silver outbound legs

    """
    columns_to_group = [
        "Conversation_Date",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "Employee_Key",
        "Disconnect_Type",
        "Transfer_Queue",
        "Transfer_DDI",
    ]
    columns_to_aggregate = [
        "Talk_Time",
        "Held_Time",
        "ACW_Time",
        "Handle_Time",
        "Dialing_Time",
        "Contacting_Time",
        "ACD_OB_Attempt",
        "ACD_OB_Connected",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Consult",
    ]
    s_outbound = silver_outbound_legs.withColumn("Employee_Key", F.lit("UNKNOWN"))
    final = (
        s_outbound.groupBy(*columns_to_group)
        .agg(*[F.sum(col_name).alias(col_name) for col_name in columns_to_aggregate])
        .withColumnRenamed("Conversation_Date", "Call_Date")
    )
    return final

# COMMAND ----------

# DBTITLE 1,Gold Inbound Conversation Leg
@dlt.table(comment="Pipeline - gold_inbound_conversation_leg")
def gold_inbound_conversation_leg():

    s_inbound = dlt.read("silver_inbound_conversation_leg")  # silver inbound data

    cols_agent_to_group = [
        "Conversation_Date",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "DNIS",
        "Country_Code",
        "Client_Brand",
        "Commercial_Type",
        "Call_Type",
        "Planning_Unit",
        "Queue",
        "Employee_Key",
        "Disconnect_Type",
        "Transfer_Queue",
        "Transfer_DDI",
    ]

    cols_to_aggregate = [
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
        "Not_Responding_Time",
        "User_Response_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Consult",
        "Callback_Request",
        "Callback_Handled",
        "Callback_No_Answer",
        "Callback_Abandoned",
        "Pre_Request_Wait_Time",
        "Callback_Wait_Time",
    ]

    return goldInboundConversationLegAggregate(
        s_inbound, cols_agent_to_group, cols_to_aggregate
    )

# COMMAND ----------

@dlt.table(comment="Pipeline - gold_inbound_conversation_leg_interval")
def gold_inbound_conversation_leg_interval():

    s_inbound = dlt.read("silver_inbound_conversation_leg")  # silver inbound data
    g_intraday_target = dlt.read("gold_intraday_eight_day")

    cols_com_plan_to_group = [
        "Conversation_Date",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "Timezone",
        "Time_Interval",
        "DNIS",
        "Country_Code",
        "Client_Brand",
        "Commercial_Type",
        "Call_Type",
        "Planning_Unit",
        "Queue",
        "Skill",
        "Organisation",
        "Transfer_Queue",
        "Transfer_DDI",
    ]

    cols_to_aggregate = [
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
        "Not_Responding_Time",
        "User_Response_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Consult",
        "Callback_Request",
        "Callback_Handled",
        "Callback_No_Answer",
        "Callback_Abandoned",
        "Pre_Request_Wait_Time",
        "Callback_Wait_Time",
    ]
    conv_intraday = goldInboundConversationLegAggregate(
        s_inbound, cols_com_plan_to_group, cols_to_aggregate
    )

    return(g_intraday_target
    .merge(conv_intraday, "conv_intraday.Conversation_Id = g_intraday_target.Conversation_Id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
    )


# COMMAND ----------

@dlt.table(comment="Pipeline - gold_acd_outbound_conversation_leg ")
def gold_acd_outbound_conversation_leg():

    s_outbound = dlt.read("silver_outbound_conversation_leg")  # silver inbound data

    return goldOutboundConversationLeg(s_outbound)

# COMMAND ----------

def goldIntradayAggregate(silver_inbound_legs, current_date, interval):

    crnt_date = datetime.strptime(f"{current_date}", "%Y-%m-%d").date()
    date_interval = crnt_date - timedelta(days=interval)
    s_inbound_intraday = silver_inbound_legs.filter(
        F.col("Conversation_Date") >= date_interval
    )

    print(f"Data ingestion from {crnt_date} to {date_interval} completed")

    cols_to_group = [
        "Conversation_Date",
        "Transfer_Leg",
        "Leg_Ordinal",
        "Alternate_Leg_Flag",
        "Timezone",
        "Time_Interval",
        "DNIS",
        "Country_Code",
        "Client_Brand",
        "Commercial_Type",
        "Call_Type",
        "Planning_Unit",
        "Queue",
        "Employee_Key",
        "Disconnect_Type",
        "Transfer_Queue",
        "Transfer_DDI",
    ]

    cols_to_aggregate = [
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
        "Not_Responding_Time",
        "User_Response_Time",
        "Voice_Mail_Time",
        "Transferred",
        "Transferred_Blind",
        "Transferred_Consult",
        "Consult",
        "Callback_Request",
        "Callback_Handled",
        "Callback_No_Answer",
        "Callback_Abandoned",
        "Pre_Request_Wait_Time",
        "Callback_Wait_Time",
    ]

    return goldInboundConversationLegAggregate(
        s_inbound_intraday, cols_to_group, cols_to_aggregate
    )

# COMMAND ----------

@dlt.table(comment="Pipeline - gold_intraday_eight_day ")
def gold_intraday_eight_day():

    s_outbound = dlt.read("silver_inbound_conversation_leg")#silver inbound data
    
    return goldIntradayAggregate(s_outbound, "2023-09-21",1)
