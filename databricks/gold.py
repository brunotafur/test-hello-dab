from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from utility import *

def goldInboundConversationLeg(silver_inbound_legs, columns_to_group):

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

    time_format = "yyyy-MM-dd HH:mm:ss:SSS"

    s_inbound = (silver_inbound_legs
       .withColumn("dummy_date", F.concat(F.lit("1970-01-01"),F.lit(" "), F.col("Leg_Start_Time")))
       .withColumn("dummy_timestamp", F.to_timestamp("dummy_date", time_format))
       .withColumn("unix_time", F.unix_timestamp((F.col("dummy_timestamp")), format=time_format))
       .withColumn("quarter_interval", (F.expr("cast(unix_time as long) div 900") * 900).cast("timestamp"))
       .withColumn("Time_Interval", F.date_format("quarter_interval", "HH:mm"))
       .withColumn("Employee_Key", F.lit("UNKNOWN"))
       .withColumn("Organisation", F.lit("UNKNOWN"))
             )
    
    final = (s_inbound
          .groupBy(*columns_to_group)
          .agg(*[F.sum(col_name).alias(col_name) for col_name in columns_to_aggregate])
          .withColumnRenamed("Conversation_Date", "Call_Date")
          .withColumnRenamed("Pre_Request_Wait_Time", "Callback_Pre_Request_Wait_Time")
          .withColumnRenamed("Callback_Wait_Time", "Callback_Attempt_Wait_Time")
          )
    return final
