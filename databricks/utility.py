import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import *

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