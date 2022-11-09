import sys
import time
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging
import re


class Source:
    spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    raw_df = spark.read.csv("s3://finaldemoproject/RawLayer/",header=True)
    def create_spark_session(self):
        spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()

    def read_s3_data(self):
        try:
            self.raw_df = self.spark.read.csv("s3://finaldemoproject/RawLayer/",header=True)
        except Exception as e:
            logging.error('Reason for the error : %s' % e)
            print("Error is {}".format(e))
            sys.exit(1)
        else:
            self.raw_df.printSchema()

class Cleansed(Source):
    
    def drop_duplicate_values(self):
        self.raw_df=self.raw_df.drop_duplicates(["host","timestamp","method"]).drop("row_id")
        self.raw_df=self.raw_df.withColumn("row_id",monotonically_increasing_id())
        self.raw_df=self.raw_df.select("row_id","host","timestamp","method","request","status_code","size","referer","user_agent")
    
    def rmv_spl_chars(self):
        # Remove any special characters in the request column(% ,- ? =)
        self.raw_df = self.raw_df.withColumn('request', regexp_replace('request', '%|-|\?=', '')) 
      
    def size_to_kb(self):
        self.raw_df = self.raw_df.withColumn('size', round(self.raw_df.size / 1024, 2))

    def remove_empty_col_with_na(self):
        self.raw_df = self.raw_df.select(
            [when(col(c).isNull(), "N/A").otherwise(col(c)).alias(c) for c in self.raw_df.columns])
        self.raw_df.show()

    def count_null_each_row(self):
        self.raw_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.raw_df.columns])

    def datetime_formatter(self):
        self.raw_df = self.raw_df.withColumn("timestamp",to_timestamp("timestamp",'dd/MMM/yyyy:hh:mm:ss'))\
                                 .withColumn("timestamp",to_timestamp("timestamp",'MMM/dd/yyyy:hh:mm:ss'))

    def referer_present(self):
        self.raw_df = self.raw_df.withColumn("referer_present_y_n",
                                      when(col("referer")=="N\A", "N") \
                                      .otherwise("Y"))
        return self.raw_df.printSchema()
    
    def store_cleansed_Data(self):
        self.raw_df.coalesce(1).write.csv("s3://finaldemoproject/CleansedLayer/", mode="overwrite",header=True)
    
    def raw_to_hive(self):
        self.raw_df.coalesce(1).write.saveAsTable('cleansedtable')
    
    def write_to_snowflake(self):
      SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
      snowflake_database="sushantdb"
      snowflake_schema="public"
      target_table_name="curatednew"
      snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "sushantsangle",
        "sfPassword": "Stanford@01",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
      spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
      df = spark.read\
        .format('csv').load('s3://finaldemoproject/CleansedLayer/',header=True)
      df1 = df.select("*")
      df1.coalesce(1).write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "cleansedtable")\
        .option("header","true")\
        .mode("overwrite")\
        .save()
    
if __name__ == "__main__":
   
    source = Source()
    source.read_s3_data()
    cleansed = Cleansed()    
    
    try:
        cleansed.drop_duplicate_values()
    except Exception as e:
        logging.error('Error at %s', 'drop_duplicate_values', exc_info=e)
        sys.exit(1)
    
    try:
        cleansed.rmv_spl_chars()
    except Exception as e:
        logging.error('Error at %s', 'remove_special_character', exc_info=e)
        sys.exit(1)

    try:
        cleansed.size_to_kb()
    except Exception as e:
        logging.error('Error at %s', 'size_to_kb', exc_info=e)
        sys.exit(1)

    try:
        cleansed.remove_empty_col_with_na()
    except Exception as e:
        logging.error('Error at %s', 'remove empty string with null', exc_info=e)
        sys.exit(1)

    try:
        cleansed.count_null_each_row()
    except Exception as e:
        logging.error('Error at %s', 'count null each column', exc_info=e)
        sys.exit(1)
    
    try:
        cleansed.datetime_formatter()
    except Exception as e:
        logging.error('Error at %s', 'Error at datetime formatter', exc_info=e)
        sys.exit(1)

    try:
        cleansed.referer_present()
    except Exception as e:
        logging.error('Error at %s', 'Error at referer present', exc_info=e)
        sys.exit(1)
        
    try:
        cleansed.store_cleansed_Data()
    except Exception as e:
        logging.error('Error occured while writing the data in S3', e)
        sys.exit(1) 
        
    try:
        cleansed.raw_to_hive()
    except Exception as e:
        logging.error('Error occured while writing the data in HIVE', e)
        sys.exit(1)
        
    try:
        cleansed.write_to_snowflake()
    except Exception as e:
        logging.error('Error occured while writing to snowflake',e)
        sys.exit(1)