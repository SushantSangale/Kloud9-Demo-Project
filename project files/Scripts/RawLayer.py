import sys
import time
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging
import re


class Pipeline:
    spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    raw_df = spark.read.text("s3://finaldemoproject/Sourcedata/new_log.txt")
    def create_spark_session(self):
        self.spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()

    def read_s3_data(self):
        try:
            self.raw_df = self.spark.read.text("s3://finaldemoproject/Sourcedata/new_log.txt")
        except Exception as e:
            logging.error('Reason for the error : %s' % e)
            print("Error is {}".format(e))
            sys.exit(1)
        else:
            self.raw_df.printSchema()

class Rawdata(Pipeline):
    
    def extract_columns_regex(self,raw_df):
        host_pattern = r'\d+\.[\d+\.]+\d+\s'
        ts_pattern = r'\d{2}\/\w{3}\/\d{4}\:\d{2}\:\d{2}\:\d{2}'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        content_size_pattern = r'(\d{3})\ (\d+)'
        referer_pattern = r'("https\S+")'
        useragent_pattern = r'((\")(\w+\/\d.+\)))'

        self.logs_df = Pipeline.raw_df.withColumn("id", monotonically_increasing_id()) \
            .select("id", regexp_extract('value', host_pattern, 0).alias('host'),
                    regexp_extract('value', ts_pattern, 0).alias('timestamp'),
                    regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                    regexp_extract('value', method_uri_protocol_pattern, 2).alias('request'),
                    regexp_extract('value', content_size_pattern, 1).alias('status_code'),
                    regexp_extract('value', content_size_pattern, 2).alias('size'),
                    regexp_extract('value', referer_pattern, 1).alias('referer'),
                    regexp_extract('value', useragent_pattern, 3).alias('user_agent'))
        self.logs_df.show()
        return self.logs_df
        
    def store_raw_Data(self,logs_df):
        logs_df.coalesce(1).write.csv("s3://finaldemoproject/RawLayer/", mode="overwrite",header=True)
    
    def raw_to_hive(self,logs_df):
        logs_df.coalesce(1).write.saveAsTable('rawtable')
        
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
        .format('csv').load('s3://finaldemoproject/RawLayer/',header=True)
      df1 = df.select("*")
      df1.coalesce(1).write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "rawtable")\
        .option("header","true")\
        .mode("overwrite")\
        .save()
        
if __name__ == '__main__':
    pipeline = Pipeline()
    try:
        pipeline.read_s3_data()
    except Exception as e:
        logging.error('Error occured while reading data from S3', e)
        sys.exit(1)
#     pipeline.create_spark_session()
    temp_df= pipeline.read_s3_data()
    rawdata=Rawdata()
    df=rawdata.extract_columns_regex(temp_df)
    
    try:
        rawdata.store_raw_Data(df)
    except Exception as e:
        logging.error('Error occured while writing the data in S3', e)
        sys.exit(1) 
        
    try:
        rawdata.raw_to_hive(df)
    except Exception as e:
        logging.error('Error occured while writing the data in HIVE', e)
        sys.exit(1)
    
    try:
        rawdata.write_to_snowflake()
    except Exception as e:
        logging.error('Error occured while writing to snowflake',e)
        sys.exit(1)
        
#     temp_df= pipeline.read_s3_data()
#     rawdata=Rawdata()
#     df=rawdata.extract_columns_regex(temp_df)
#     rawdata.raw_to_hive(df)