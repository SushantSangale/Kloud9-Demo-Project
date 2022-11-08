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
    curated_df = spark.read.csv("s3://finaldemoproject/CleansedLayer/",header=True)
    def create_spark_session(self):
        spark=SparkSession.builder\
        .appName("Demo_Project").enableHiveSupport().getOrCreate()

    def read_s3_data(self):
        try:
            self.curated_df = self.spark.read.csv("s3://finaldemoproject/CleansedLayer/",header=True,inferSchema=True)
        except Exception as e:
            logging.error('Reason for the error : %s' % e)
            print("Error is {}".format(e))
            sys.exit(1)
        else:
            self.curated_df.printSchema()
    
class Curated(Source):

    def drop_referer(self):
      self.curated_df = self.curated_df.drop("referer")
      self.curated_df.show()
      return self.curated_df

    def write_to_s3(self):
      self.curated_df.coalesce(1).write.csv("s3://finaldemoproject/CurateLayer/", mode="overwrite", header=True)
      
    def curated_to_hive(self):
        self.curated_df.coalesce(1).write.saveAsTable('curatedtable')
    
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
        .format('csv').load('s3://finaldemoproject/CurateLayer/',header=True)
      df1 = df.select("*")
      df1.coalesce(1).write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "curatedtable")\
        .option("header","true")\
        .mode("overwrite")\
        .save()
        
class Agg(Curated):

    # check distinct no. of user device ie ip
    def distinct_user(self):
        self.curated_df.select("host").distinct().count()
        
    def add_temp_columns(self):
        df_temp = self.curated_df.withColumn("No_get", when(col("method") == "GET", "GET")) \
        .withColumn("No_post", when(col("method") == "POST", "POST")) \
        .withColumn("No_Head", when(col("method") == "HEAD", "HEAD")) \
        .withColumn("day", to_date(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_hour", concat(col("day"), lit(" "), col("hour")))

        df_temp.show()
        return df_temp

        # perform aggregation per device
    def agg_per_device(self, df_temp):
         df_agg_per_device = df_temp.select("id", "day_hour", "host", "no_get", "no_post", "no_head") \
        .groupBy("day_hour", "host") \
        .agg(count("id").alias("row_id"),
             count(col("No_get")).alias("no_get"),
             count(col("No_post")).alias("no_post"),
             count(col("No_head")).alias("no_head")) \
        # .orderBy(col("row_id").desc())

         df_agg_per_device.show()
         return df_agg_per_device
      
    def agg_across_device(self,df_temp):
         df_agg_across_device = df_temp.select("*") \
                                       .groupBy("day_hour") \
                                       .agg(
                                        count("host").alias("no_of_clients"),
                                        count("id").alias("row_id"),
                                        count(col("No_get")).alias("no_get"),
                                        count(col("No_post")).alias("no_post"),
                                        count(col("No_head")).alias("no_head"))
                                       
         df_agg_across_device.show()
         return df_agg_across_device

    def write_to_s3_perdevice(self,df):
        df.coalesce(1).write.csv("s3://finaldemoproject/Aggregation/perdevice/", mode="overwrite", header=True)
    
    def perdevice_to_hive(self,df):
        df.coalesce(1).write.saveAsTable('perdevicetable')    
    
    def write_to_snowflake_perdevice(self):
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
        .format('csv').load('s3://finaldemoproject/Aggregation/perdevice/',header=True)
      df1 = df.select("*")
      df1.coalesce(1).write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "perdevice")\
        .option("header","true")\
        .mode("overwrite")\
        .save()
    
    def write_to_s3_acrossdevice(self,df):
        df.coalesce(1).write.csv("s3://finaldemoproject/Aggregation/acrossdevice/", mode="overwrite", header=True)
    
    def across_device_to_hive(self,df):
        df.coalesce(1).write.saveAsTable('acrossdevicetable') 
    
    def write_to_s3_snowflake_acrossdevice(self):
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
        .format('csv').load('s3://finaldemoproject/Aggregation/acrossdevice/',header=True)
      df1 = df.select("*")
      df1.coalesce(1).write.format("snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", "acrossdevicetable")\
        .option("header","true")\
        .mode("overwrite")\
        .save()
 
if __name__ == '__main__':
    try:
        source = Source()
    except Exception as e:
        logging.error('Error occured while Source Object creation', e)
        sys.exit(1)

    try:
        source.read_s3_data()
    except Exception as e:
        logging.error('Error occured while reading from s3 clean', e)
        sys.exit(1)

    try:
        curated = Curated()
    except Exception as e:
        logging.error('Error occured while curated Object creation', e)
        sys.exit(1)
    
    try:
        curated.drop_referer()
    except Exception as e:
        logging.error('Error occured while dropping referer field', e)
        sys.exit(1)

    try:
        curated.write_to_s3()
    except Exception as e:
        logging.error('Error occured while writing to s3',e)
        sys.exit(1)
    
    try:
        curated.curated_to_hive()
    except Exception as e:
        logging.error('Error occured while writing to HIVE',e)
        sys.exit(1)
    
    try:
        curated.write_to_snowflake()
    except Exception as e:
        logging.error('Error occured while writing to snowflake',e)
        sys.exit(1)
        
    # Agg
    try:
        agg = Agg()
    except Exception as e:
        logging.error('Error occured while creating agg object',e)
        sys.exit(1)

    try:
        agg.distinct_user()
    except Exception as e:
        logging.error('Error occured at distinct_user',e)
        sys.exit(1)
    
    try:
        temp_df = agg.add_temp_columns()

    except Exception as e:
        logging.error('Error occured at add_temp_columns',e)
        sys.exit(1)
    
    try:
        agg_df=agg.agg_per_device(temp_df)
    except Exception as e:
        logging.error('Error occured at device_agg',e)
        sys.exit(1)
    
    try:
        agg_across_df=agg.agg_across_device(temp_df)
    except Exception as e:
        logging.error('Error occured at device_agg',e)
        sys.exit(1)
    
    try:
        agg.write_to_s3_perdevice(agg_df)
    except Exception as e:
        logging.error('Error occured while writing to S3 per device',e)
        sys.exit(1)

    try:
        agg.write_to_s3_acrossdevice(agg_across_df)
    except Exception as e:
        logging.error('Error occured while writing to S3 across device',e)
        sys.exit(1)
        
    try:
        agg.perdevice_to_hive(agg_df)
    except Exception as e:
        logging.error('Error occured while writing to S3 per device',e)
        sys.exit(1)
        
    try:
        agg.across_device_to_hive(agg_across_df)
    except Exception as e:
        logging.error('Error occured while writing to S3 per device',e)
        sys.exit(1)
    
    try:
        agg.write_to_snowflake_perdevice()
    except Exception as e:
        logging.error('Error occured while writing to snowflake',e)
        sys.exit(1)
    
    try:
        agg.write_to_s3_snowflake_acrossdevice()
    except Exception as e:
        logging.error('Error occured while writing to snowflake',e)
        sys.exit(1)

# spark-submit --master yarn --deploy-mode client --packages net.snowflake:snowflake-jdbc:3.11.1,net.snowflake:spark-snowflake_2.11:2.5.7-spark_2.4 CuratedLayer.py