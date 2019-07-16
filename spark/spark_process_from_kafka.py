from __future__ import print_function
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import DataFrameReader
from pyspark.sql.types import FloatType, BooleanType, IntegerType
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, StructField, LongType

import io
import os
import sys
import json
import argparse
import time
import json
import numpy as np
import scipy.stats as stats
import math
import pandas as pd

from sqlalchemy import create_engine
import psycopg2


databaseName = os.environ.get("databaseName")
databaseIP = os.environ.get("databaseIP")
databaseUser = os.environ.get("databaseUser")
databasePassword = os.environ.get("databasePassword")
kafkaIPZooPort = os.environ.get("kafkaIPZooPort")


def diff_forex(rdd):
    if rdd.isEmpty():
        print("Forex RDD is empty")
    else:
        df = rdd.toDF()
        df = df.na.drop()
        df = df.selectExpr("_1 as time", "_2 as code", "_3 as bid_price", "_4 ask_price")
        df = df.withColumn("mid_price", (df["bid_price"] + df["ask_price"])/2)
        df = df.withColumn("bid_ask_spread", (df["ask_price"]-df["bid_price"]))
        df = df.withColumn("lagged_mid_price", func.lag(df["mid_price"]).over(Window.partitionBy("code").orderBy("time")))
        df = df.withColumn("percent_change", ((df["mid_price"] - df["lagged_mid_price"])/df["lagged_mid_price"])*100)
        df = df.withColumn("processing_time", func.current_timestamp())
       
        df = df.na.drop()
        df = df.select(["processing_time", "code", "bid_price", "ask_price", "mid_price", "bid_ask_spread", "lagged_mid_price", "percent_change"])


        addToDB(df, "all_differenced")

        detect_anomaly(df)


def diff_stocks(rdd):
    if rdd.isEmpty():
        print("Stock RDD is empty")
    else:
      
        #set schema for dataframe
        schema = StructType([
                    StructField("time", LongType()),
                    StructField("code", StringType()),
                    StructField("bid_price", FloatType()),
                    StructField("ask_price", FloatType())])
        

        df = sql_context.createDataFrame(rdd, schema)
        df = df.na.drop()
        
        df = df.withColumn("mid_price", (df["bid_price"] + df["ask_price"])/2)
        df = df.withColumn("bid_ask_spread", (df["ask_price"]-df["bid_price"]))
        df = df.withColumn("lagged_mid_price", func.lag(df["mid_price"]).over(Window.partitionBy("code").orderBy("time")))
        df = df.withColumn("percent_change", ((df["mid_price"] - df["lagged_mid_price"])/df["lagged_mid_price"])*100)

        #once differenced na appears for percent_change column. drop those na's
        df = df.na.drop()
        df.show()

        addToDB(df, "stock_prices")
        detect_anomaly(df)

def detect_anomaly(df):
    #get distinct tickers in the micro-batch dataframe
    df.createOrReplaceTempView("prices")

    #get distributions informations for all the stocks
    url = 'postgresql://{}:5432/{}'.format(databaseIP, databaseName)
    properties = {'user': databaseUser, 'password': databasePassword}
    distributions_df = DataFrameReader(sql_context).jdbc(url='jdbc:%s' % url, table='stock_distributions', properties=properties)
    distributions_df.createOrReplaceTempView("distributions")

    #calculate upper and lower limit for percent change
    anomalyFactor = 3
    distributions_df = distributions_df.withColumn("upper_limit", distributions_df["mean"] + anomalyFactor*distributions_df["stdev"])
    distributions_df = distributions_df.withColumn("lower_limit", distributions_df["mean"] - anomalyFactor*distributions_df["stdev"])

    #join the mini batch data frame that holds the prices and percent changes with the distributions table
    ta = df.alias('ta')
    tb = distributions_df.alias('tb')
    batch_join_anom = ta.join(tb, ta.code == tb.code, how="left")
    batch_join_anom = batch_join_anom.select(["ta.time", "ta.code", "percent_change", "upper_limit", "lower_limit", "stdev"])
    batch_join_anom.show()

    #keep the rows that has percent_change below the lower_limit or above the upper_limit
    #in other words keep the anomalies
    batch_join_anom = batch_join_anom.filter( (batch_join_anom["percent_change"] < batch_join_anom["lower_limit"]) | (batch_join_anom["percent_change"] > batch_join_anom["upper_limit"]) )
    #calculate how many standard deviations away from the mean this anomaly was
    batch_join_anom = batch_join_anom.withColumn("num_of_std_away", func.abs(batch_join_anom["percent_change"])/batch_join_anom["stdev"])
    batch_join_anom.show()

    #prepare to push to database anomalies table
    batch_join_anom = batch_join_anom.select(["ta.time", "ta.code", "percent_change", "num_of_std_away"])

    addToDB(batch_join_anom, "stock_anomalies")


#stores the dataframe into the given table in the database
def addToDB(df, tableNameOnDB):
    df = df.toPandas()
    engine = create_engine("postgresql://{}:{}@{}:5432/{}".format(databaseUser, databaseName, databaseIP, databaseName))
    df.head(0).to_sql(tableNameOnDB, engine, if_exists='append',index=False) #truncates the table
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, tableNameOnDB, null="") # null values become ''
    conn.commit()



#setting spark connection to kafka and configs
sc = SparkContext(appName="sparkStreamConsumerFromKafkaForex").getOrCreate()
sc.setLogLevel("WARN")
#second argument is the micro-batch duration
ssc = StreamingContext(sc, 10)
spark = SparkSession(sc)
#spark.conf.set("spark.sql.session.timeZone", "EST")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
broker = kafkaIPZooPort
sql_context = SQLContext(sc, spark)


#processing forex data coming from kafka #########################
input_from_forex_topic = KafkaUtils.createStream(ssc, kafkaIPZooPort, "spark-consumer", {"forex_topic": 1})
#json loads returns a dictionary
parsed_forex = input_from_forex_topic.map(lambda v: json.loads(v[1]))
parsed_forex = parsed_forex.map(lambda w: (str(w["time"]), str(w["code"]), w["bid_price"], w["ask_price"]))
parsed_forex.pprint()
parsed_forex.foreachRDD(diff_forex)
##################################################################

#processing stock data coming from kafka #########################
input_from_stock_topic = KafkaUtils.createStream(ssc, kafkaIPZooPort, "spark-consumer", {"stock_topic": 1})
#json loads returns a dictionary
parsed_stock = input_from_stock_topic.map(lambda v: json.loads(v[1]))
#incoming stock data is messy check whether the important fields are present on a given quote, if not map to a tuple of none
parsed_stock = parsed_stock.map(lambda w: (int(w["quote_time"]), w["ticker"][:-3], float(w["bid_price_4d"])/10000, float(w["ask_price_4d"])/10000) if all (key in w for key in ("ticker", "quote_time","bid_price_4d", "ask_price_4d")) else (None,None,None,None))
parsed_stock.pprint()
parsed_stock.foreachRDD(diff_stocks)
###################################################################

ssc.start()
ssc.awaitTermination()
