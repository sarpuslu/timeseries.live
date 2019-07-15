import psycopg2
import numpy as np
import pandas as pd
from scipy import stats
import math
import datetime
import os 


#connect to database
databaseName = os.environ.get("databaseName")
databaseIP = os.environ.get("databaseIP")
databaseUser = os.environ.get("databaseUser")
databasePassword = os.environ.get("databasePassword")
conn = psycopg2.connect(host=databaseIP,database=databaseName,user=databaseUser,password=databasePassword)
cur = conn.cursor()


#get all the tickers which price data is present
cur.execute("select distinct code from all_differenced")
dbResponse = cur.fetchall()
distinct_codes = [row[0] for row in dbResponse]

#delete all rows in the distributions table
#distributions table is updated hourly  with new values
cur.execute("delete from distributions")

#recalculate mean and stdev for all stocks
distribution_db_insert = []
for code in distinct_codes:
    singleInsert = []
    singleInsert.append(code)
    cur.execute("select percent_change from stock_prices where code='{}'".format(code))
    dbResponse = cur.fetchall()
    diffs = [row[0] for row in dbResponse]

    #you need at least 30 datapoints to make get meaningful statistics
    if (len(diffs) < 30):
        continue

    #calculate mean and stdev for all stocks
    stockMean = stats.describe(diffs).mean
    stockStdev = math.sqrt(stats.describe(diffs).variance)

    if isinstance(stockMean, float):
        singleInsert.append(stockMean)
    else:
        singleInsert.append(None)
    if isinstance(stockStdev, float):
        singleInsert.append(stockStdev)
    else:
        singleInsert.append(None)
       
    print(singleInsert)
    distribution_db_insert.append(singleInsert)


    #insert new mean and stdev into database
    cur.execute("insert into stock_distributions (time, code, mean, stdev) values ({time}, {code}, {mean}, {stdev});".format(time="'{}'".format(datetime.datetime.now()), 
                                           code="'{}'".format(singleInsert[0]), 
                                           mean=singleInsert[1], 
                                           stdev=singleInsert[2]))
conn.commit()


cur.close()
conn.close()
