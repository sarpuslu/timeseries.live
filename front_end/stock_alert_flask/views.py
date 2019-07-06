from flask import render_template, jsonify
from stock_alert_flask import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
import json
import datetime
import pytz
import os 

#get credentials
databaseName = os.environ.get("databaseName")
databaseIP = os.environ.get("databaseIP")
databaseUser = os.environ.get("databaseUser")
databasePassword = os.environ.get("databasePassword")

#connect to database
db = create_engine("postgresql://{}:{}@{}:5432/{}".format(databaseUser, databaseName, databaseIP, databaseName))
con = psycopg2.connect(host=databaseIP,database=databaseName, user=databaseUser, password=databasePassword)

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html",
       title = 'Home', user = { 'nickname': 'Miguel' },
       )

#list all the anomalies detected
@app.route('/anomalies')
def anomalies():
    #anomalies = db.execute("select * from anomalies").fetchall()
    anomalies = db.execute("select * from stock_anomalies").fetchall()
    return render_template("anomalies.html", anomalies=anomalies)

@app.route('/<ticker>')
def ticker(ticker):
    timeseriesdata = db.execute("select * from stock_prices where code = '{}'".format(ticker)).fetchall()
    anomalies = db.execute("select * from stock_anomalies where code = '{}'".format(ticker)).fetchall()

    return render_template("ticker.html", timeseries=timeseriesdata, anomalies=anomalies, ticker=ticker)


#return a list of lists of unixtime stamps and prices to be plotted 
#used for plotting the time series data for a given ticker
@app.route('/json/<ticker>')
def api(ticker):
    timeseriesdata = db.execute("select * from stock_prices  where code = '{}' order by time".format(ticker)).fetchall()
    
    #get unix time in miliseconds and prices
    temp =  [[data_point[0], data_point[3]] for data_point in timeseriesdata]


    #temp2 = []
    #for i in range(0,len(temp)-1):
    #    if temp[i][0] != temp[i+1][0]:
    #        temp2.append(temp[i])

    #for element in temp2:
    #    element[0] = int(element[0]*1000)


    #return json.dumps(temp2)
    return jsonify(temp)
    
#return a list of lists of  unixtime stamps and anomaly percent changes representing anomalies
#which will be used to plot anomalies on top of price data
@app.route('/anomalies/<ticker>')
def anomalies_ticker(ticker):

    anomalies = db.execute("select * from stock_anomalies where code = '{}'".format(ticker)).fetchall()


    #list_of_anom =  [{"x": int((data_point[0]-datetime.datetime(1970,1,1,tzinfo=pytz.utc)).total_seconds()*1000), "title":"!", "text":data_point[2]} for data_point in anomalies]
    list_of_anom =  [{"x": data_point[0], "title":("+{}".format(int(data_point[3])) if data_point[2] > 0 else "-{}".format(int(data_point[3]))), "text":data_point[2]} for data_point in anomalies]


    return jsonify(list_of_anom)

