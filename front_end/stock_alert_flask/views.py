from flask import render_template, jsonify, request
from stock_alert_flask import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import psycopg2
import json
import time
import datetime
import pytz
import os 
import requests


#get credentials
databaseName = os.environ.get("databaseName")
databaseIP = os.environ.get("databaseIP")
databaseUser = os.environ.get("databaseUser")
databasePassword = os.environ.get("databasePassword")

#connect to database
db = create_engine("postgresql://{}:{}@{}:5432/{}".format(databaseUser, databaseName, databaseIP, databaseName))
con = psycopg2.connect(host=databaseIP,database=databaseName, user=databaseUser, password=databasePassword)

#@app.route('/index', methods=["POST"])
@app.route('/')
def index():
    #sinceDate = request.form.get("since_date")
    #intensity = request.form.get("intensity")

    return render_template("index.html")

#list all the anomalies detected
@app.route('/anomalies', methods=["POST"])
def anomalies():
    #anomalies = db.execute("select * from anomalies").fetchall()
    anomalies = db.execute("select * from stock_anomalies").fetchall()

    sinceDate = request.form.get("since_date")    
    unixSinceTime = int(time.mktime(datetime.datetime.strptime(sinceDate, "%Y-%m-%d").timetuple())*1000)
    intensityThreshold = request.form.get("intensity")
    
    anomalies = db.execute("select * from stock_anomalies where time > {} and num_of_std_away > {}".format(unixSinceTime, intensityThreshold))
    
    return render_template("anomalies.html", anomalies=anomalies)

#bubble plot for all anomalies with the given condition
@app.route('/bubbles', methods=["POST"])
def bubbles():
    since_date = request.form.get("since_date")
    unixSinceTime = int(time.mktime(datetime.datetime.strptime(since_date, "%Y-%m-%d").timetuple())*1000)
    intensityThreshold = request.form.get("intensity")

    return render_template("bubbles.html", since_date=since_date, intensity=intensityThreshold)

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


   

    return jsonify(temp)
    
#return a list of lists of  unixtime stamps and anomaly percent changes representing anomalies
#which will be used to plot anomalies on top of price data
@app.route('/anomalies/<ticker>')
def anomalies_ticker(ticker):

    anomalies = db.execute("select * from stock_anomalies where code = '{}'".format(ticker)).fetchall()


    #list_of_anom =  [{"x": int((data_point[0]-datetime.datetime(1970,1,1,tzinfo=pytz.utc)).total_seconds()*1000), "title":"!", "text":data_point[2]} for data_point in anomalies]
    list_of_anom =  [{"x": data_point[0], "title":("+{}".format(int(data_point[3])) if data_point[2] > 0 else "-{}".format(int(data_point[3]))), "text":data_point[2]} for data_point in anomalies]


    return jsonify(list_of_anom)


@app.route('/bubble_json', methods=["GET"])
def bubble_json():

                               
    since_date = request.args.get("since_date")
    unixSinceTime = int(time.mktime(datetime.datetime.strptime(since_date, "%Y-%m-%d").timetuple())*1000)
    intensityThreshold = request.args.get("intensity")

    anomalies = db.execute("select * from stock_anomalies where time > {} and num_of_std_away > {}".format(unixSinceTime, intensityThreshold)).fetchall()
    df = pd.DataFrame(anomalies)
    #multiply the num_of_std_away column with the sign of percent_change
    df[3] = df[3]*np.sign(df[2])
    #sum to get sum of percent_change and num_of_std_away for all tickers
    df_grouped = df.groupby([df[1]]).sum()
    #count to get the number of anomalies since sinceDate and above intensityThreshold for all tickers
    df_count = df.groupby([df[1]]).count()
    #replace the sum of dates (useless) with the count of anomalies
    df_grouped[0] = df_count[2]
    #in the end df_grouped dataframe entries will be in the following form
    #ZS     4  0.867157  10.175171

    #create a dictionary for company information where entries are in the following form
    #'PRNB': ['Principia Biopharma Inc.', 'Health Care', 'Major Pharmaceuticals']
    nasdaq_companies = db.execute("select * from nasdaq_companies").fetchall()
    company_info_dict = {}
    for row in nasdaq_companies:
        company_info_dict[row[0]] = [row[1], row[2], row[3]]

    #put the aggregated anomaly information and company information together for the bubble plot    
    tickers = list(df_grouped.index.values)
    bubblePlotInput = []
    for ticker in tickers:
        grouped_row = list(df_grouped.loc[ticker,:])
        insertDict = {"name":ticker, "x": grouped_row[1], "y":grouped_row[2], "z":grouped_row[0], "company_name":company_info_dict[ticker][0], "sector":company_info_dict[ticker][1], "industry":company_info_dict[ticker][2]}
        bubblePlotInput.append(insertDict)
    
    return jsonify(bubblePlotInput)
