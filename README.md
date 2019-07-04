# timeseries.live
Anomaly detection on high frequency time series data

## Motivation
In 2018, 55 percent of adults in the United States invested in the stock market. There are thousands of companies in the market and all sorts idiosyncratic events takes place that can have dramatic impact on individual stock prices in a short period of time. This is a significant risk for stockholders and ideally they should be notified as soon as possible when such events occur such that they have the option to protect themselves by selling the stocks before the full impact is observed.

## Approach
My approach is to ingest real time stock prices for all the stocks in the NASDAQ exchange, transform them into stationary processes that have (roughly) normal distributions. Information about the distribution of every stock is stored in the database and updated hourly. Then incoming ticks are compared to the stored distributions and marked "anomalous" if they are larger than three standard deviations around the mean. Stock prices and anomalies that are detected are shown in the front-end using Flask and HighStocks charting framework.

## Pipeline
Prices are ingested by Kafka, processed by Spark Streaming and stored into TimescaleDB.

