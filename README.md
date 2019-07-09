# Stock Alert - Detectimng Anomalies in High Frequency Time Series Data
Anomaly detection on high frequency time series data

## Motivation
In 2018, 55 percent of adults in the United States invested in the stock market. There are thousands of companies in the market and all sorts idiosyncratic events takes place that can have dramatic impact on individual stock prices in a short period of time. This is a significant risk for stockholders and ideally they should be notified as soon as possible when such events occur such that they have the option to protect themselves by selling the stocks before the full impact is observed.

Consider this example below where this pharmaceutical company lost almost half of it's value within a day. An extreme event of this magnitude is likely to be composed of consectutive price changes that is unlike of what has been observed before hand. So to detect an anomalous event like this one should look for consecutive anomalous directional changes happening one after another. 


[INSERT IMAGE FROM PRESENTATION]

## Approach
My approach is to ingest real time stock prices for all the stocks in the NASDAQ exchange, transform them into stationary processes that have (roughly) normal distributions by first taking the log and differencing the time series. A  visual example of this transformation is shown below:

[INSERT TRANSFORMATION IMAGE HERE]

Prices and differences are stored into TimescaleDB. Once every day mean and standard deviation of these differences are calculated and stored into the distributions table. As new data is ingested and processed it is compared to the stored distributions. The property of the normal distribution dictates that 99.7% of the data falls within 3 standard deviations of the mean. If the incoming price changes are outside three standard deviations of the mean that difference is marked as anomalous and stored into the anomalies table.

[INSERT NORMAL DISTRIBUTION IMAGE HERE]

Prices and anomalies detected are shown on the front-end using Flask and Highcharts. 

## Assumptions
One of the big assumptions made here is that the transformed time series discussed above has a normal distribution. This assumption roughly holds true for longer time periods but it is not necessarily true especially for shorter time frames. I've noticed the distributions to be more leptokurtic, in other words it has higher peaks and fatter tails. What this implies is that unlikely events are more likely to happen that is implied by the properties of the normal distribution. In other words 99.7% of data points does not fall into 3 standard deviations within the mean. 

[INSERT AN IMAGE ABOUT LEPTOKURTIC DISTRIBUTIONS HERE]

This is easily observed by the number of anomalies detected by this system. These "more than 3 standard deviation" events happen all the time and it is marked on the price plots. 

[INSERT AN IMAGE WITH MULTIPLE ANOMALIES DETECTED WITHIN A SHORT TIME FRAME]

This is not particularly a problem as the 3 standadard deviation limit is just a threshold 

## Pipeline
Prices are ingested by Kafka, processed by Spark Streaming and stored into TimescaleDB.
[INSERT PIPELINE IMAGES HERE]

## File Structure

## Environment Setup

## Example Dataset



