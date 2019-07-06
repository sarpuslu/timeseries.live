from intriniorealtime.client import IntrinioRealtimeClient
import pandas as pd
import kafka
from kafka import KeyedProducer
import concurrent.futures
import json
import os

kafkaIPandPort = os.environ.get("kafkaIPandPort")
intrinio_stock_key = os.environ.get("intrinio_stock_key")


#send each to quote to kafka topic
def on_quote(quote, backlog):
    print("QUOTE: ", quote, "BACKLOG LENGTH: ", backlog)
    strQuote = json.dumps(quote)
    byteQuote = strQuote.encode('utf-8')
    # prod.send_messages(topic, byteQuote)

    #sending keyed messages are in this format
    #producer.send_messages(b'my-topic', b'key1', b'some message')
    prod.send_messages(topic, quote["ticker"].encode("utf-8"), byteQuote)


#kafka connection
cluster = kafka.KafkaClient(kafkaIPandPort)
prod = KeyedProducer(cluster)
topic = "stock_topic"

#get a list of tickers to listen
nasdaq_tickers_df = pd.read_csv("nasdaq_company_list.csv")
nasdaq_ticker_list = list(nasdaq_tickers_df["Symbol"][1:2500])
api_tickers = [ticker + ".NB" for ticker in nasdaq_ticker_list]

#single thread isn't enough to handle all stocks. Backlog grows large.
#multi-threading with 1000 workers solves this issue
with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
        executor.map(on_quote)

#intrinio connection
options = {
    'api_key': intrinio_stock_key,
    'provider': 'quodd',
    'on_quote': on_quote
}
client = IntrinioRealtimeClient(options)
client.join(api_tickers)
client.connect()
client.keep_alive()
