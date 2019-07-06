from intriniorealtime.client import IntrinioRealtimeClient
from kafka import KeyedProducer
import json
import kafka
import os

kafkaIPandPort = os.environ.get("kafkaIPandPort")
intrinio_forex_key  = os.environ.get("intrinio_forex_key")

#send each quote to kafka topic
def on_quote(quote, backlog):
    print("QUOTE: " , quote, "BACKLOG LENGTH: ", backlog)
    strQuote = json.dumps(quote)
    byteQuote = strQuote.encode('utf-8')

    #sending keyed messages are in this format
    #producer.send_messages(b'my-topic', b'key1', b'some message')
    prod.send_messages(topic, quote["code"].encode("utf-8"), byteQuote)

#kafka connection 
cluster = kafka.KafkaClient(kafkaIPandPort)
prod = KeyedProducer(cluster)
topic = "forex_topic"

#intrinio connection 
options = {
    'api_key': intrinio_forex_key,
    'provider': 'fxcm',
    'on_quote': on_quote
}
client = IntrinioRealtimeClient(options)
client.join(['fxcm:pair:EUR/USD','fxcm:pair:USD/JPY', 'fxcm:pair:GBP/USD','fxcm:pair:USD/CHF', 'fxcm:pair:EUR/CHF', 'fxcm:pair:AUD/USD', 'fxcm:pair:USD/CAD', 'fxcm:pair:NZD/USD', 'fxcm:pair:EUR/GBP', 'fxcm:pair:EUR/JPY', 'fxcm:pair:GBP/JPY', 'fxcm:pair:CHF/JPY', 'fxcm:pair:GBP/CHF', 'fxcm:pair:EUR/JPY','fxcm:pair:EUR/AUD','fxcm:pair:EUR/CAD','fxcm:pair:AUD/CAD','fxcm:pair:AUD/JPY','fxcm:pair:CAD/JPY','fxcm:pair:NZD/JPY','fxcm:pair:GBP/CAD','fxcm:pair:GBP/NZD', 'fxcm:pair:GBP/AUD','fxcm:pair:AUD/NZD','fxcm:pair:USD/SEK', 'fxcm:pair:EUR/SEK', 'fxcm:pair:EUR/NOK',	'fxcm:pair:USD/NOK','fxcm:pair:USD/MXN','fxcm:pair:AUD/CHF','fxcm:pair:EUR/NZD','fxcm:pair:USD/ZAR', 'fxcm:pair:ZAR/JPY','fxcm:pair:USD/TRY','fxcm:pair:EUR/TRY', 'fxcm:pair:NZD/CHF','fxcm:pair:CAD/CHF', 'fxcm:pair:NZD/CAD', 'fxcm:pair:TRY/JPY'])
client.connect()
client.keep_alive()

