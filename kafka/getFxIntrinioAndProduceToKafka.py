from intriniorealtime.client import IntrinioRealtimeClient
from kafka import KeyedProducer
import json
import kafka
import config

def on_quote(quote, backlog):
    print("QUOTE: " , quote, "BACKLOG LENGTH: ", backlog)
    # print(quote["code"])
    strQuote = json.dumps(quote)
    byteQuote = strQuote.encode('utf-8')
    # prod.send_messages(topic, byteQuote)

    #sending keyed messages are in this format
    #producer.send_messages(b'my-topic', b'key1', b'some message')
    prod.send_messages(topic, quote["code"].encode("utf-8"), byteQuote)

options = {
    'api_key': config.intrinio_api_key,
    'provider': 'fxcm',
    'on_quote': on_quote
}

cluster = kafka.KafkaClient(config.kafkaIPandPort)
# prod = kafka.SimpleProducer(cluster, async=False)
prod = KeyedProducer(cluster)

topic = "forex_topic"

client = IntrinioRealtimeClient(options)
client.join(['fxcm:pair:AUD/USD'])
# client.join(['AAPL.NB'])
client.connect()
client.keep_alive()
