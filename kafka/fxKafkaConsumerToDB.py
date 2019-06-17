import kafka
import psycopg2
import json
import config

cluster = kafka.KafkaClient(config.kafkaIPandPort)
topic = "forex_topic"
consumer_group = "raw_fx_to_db"
consumer = kafka.SimpleConsumer(cluster, consumer_group, topic)

connection = psycopg2.connect(host=config.databaseIP, database=config.databaseName,
                            user=config.databaseUser, password =config.databasePassword)

cursor = connection.cursor()

# #consume all messages
# for raw in consumer:
#     msg = raw.value
#     print(msg)

# print(consumer.get_messages())

#format of messages & ###################################################################
#a message that is consumed is a OffsetAndMessage object which is in the following format:
#[OffsetAndMessage(offset=22, message=Message(crc=1009896926, magic=0, attributes=0, timestamp=None, key=None, value=b'{"time": "2019-06-12 17:49:32.177Z", "code": "EUR/USD", "bid_price": 1.12923, "ask_price": 1.12944}'))]
#get the message which is a byte and decode it into a string
strMessage = consumer.get_messages(1)[0].message.value.decode("utf-8")
#turns the string message into a dictionary
jsonMessage = json.loads(strMessage)

print(jsonMessage["time"])
print('"{}"'.format(jsonMessage["code"]))
print(jsonMessage["bid_price"])
print(jsonMessage["ask_price"])
#########################################################################################

# inserting a single message into database: ###############################################
cursor.execute("insert into forex_raw_prices (time, currency_pair, bid_price, ask_price) values ({time}, {currency_pair}, {bid_price}, {ask_price});".format(time="'{}'".format(jsonMessage["time"]), currency_pair="'{}'".format(jsonMessage["code"]), bid_price=jsonMessage["bid_price"], ask_price=jsonMessage["ask_price"]))
# cursor.execute("select * from forex_raw_prices;")
connection.commit()
##########################################################################################

# (jsonMessage["time"], jsonMessage["code"], jsonMessage["bid_price"], jsonMessage["ask_price"])



# #consume messages and store into timeseries database #########################################
# for rawMessage in consumer:
#     strMessage =  consumer.get_messages(1)[0].message.value.decode("utf-8")
#     print(strMessage)
#     jsonMessage = json.loads(strMessage)
#     cursor.execute("insert into forex_raw_prices (time, currency_pair, bid_price, ask_price) values ({time}, {currency_pair}, {bid_price}, {ask_price});".format(time="'{}'".format(jsonMessage["time"]), currency_pair="'{}'".format(jsonMessage["code"]), bid_price=jsonMessage["bid_price"], ask_price=jsonMessage["ask_price"]))
#     connection.commit()
#
#
# ###########################################################################################
