#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
import config

sc = SparkContext(appName="sparkStreamConsumerFromKafkaForex")
sc.setLogLevel("WARN")

#second argument is the micro-batch duration
ssc = StreamingContext(sc, 60)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream(config.kafkaIP, 2181)
lines.pprint()
# print(type(lines))
# print(json.loads(lines))
#kafkastream = KafkaUtils.createStream(ssc, '3.216.128.178:2181', 'spark-streaming', {'twitter':1})
#parsed = kafkaStream.map(lambda v: json.loads(v[1]))

inputFromKafka = KafkaUtils.createStream(ssc,config.kafkaIPZooPort, "spark-consumer", {"forex_topic": 1})
#inputFromKafka = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": broker})
#print(inputFromKafka)

#inputFromKafka.pprint()
parsed = inputFromKafka.map(lambda v: json.loads(v[1]))
parsed.pprint()

ssc.start()
ssc.awaitTermination()
#ssc.stop()
# print(type(lines))
# print(json.loads(lines))


#kafkastream = KafkaUtils.createStream(ssc, '3.216.128.178:2181', 'spark-streaming', {'twitter':1})
#parsed = kafkaStream.map(lambda v: json.loads(v[1]))
