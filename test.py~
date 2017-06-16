import sys
from pykafka import KafkaClient
from pykafka.partitioners import HashingPartitioner
from pykafka.partitioners import BasePartitioner
import numpy as np
import time


hash_partitioner = HashingPartitioner()


#client   = KafkaClient("172.31.53.147:9092") # , socket_timeout_ms=1000
client   = KafkaClient(hosts="172.31.55.173:9092,172.31.53.162:9092,172.31.56.220:9092",zookeeper_hosts="172.31.53.147:2181")
topic    = client.topics["jtest30b"]
#producer = topic.get_producer(partitioner=hash_partitioner, linger_ms = 200)

totallogs = 0
times = time.time()
ids= np.arange(30)
with topic.get_producer(partitioner=hash_partitioner, linger_ms = 200) as producer:
	while True:
		#time.sleep((eventTime-eventTimeo)) # sleep time in miliseconds
		currID = np.random.randint(30, size=1)
		#outputStr = "%s;%s" % (currID, eventTime+dtime)
                outputStr = "%s;%s" % (currID, np.int64(1))
		producer.produce(outputStr, partition_key=str(currID))

print time.time()-times

