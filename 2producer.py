#!/usr/bin/env python
## printing to stderr in python3 style
from __future__ import print_function
import sys

import threading, logging, time

from pykafka import KafkaClient
from pykafka import partitioners
import numpy as np

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class ModPartitioner(partitioners.BasePartitioner):
    """
    Returns a the int value of the key mod the number of partitions
    """
    def __call__(self, partitions, key):
        """
        :param partitions: The partitions from which to choose
        :type partitions: sequence of :class:`pykafka.base.BasePartition`
        :param key: Key used for routing
        :type key: int
        :returns: A partition
        :rtype: :class:`pykafka.base.BasePartition`
        """
        if key is None:
            raise ValueError(
                'key cannot be `None` when using int partitioner'
            )
        partitions = sorted(partitions)  # sorting is VERY important
        return partitions[abs(int(key)) % len(partitions)]

mod_partitioner = ModPartitioner()

class Producer(threading.Thread):
  daemon = True

  def run(self):
    client = KafkaClient("localhost:9092")
    topic = client.topics["jtest"]
    producer = topic.get_producer(partitioner=mod_partitioner, linger_ms = 200)

    keyNum = 6
    key = list(np.arange(keyNum))

    idNum = 10000
    id = list(np.arange(idNum))

    ratepersecond = 5000
    burstFraction=0.1
    burstnum = ratepersecond*burstFraction

    ## Gaussian parameters for variation of message rate
    gmu = 0.0
    gsig = 0.1

    ## Lognormal parameters for delay of message delivery
    lmu = 2.5
    lsig = 1.0

    global totalSamples

    while True:
      ## emit data in bursts, compute number of messages in this burst
      ## from target rate
      currNum = int(round( burstnum*(1 + np.random.normal(gmu, gsig)) ))
      tt = time.time()
      totalSamples += currNum

      tw = 21*60. # The time window in seconds
      #visitg = [1,2,10,100]
      visitg = [1, 2, 10, 100]
      nvfv = {'1'   :[0       , int(1e6), int(1e3)], # [0,   1e6): (forgetters)
            '2'   :[int(1e6), int(2e6), int(1e3)], # [1e6, 2e6): (login-logout) 
            '10'  :[int(2e6), int(3e6), int(1e3)], # [2e6, 3e6): (active-user)
            '100' :[int(3e6), int(4e6), int(5)]} # [3e6, 4e6): (machine-spam)

      nv = 0 # Initiate the number of the visits to the website during the time window
      ld = []
      for v in visitg:
	      nv += v*nvfv[str(v)][2]
	      ld.append(np.repeat(np.random.randint(nvfv[str(v)][0], nvfv[str(v)][1], size=nvfv[str(v)][2]), v)) 
      ld1 = np.concatenate(ld[:])
      np.random.shuffle(ld1)
      dt = tw/len(ld1)
      times = time.time()
      for index, item in enumerate(ld1):
        currKey = key[np.random.randint(keyNum)]
        eventTime = dt*(index+np.random.random_sample())
        currID = item
	outputStr = "%s;%s" % (currKey, int(currID))
        producer.produce(outputStr, partition_key=str(currKey))
      #if total
    #  for sample in xrange(currNum):
    #    delay = np.random.lognormal(lmu, lsig)
    #    currKey = key[np.random.randint(keyNum)]
    #    currID = id[np.random.randint(idNum)]
	## Emulate delayed message delivery by back dating event time
    #    eventTime = tt - delay
	#outputStr = "%s;%s;%s" % (currKey, currID, eventTime)
    #    #print(outputStr)
    #    producer.produce(outputStr, partition_key=str(currKey))


      timeSpent = time.time() - tt
      sleepTime = burstFraction - timeSpent
      if sleepTime < 0:
        print("Rate too high! Launch more producers with smaller rate each.")
      else:
        time.sleep(sleepTime)


startTime = time.time()
totalSamples = 0
Producer().start()
while True:
  time.sleep(3)
endTime = time.time()
eprint(totalSamples, " samples produced in ", endTime - startTime, " seconds.")
