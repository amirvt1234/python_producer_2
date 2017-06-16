from kafka import KafkaClient,KafkaProducer
import numpy as np
import time


client = "localhost:9092"
topic =  "jtest"
#producer = topic.get_producer(partitioner=mod_partitioner, linger_ms = 200)
#producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
producer = KafkaProducer(bootstrap_servers = client)

"""
tw = 1*60. # The time window in seconds
#visitg = [1,2,10,100]
visitg = [1, 2, 10, 20] 
nvfv = {'1'   :[0       , int(1e6), int(1)], # [0,   1e6): (forgetters)
    '2'   :[int(1e6), int(2e6), int(1)], # [1e6, 2e6): (login-logout) 
    '10'  :[int(2e6), int(3e6), int(1)], # [2e6, 3e6): (active-user)
    '20' :[int(3e6), int(4e6), int(1)]} # [3e6, 4e6): (machine-spam)
dtime = np.int64(0)
for kkk in range(1):
	nv = 0 # Initiate the number of the visits to the website during the time window
	ld = []
	for v in visitg:
	    nv += v*nvfv[str(v)][2]
	    ld.append(np.repeat(np.random.randint(nvfv[str(v)][0], nvfv[str(v)][1], size=nvfv[str(v)][2]), v)) 

	ld1 = np.concatenate(ld[:])
	np.random.shuffle(ld1)
	dt = tw/len(ld1)
	times = time.time()

	eventTime = np.int64(0)

	for index, item in enumerate(ld1):
	    #currKey = key[np.random.randint(keyNum)]
	    eventTimeo = eventTime  
	    eventTime  = np.int64(np.floor(dt*(index+np.random.random_sample())*1000)) # event time in miliseconds
	    #time.sleep(eventTime-eventTimeo)
	    #print eventTime-eventTimeo
	    time.sleep((eventTime-eventTimeo)/1000.) # sleep time in miliseconds
	    currID = item
	    outputStr = "%s;%s" % (currID, dtime)
		#outputStr = "%s;%s" % (currID, np.int64(eventTime+dtime))
		#print outputStr
	    producer.send(topic,outputStr)
		#time.sleep(1)
	dtime += np.int64(60*1000)

timee = time.time()
print "takes:", timee - times
"""


ld1  = ["1;1", "1;2", "1;3", "1;4", "2;1", "2;1", "2;1", "3;1", "3;1"]
#ld1  = [0.5, 1, 2, 4, 5, 5.5, 6, 9, 10]

for index, item in enumerate(ld1):
    producer.send(topic,item)
    time.sleep(0.6)
	#time.sleep(1)



