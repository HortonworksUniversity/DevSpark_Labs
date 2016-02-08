
##Lab 4

#2
flightRdd=sc.textFile("/user/root/flight.csv").map(lambda line: line.split(","))
flightsKVRdd=flightRdd.keyBy(lambda line: line[5])
flightsKVRdd.getNumPartitions()

carrierRdd = sc.textFile("/user/root/carriers.csv").map(lambda line: line.split(",")).map(lambda line: (line[0], line[1]))

#3
joinedRdd = flightsKVRdd.join(carrierRdd)
joinedRdd.count()

flightspartKVRdd=flightsKVRdd.repartition(10)
flightspartKVRdd.getNumPartitions()
flightspartKVRdd.join(carrierRdd).count()

flightspartKVRdd.map(lambda (a,b): (a,1)).reduceByKey(lambda a,b: a+b).join(carrierRdd).map(lambda (a,b): (b[0],b[1])).sortByKey(ascending=False).collect()
