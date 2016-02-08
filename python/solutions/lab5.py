
##Lab 5

#1

joinedRdd=flightsKVRdd.join(carrierRdd)
joinedRdd.count()
##Note how long this took to complete

joinedRdd.cache()
joinedRdd.count()

joinedRdd.count()

#2

flightRdd=sc.textFile("/user/root/flight.csv").map(lambda line: line.split(",")).keyBy(lambda line: line[5])
carrierRdd = sc.textFile("/user/root/carriers.csv").map(lambda line: line.split(",")).map(lambda line: (line[0], line[1])) 
joinedRdd = flightRdd.join(carrierRdd)

from pyspark import StorageLevel
joinedRdd.persist(StorageLevel.MEMORY_ONLY)
joinedRdd.count()

joinedRdd.count()

joinedRdd.unpersist()
joinedRdd.persist(StorageLevel.DISK_ONLY)
joinedRdd.count()
joinedRdd.count()

joinedRdd.unpersist()
