
###Lab 3

#3
flightRdd=sc.textFile("/user/root/flights.csv").map(lambda line: line.split(","))
carrierRdd = flightRdd.map(lambda line: (line[5],1))
cReducedRdd = carrierRdd.reduceByKey(lambda a,b: a+b)
carriersSorted = cReducedRdd.map(lambda (a,b): (b,a)).sortByKey(ascending=False)

#3 Challenge
carrierNameRdd=sc.textFile("/user/root/carriers.csv").map(lambda line: line.split(",")).map(lambda line: (line[0], line[1]))
carrierNameRdd.join(carriersSorted).take(10)

#4
airportsRdd = sc.textFile("/user/root/airports.csv").map(lambda line: line.split(","))
cityRdd = airportsRdd.map(lambda line: (line[0], line[2]))
flightOrigDestRdd = flightRdd.map(lambda line: (line[12], line[13]))
origJoinRdd = flightOriginDestRdd.join(cityRdd)
destAndOrigJoinRdd = origJoinRdd.map(lambda (a,b): (b[0],b[1])).join(cityRdd)
citiesCleanRdd = destAndOrigJoinRdd.values()
citiesReducedRdd = itiesCleanRdd.map(lambda line: (line,1)).reduceByKey(lambda a,b: a+b)
citiesCleanRdd.map(lambda line: (line,1)).reduceByKey(lambda a,b: a+b).map(lambda (a,b): (b,a)).sortByKey(ascending=False).take(5)

#5 Challenge
flightRdd.filter(lambda line: int(line[11]) > 15).map(lambda line: (line[5], line[11])).reduceByKey(lambda a,b: max(int(a),int(b))).take(10)

#6 Challenge
airplanesRdd = sc.textFile("/user/root/plane-data.csv").map(lambda line: line.split(",")).filter(lambda line:len(line) == 9)
flight15Rdd = flightRdd.filter(lambda line: int(line[14]) > 1500).map(lambda line: (line[7],1))
flight15Rdd.join(tailModelRdd).map(lambda (a,b): (b[1],b[0])).reduceByKey(lambda a,b: a+b).map(lambda (a,b): (b,a)).sortByKey(ascending=False).take(2)
