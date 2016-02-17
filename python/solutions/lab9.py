import csv
reader = csv.reader(open('/root/spark/data/carriers.csv'))
result = {}
for row in reader:
	key = row[0]
	result[key] = row[1]

## or the developer can execute this instead of lines 1->6: >>execfile("/root/spark/python/stubs/lab9.py")

print(result)
type(result)

carrierbc=sc.broadcast(result)
flightRdd=sc.textFile("/user/root/flights.csv").map(lambda line: line.split(",")).map(lambda line: (line[6],line[5]))
flightUpdate=flightRdd.map(lambda (a,b): (a,carrierbc.value[b]))

flightUpdate.take(5)
