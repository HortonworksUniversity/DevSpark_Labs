
planeRdd=sc.textFile("/user/root/plane-data.csv").map(lambda line: line.split(","))

badData=sc.accumulator(0)
def dataCheck(line,dataCounter):
	if len(line) != 9:
		dataCounter += 1

planeRdd.foreach(lambda line: dataCheck(line, badData))

print(badData)
